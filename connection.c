/*-------------------------------------------------------------------------
 *
 * connection.c
 *        Connection management functions for jdbc_fdw
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *        contrib/jdbc_fdw/connection.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "jdbc_fdw.h"

#if PG_VERSION_NUM >= 130000
#include "common/hashfn.h"
#endif
#include "access/xact.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/elog.h"
#include "utils/syscache.h"
#include "commands/defrem.h"

/*
 * Connection cache hash table entry
 *
 * The lookup key in this hash table is the foreign server OID plus the user
 * mapping OID.  (We use just one connection per user per foreign server, so
 * that we can ensure all scans use the same snapshot during a query.)
 *
 * The "conn" pointer can be NULL if we don't currently have a live
 * connection.
 */
typedef struct ConnCacheKey
{
	Oid			serverid;		/* OID of foreign server */
	Oid			userid;			/* OID of local user whose mapping we use */
} ConnCacheKey;

typedef struct ConnCacheEntry
{
	ConnCacheKey key;			/* hash key (must be first) */
	Jconn	   *conn;			/* connection to foreign server, or NULL */
	bool		have_prep_stmt; /* have we prepared any stmts in this xact? */
	bool		have_error;		/* have any subxacts aborted in this xact? */
} ConnCacheEntry;

/*
 * Connection cache (initialized on first use)
 */
static HTAB *ConnectionHash = NULL;

/* for assigning cursor numbers and prepared statement numbers */
static volatile unsigned int cursor_number = 0;
static unsigned int prep_stmt_number = 0;

/* tracks whether any work is needed in callback functions */
static volatile bool xact_got_connection = false;

/* prototypes of private functions */
static Jconn * connect_jdbc_server(ForeignServer *server, UserMapping *user, char *username);
static void jdbc_do_sql_command(Jconn * conn, const char *sql);
static void jdbcfdw_xact_callback(XactEvent event, void *arg);

/*
 * Get a Jconn which can be used to execute queries on the remote JDBC server
 * server with the user's authorization.  A new connection is established if
 * we don't already have a suitable one, and a transaction is opened at the
 * right subtransaction nesting depth if we didn't do that already.
 *
 * will_prep_stmt must be true if caller intends to create any prepared
 * statements.  Since those don't go away automatically at transaction end
 * (not even on error), we need this flag to cue manual cleanup.
 *
 * XXX Note that caching connections theoretically requires a mechanism to
 * detect change of FDW objects to invalidate already established
 * connections. We could manage that by watching for invalidation events on
 * the relevant syscaches.  For the moment, though, it's not clear that this
 * would really be useful and not mere pedantry.  We could not flush any
 * active connections mid-transaction anyway.
 */
Jconn *
jdbc_get_connection(ForeignServer *server, UserMapping *user,
					bool will_prep_stmt)
{
	bool		found;
	ConnCacheEntry *entry;
	ConnCacheKey key;

	ListCell   *lc;
	DefElem    *userDef = NULL;
	char	   *usePgRoleName = NULL;
	char       *username;

	ereport(DEBUG3, (errmsg("jdbc_get_connection") ));
	/* Loop through the options, and get the values */
	foreach(lc, user->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		if (strcmp(def->defname, "usePgRoleName") == 0)
		{
			usePgRoleName = defGetString(def);
		}
		if (strcmp(def->defname, "username") == 0)
		{
			userDef = def;
		}
	}

	if(userDef == NULL)
	{
		ereport(ERROR, (errmsg("No username provided in user mapping options!")));
	}

	if (usePgRoleName != NULL && strcmp(usePgRoleName, "true") == 0)
	{
		username = GetUserNameFromId(user->userid, false);
	} else {
		username = defGetString(userDef);
	}

	/* First time through, initialize connection cache hashtable */
	if (ConnectionHash == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(ConnCacheKey);
		ctl.entrysize = sizeof(ConnCacheEntry);
		ctl.hash = tag_hash;
		/* allocate ConnectionHash in the cache context */
		ctl.hcxt = CacheMemoryContext;
		ConnectionHash = hash_create("jdbc_fdw connections", 8,
									 &ctl,
									 HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

		/*
		 * Register some callback functions that manage connection cleanup.
		 * This should be done just once in each backend.
		 */
		RegisterXactCallback(jdbcfdw_xact_callback, NULL);
	}
	ereport(DEBUG3, (errmsg("Added server = %s to hashtable", server->servername)));

	/* Set flag that we did GetConnection during the current transaction */
	xact_got_connection = true;

	/* Create hash key for the entry.  Assume no pad bytes in key struct */
	key.serverid = server->serverid;
	key.userid = user->userid;

	/*
	 * Find or create cached entry for requested connection.
	 */
	entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		/* initialize new hashtable entry (key is already filled in) */
		entry->conn = NULL;
		entry->have_prep_stmt = false;
		entry->have_error = false;
	}

	/*
	 * We don't check the health of cached connection here, because it would
	 * require some overhead.  Broken connection will be detected when the
	 * connection is actually used.
	 */

	/*
	 * If cache entry doesn't have a connection, we have to establish a new
	 * connection.  (If connect_jdbc_server throws an error, the cache entry
	 * will be left in a valid empty state.)
	 */
	if (entry->conn == NULL)
	{
		entry->have_prep_stmt = false;
		entry->have_error = false;
		entry->conn = connect_jdbc_server(server, user, username);
	}
	else
	{
		jdbc_jvm_init(server, user);
	}

	/* Remember if caller will prepare statements */
	entry->have_prep_stmt |= will_prep_stmt;

	return entry->conn;
}

/*
 * Connect to remote server using specified server and user mapping
 * properties.
 */
static Jconn *
connect_jdbc_server(ForeignServer *server, UserMapping *user, char *username)
{
	Jconn	   *volatile conn = NULL;

	/*
	 * Use PG_TRY block to ensure closing connection on error.
	 */
	PG_TRY();
	{
		conn = jq_connect_db_params(server, user, username);
		if (!conn || jq_status(conn) != CONNECTION_OK)
		{
			char	   *connmessage;
			int			msglen;

			/* libpq typically appends a newline, strip that */
			connmessage = pstrdup(jq_error_message(conn));
			msglen = strlen(connmessage);
			if (msglen > 0 && connmessage[msglen - 1] == '\n')
				connmessage[msglen - 1] = '\0';
			ereport(ERROR,
					(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
					 errmsg("could not connect to server \"%s\"",
							server->servername),
					 errdetail_internal("%s", connmessage)));
		}

		/*
		 * Check that non-superuser has used password to establish connection;
		 * otherwise, he's piggybacking on the JDBC server's user identity.
		 * See also dblink_security_check() in contrib/dblink.
		 */
		if (!superuser() && !jq_connection_used_password(conn))
			ereport(ERROR,
					(errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
					 errmsg("password is required"),
					 errdetail("Non-superuser cannot connect if the server does not request a password."),
					 errhint("Target server's authentication method must be changed.")));
	}
	PG_CATCH();
	{
		/* Release Jconn data structure if we managed to create one */
		jq_close(conn);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return conn;
}

/*
 * Convenience subroutine to issue a non-data-returning SQL command to remote
 */
static void
jdbc_do_sql_command(Jconn * conn, const char *sql)
{
	Jresult    *res;

	res = jq_exec(conn, sql);
	if (*res != PGRES_COMMAND_OK)
		jdbc_fdw_report_error(ERROR, res, conn, true, sql);
	jq_clear(res);
}

/*
 * Release connection reference count created by calling GetConnection.
 */
void
jdbc_release_connection(Jconn * conn)
{
	/*
	 * Currently, we don't actually track connection references because all
	 * cleanup is managed on a transaction or subtransaction basis instead. So
	 * there's nothing to do here.
	 */
}

/*
 * Assign a "unique" number for a cursor.
 *
 * These really only need to be unique per connection within a transaction.
 * For the moment we ignore the per-connection point and assign them across
 * all connections in the transaction, but we ask for the connection to be
 * supplied in case we want to refine that.
 *
 * Note that even if wraparound happens in a very long transaction, actual
 * collisions are highly improbable; just be sure to use %u not %d to print.
 */
unsigned int
jdbc_get_cursor_number(Jconn * conn)
{
	return ++cursor_number;
}

/*
 * Assign a "unique" number for a prepared statement.
 *
 * This works much like jdbc_get_cursor_number, except that we never reset
 * the counter within a session.  That's because we can't be 100% sure we've
 * gotten rid of all prepared statements on all connections, and it's not
 * really worth increasing the risk of prepared-statement name collisions by
 * resetting.
 */
unsigned int
jdbc_get_prep_stmt_number(Jconn * conn)
{
	return ++prep_stmt_number;
}

/*
 * Report an error we got from the remote server.
 *
 * elevel: error level to use (typically ERROR, but might be less) res:
 * Jresult containing the error conn: connection we did the query on clear:
 * if true, jq_clear the result (otherwise caller will handle it) sql: NULL,
 * or text of remote command we tried to execute
 *
 * Note: callers that choose not to throw ERROR for a remote error are
 * responsible for making sure that the associated ConnCacheEntry gets marked
 * with have_error = true.
 */
void
jdbc_fdw_report_error(int elevel, Jresult * res, Jconn * conn,
					  bool clear, const char *sql)
{
	/*
	 * If requested, Jresult must be released before leaving this function.
	 */
	PG_TRY();
	{
		char	   *diag_sqlstate = jq_result_error_field(res, PG_DIAG_SQLSTATE);
		char	   *message_primary = jq_result_error_field(res, PG_DIAG_MESSAGE_PRIMARY);
		char	   *message_detail = jq_result_error_field(res, PG_DIAG_MESSAGE_DETAIL);
		char	   *message_hint = jq_result_error_field(res, PG_DIAG_MESSAGE_HINT);
		char	   *message_context = jq_result_error_field(res, PG_DIAG_CONTEXT);
		int			sqlstate;

		if (diag_sqlstate)
			sqlstate = MAKE_SQLSTATE(diag_sqlstate[0],
									 diag_sqlstate[1],
									 diag_sqlstate[2],
									 diag_sqlstate[3],
									 diag_sqlstate[4]);
		else
			sqlstate = ERRCODE_CONNECTION_FAILURE;

		/*
		 * If we don't get a message from the Jresult, try the Jconn. This is
		 * needed because for connection-level failures, jq_exec may just
		 * return NULL, not a Jresult at all.
		 */
		if (message_primary == NULL)
			message_primary = jq_error_message(conn);

		ereport(elevel,
				(errcode(sqlstate),
				 message_primary ? errmsg_internal("%s", message_primary) :
				 errmsg("unknown error"),
				 message_detail ? errdetail_internal("%s", message_detail) : 0,
				 message_hint ? errhint("%s", message_hint) : 0,
				 message_context ? errcontext("%s", message_context) : 0,
				 sql ? errcontext("Remote SQL command: %s", sql) : 0));
	}
	PG_CATCH();
	{
		if (clear)
			jq_clear(res);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (clear)
		jq_clear(res);
}

/*
 * jdbcfdw_xact_callback --- cleanup at main-transaction end.
 */
static void
jdbcfdw_xact_callback(XactEvent event, void *arg)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	/* Quick exit if no connections were touched in this transaction. */
	if (!xact_got_connection)
		return;

	if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT)
	{
		/*
		 * Scan all connection cache entries and release its resource
		 */
		hash_seq_init(&scan, ConnectionHash);
		while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
		{
			/* Ignore cache entry if no open connection right now */
			if (entry->conn == NULL)
				continue;

			/* release JDBCUtils resource */
			jq_close(entry->conn);
			entry->conn->JDBCUtilsObject = NULL;
			pfree(entry->conn);
			entry->conn = NULL;
		}

		//jq_release_all_result_sets();
		jq_finish();
		xact_got_connection = false;
	}
}

