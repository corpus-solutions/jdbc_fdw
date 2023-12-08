/*-------------------------------------------------------------------------
 *
 *                foreign-data wrapper for JDBC
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * This software is released under the PostgreSQL Licence
 *
 * IDENTIFICATION
 *                jdbc_fdw/ResultSetInfo.java
 *
 *-------------------------------------------------------------------------
 */
import java.sql.*;
import java.util.*;

public class ResultSetInfo {
  private ResultSet resultSet;
  private Integer numberOfColumns;
  private int numberOfAffectedRows;
  private PreparedStatement pstmt;

  public ResultSetInfo(
      ResultSet fieldResultSet,
      Integer fieldNumberOfColumns,
      int fieldNumberOfAffectedRows,
      PreparedStatement fieldPstmt) {
    this.resultSet = fieldResultSet;
    this.numberOfColumns = fieldNumberOfColumns;
    this.numberOfAffectedRows = fieldNumberOfAffectedRows;
    this.pstmt = fieldPstmt;
  }

  public void setPstmt(PreparedStatement fieldPstmt) {
    this.pstmt = fieldPstmt;
  }

  public void setNumberOfAffectedRows(int fieldNumberOfAffectedRows) {
    this.numberOfAffectedRows = fieldNumberOfAffectedRows;
  }

  public void setNumberOfColumns(int numberOfColumns) {
    this.numberOfColumns = numberOfColumns;
  }

  public void setResultSet(ResultSet resultSet) {
	this.resultSet = resultSet;
  }

  public ResultSet getResultSet() {
    return resultSet;
  }

  public Integer getNumberOfColumns() {
    return numberOfColumns;
  }

  public int getNumberOfAffectedRows() {
    return numberOfAffectedRows;
  }

  public PreparedStatement getPstmt() {
    return pstmt;
  }
}
