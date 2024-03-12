/* @formatter:on */
package com.zborsos.dbloaders.db2loader;

import java.math.BigInteger;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class DB2Loader {
	final  int IDS_2_FETCH = 3; //Select IDs to delete
	 List<String> IDS_2_DELETE = new ArrayList<> (IDS_2_FETCH);
	final  int DELETE_BATCH_SIZE = 1 ;
	final String deleted_by = "_KGRY4CFWEdq-WY5y7lROQw";
	final  String inlistJoin = " /* <OPTGUIDELINES> <INLIST2JOIN TABLE=\"REPOSITORY.DELETED_ITEMS\" COLUMN=\"ITEM_UUID\"/> </OPTGUIDELINES> */";
	final String urlPrefix = "jdbc:db2:";
	final String url = urlPrefix + "//localhost:50000/JTS";
	final String user = "db2admin";
	final String password = "db2admin_password";
	final String selectIDs = "SELECT ITEM_UUID FROM REPOSITORY.DELETED_ITEMS ";
	final String whereWithDate = "WHERE CONTENT_DELETED = 1 AND STATES_DELETED = 1 AND DELETED_WHEN <= ? ";
	final String fetchFirstXRows = "FETCH FIRST ? ROWS ONLY";
	Instant now = Instant.now ();
	Instant yesterday = now.minus (1, ChronoUnit.DAYS);

	/**
     * Create the connection using the IBM Data Server Driver for JDBC and SQLJ
     */
	private Connection con;

    public DB2Loader () throws SQLException {
			// Load the driver
        try {
            Class.forName ("com.ibm.db2.jcc.DB2Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
		try{
			con = DriverManager.getConnection (url, user, password);
		}catch(SQLException ex){
			System.out.println(ex.getMessage());
			System.exit(1);
		}

        con.setAutoCommit (false);
			// Connection must be on a unit-of-work boundary to allow close
			con.commit ();
			// Close the connection
			con.close ();
	}

	public long loadRandomRecords(long count2load) throws SQLException {
		long insertedRecordsCount = 0;
		long startTime;
		Timestamp ts = new Timestamp (System.currentTimeMillis ());
		final String insertStmWithNulls = "INSERT INTO REPOSITORY.DELETED_ITEMS " +
				"(ITEM_UUID, DELETED_WHEN, DELETED_BY, ITEM_TYPE_DBID, CONTENT_DELETED, STATES_DELETED) " +
				"VALUES (?, ?, ?, 12, 0, 0)";
		final String insertStmWithOnes = "INSERT INTO REPOSITORY.DELETED_ITEMS " +
				"(ITEM_UUID, DELETED_WHEN, DELETED_BY, ITEM_TYPE_DBID, CONTENT_DELETED, STATES_DELETED) " +
				"VALUES (?, ?, ?, 12, 1, 1)";
		startTime = System.currentTimeMillis();
		for (int i = 0; i < count2load; i++){
			String insertStm = (insertWithNull()) ? insertStmWithNulls : insertStmWithOnes ;
			String uuid = String.format ("%040d", new BigInteger (UUID.randomUUID ().toString ().replace ("-", ""), 23));
			String uuid23 = uuid.substring (uuid.length () - 23);
			PreparedStatement insertSmt = con.prepareStatement (insertStm);
			insertSmt.setString (1, uuid23);
			insertSmt.setString (2, ts.toString ());
			insertSmt.setString (3, deleted_by);
			insertSmt.executeUpdate ();
			insertSmt.close ();
			if (i % 5000 == 0) {
				con.commit ();
				insertedRecordsCount = i;
			}
		}
		Duration durationUpload = Duration.ofMillis (1);
		System.out.println ("Duration of uploading "+insertedRecordsCount+" into DB2 :" +
				durationUpload.plusMillis (System.currentTimeMillis () - startTime).toSeconds () +
				" sec");
		con.commit ();
		return insertedRecordsCount;
	}

	//Delete with embedded SELECT
	public long deleteWithEmbeddedSelect() throws SQLException {
		long startTime;
		ResultSet rs;
		int loopCount = (IDS_2_FETCH%DELETE_BATCH_SIZE > 0)?
				(IDS_2_FETCH/DELETE_BATCH_SIZE) + 1 : (IDS_2_FETCH/DELETE_BATCH_SIZE);
		long totalDelCnt = 0;
		System.out.println ("DELETE with embedded SELECT( original way of doing it)");
		String deleteStmnt = "DELETE FROM REPOSITORY.DELETED_ITEMS WHERE ITEM_UUID IN " +
				"("+ selectIDs + whereWithDate + fetchFirstXRows + ")";
		System.out.println (deleteStmnt);
		System.out.println ("Deleting "+IDS_2_FETCH+" records in batches of "+DELETE_BATCH_SIZE);

		startTime = System.nanoTime ();
		Duration durationSelectLoop = Duration.ofSeconds (0) ;
		for(int i=0; i < loopCount; i++){
			PreparedStatement pstmt;
			pstmt = con.prepareStatement (deleteStmnt);
			pstmt.setTimestamp (1, Timestamp.from (yesterday));
			pstmt.setInt (2, DELETE_BATCH_SIZE);
			totalDelCnt += pstmt.executeUpdate ();
			pstmt.close ();
			con.commit ();
		}
		System.out.println ("Deleted records: " + totalDelCnt);
		System.out.println ("Deleted batch size: " + DELETE_BATCH_SIZE);
		System.out.println ("Duration of DELETE LOOP from DB2 :" +
				durationSelectLoop.plusNanos (System.nanoTime () - startTime).toSeconds () +
				" sec");
		System.out.println ("=============================================================================================================================================");
		System.out.println ();
		return totalDelCnt;
	}

	//SELECT uuids and DELETE them in loop
	public long deleteSelectedIDs() throws SQLException {
		long startTime;
		long totalDeletedCount =0;
		ResultSet rs;
		System.out.println ("SELECT uuids and DELETE them in loop");
		String selectStatement = selectIDs + whereWithDate + fetchFirstXRows;

		PreparedStatement pstmt = con.prepareStatement (selectStatement);
		pstmt.setTimestamp (1, Timestamp.from (yesterday));
		pstmt.setInt (2, IDS_2_FETCH);

		startTime = System.nanoTime ();
		Duration durationSelectLoop = Duration.ofMillis (1);

		rs = pstmt.executeQuery ();
		ResultSetMetaData rsmd = rs.getMetaData();
		int column_name = rsmd.getColumnDisplaySize (1);
		System.out.println (column_name);
		System.out.println (rs.getMetaData ().getColumnTypeName (1));

		while (rs.next()) {
			String id = rs.getString (1);
			IDS_2_DELETE.add (id);
		}
		// Close the ResultSet
		rs.close();
		pstmt.close ();

		System.out.println ("Selected records: " + IDS_2_DELETE.size());
		System.out.println ("Duration of SELECT from DB2 :" +
				durationSelectLoop.plusNanos (System.nanoTime () - startTime).toSeconds () +
				" sec");

		startTime = System.nanoTime ();
		Duration durationDeleteLoops = Duration.ofMillis (1);

		int i = IDS_2_DELETE.size();
		if(i > DELETE_BATCH_SIZE){
			do{
			totalDeletedCount += deleteRecords(con);
				i -= DELETE_BATCH_SIZE;
			}while (i > DELETE_BATCH_SIZE);
		}
		if(i > 0){
			totalDeletedCount += deleteRecords(con);
			i = 0;
		}

		System.out.println ("Total Duration of Delete: " +
				durationDeleteLoops.plusNanos (System.nanoTime () - startTime).toSeconds () +
				" sec");
		return totalDeletedCount;
	}

	private int deleteRecords(Connection con) throws SQLException {
		String [] arr2delete;
		int totalDelCnt = 0;
		if(IDS_2_DELETE.size() > DELETE_BATCH_SIZE){
			arr2delete = new String[DELETE_BATCH_SIZE];
		} else if (IDS_2_DELETE.size() > 0) {
			arr2delete = new String[IDS_2_DELETE.size()];
		}else {
			return 0;
		}

		for(int i = 0; i < arr2delete.length; i++){
			if(! IDS_2_DELETE.isEmpty()){
				arr2delete[i]= IDS_2_DELETE.get(0);
				IDS_2_DELETE.remove(0);
			}
		}
		PreparedStatement pstmt;
		String baseDelete = "DELETE FROM REPOSITORY.DELETED_ITEMS WHERE ITEM_UUID IN( ";
		for(int i=0; i<arr2delete.length; i++){
			baseDelete += " ?,";
		}
		baseDelete = baseDelete.substring(0, baseDelete.length()-1);
		baseDelete += " )";

		String updateStmnt = baseDelete + inlistJoin;

		pstmt = con.prepareStatement (updateStmnt);
		for(int i=0; i<arr2delete.length; i++){
			pstmt.setString (i+1, arr2delete[i]);
		}
		totalDelCnt = pstmt.executeUpdate();

		pstmt.close ();
		con.commit ();

		return totalDelCnt;
	}
	private Boolean insertWithNull(){
		Random random = new Random();
		return random.nextBoolean();
	}
}


