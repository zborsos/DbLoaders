/* @formatter:on */

import java.math.BigInteger;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class Main {
	final static int IDS_2_FETCH = 51000; //Select IDs to delete
	static List<String> IDS_2_DELETE = new ArrayList<> (IDS_2_FETCH);
	final static int DELETE_BATCH_SIZE = 1000 ;
	final static String inlistJoin = " /* <OPTGUIDELINES> <INLIST2JOIN TABLE=\"REPOSITORY.DELETED_ITEMS\" COLUMN=\"ITEM_UUID\"/> </OPTGUIDELINES> */";
	
	public static void main (String[] args) {
		final String urlPrefix = "jdbc:db2:";
		final String url = urlPrefix + "//localhost:50000/JTS";
		final String user = "db2admin";
		final String password = "load4now";
		final String selectIDs = "SELECT ITEM_UUID FROM REPOSITORY.DELETED_ITEMS ";
		final String whereWithDate = "WHERE CONTENT_DELETED = 1 AND STATES_DELETED = 1 AND DELETED_WHEN <= ? ";
		final String whereNoDate = "WHERE CONTENT_DELETED = 1 AND STATES_DELETED = 1 ";
		final String fetchFirstXRows = "FETCH FIRST ? ROWS ONLY";
		
		Timestamp ts = new Timestamp (System.currentTimeMillis ());
		Instant now = Instant.now ();
		Instant yesterday = now.minus (1, ChronoUnit.DAYS);
		Connection con;
		ResultSet rs;
		long startTime;
		
		System.out.println ("current Timestamp:" + ts.toString ());
		
		try {
			// Load the driver
			Class.forName ("com.ibm.db2.jcc.DB2Driver");
			System.out.println ("**** Loaded the JDBC driver");
			
			// Create the connection using the IBM Data Server Driver for JDBC and SQLJ
			con = DriverManager.getConnection (url, user, password);
			System.out.println ("**** Created a JDBC connection to the data source");
			
			// Commit changes manually
			con.setAutoCommit (false);
			
			//INSERTING records into the table
			if (false) {
				final String deleted_by = "_KGRY4CFWEdq-WY5y7lROQw";
				boolean insertNull = true;
				final String insertStmWithNulls = "INSERT INTO REPOSITORY.DELETED_ITEMS " +
						"(ITEM_UUID, DELETED_WHEN, DELETED_BY, ITEM_TYPE_DBID, CONTENT_DELETED, STATES_DELETED) " +
						"VALUES (?, ?, ?, 12, 0, 0)";
				final String insertStmWithOnes = "INSERT INTO REPOSITORY.DELETED_ITEMS " +
						"(ITEM_UUID, DELETED_WHEN, DELETED_BY, ITEM_TYPE_DBID, CONTENT_DELETED, STATES_DELETED) " +
						"VALUES (?, ?, ?, 12, 1, 1)";
				final int recordCount2Insert = 5100000;
				final String insertStm = (insertNull) ? insertStmWithNulls : insertStmWithOnes ;

				startTime = System.currentTimeMillis();
				for (int i = 0; i < recordCount2Insert; i++) {
					try {
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
						}
					} catch (SQLException se) {
						System.err.println ("Could not load into table");
						System.out.println ("Exception: " + se);
						se.printStackTrace ();
					}
				}
				Duration durationUpload = Duration.ofMillis (1);
				System.out.println ("Duration of uploading "+recordCount2Insert+" into DB2 :" +
						durationUpload.plusMillis (System.currentTimeMillis () - startTime).toSeconds () +
						" sec");
				con.commit ();
			}
			
//			Delete with embedded SELECT
			if (true) {
				int loopCount = (IDS_2_FETCH%DELETE_BATCH_SIZE > 0)?
						(int)(IDS_2_FETCH/DELETE_BATCH_SIZE) + 1 : (int)(IDS_2_FETCH/DELETE_BATCH_SIZE);
				int totalDelCnt = 0;
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
			}
			
			//SELECT uuids and DELETE them in loop
			if (true) {
				System.out.println ("SELECT uuids and DELETE them in loop");
				String selectStatement = selectIDs + whereWithDate + fetchFirstXRows;
				
				PreparedStatement pstmt = con.prepareStatement (selectStatement);
				pstmt.setTimestamp (1, Timestamp.from (yesterday));
				pstmt.setInt (2, IDS_2_FETCH);
				
				startTime = System.nanoTime ();
				Duration durationSelectLoop = Duration.ofMillis (1);

				rs = pstmt.executeQuery ();
				
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
				if(true){
					int i = IDS_2_DELETE.size();
					if(i > DELETE_BATCH_SIZE){
						do{
							//System.out.println ("Delete : "+ DELETE_BATCH_SIZE);
							deleteRecords(con);
							i -= DELETE_BATCH_SIZE;
							//System.out.println ("Remains : " + i);
						}while (i > DELETE_BATCH_SIZE);
					}
					if(i > 0){
						//System.out.println ("Delete : " + i);
						deleteRecords(con);
						i = 0;
					}
				}
				System.out.println ("Total Duration of Delete: " +
						durationDeleteLoops.plusNanos (System.nanoTime () - startTime).toSeconds () +
						" sec");
			}
			
			
			//DELETE without SELECT use to overload transaction-logs
			if (false) {
				PreparedStatement pstmt;
				startTime = System.nanoTime ();
				Duration durationSelectLoop = Duration.ofMillis (1);
				
				int totalDelCnt ;
				
				pstmt = con.prepareStatement (
						"DELETE FROM REPOSITORY.DELETED_ITEMS "+ whereNoDate );
				totalDelCnt = pstmt.executeUpdate ();
				
				pstmt.close ();
				con.commit ();
				
				System.out.println ("Deleted records: " + totalDelCnt);
				System.out.println ("Deleted batch size: ALLinONE");
				System.out.println ("Duration of DELETE LOOP from DB2 :" +
						durationSelectLoop.plusNanos (System.nanoTime () - startTime).toSeconds () +
						" sec");
			}
			
			// Connection must be on a unit-of-work boundary to allow close
			con.commit ();
			
			// Close the connection
			con.close ();
			System.out.println ("**** Disconnected from data source");
		} catch (ClassNotFoundException e) {
			System.err.println ("Could not load JDBC driver");
			System.out.println ("Exception: " + e);
			
		} catch (SQLException ex) {
			System.err.println ("SQLException information");
			while (ex != null) {
				System.err.println ("Error msg: " + ex.getMessage ());
				System.err.println ("SQLSTATE: " + ex.getSQLState ());
				System.err.println ("Error code: " + ex.getErrorCode ());
				ex = ex.getNextException (); // For drivers that support chained exceptions
			}
		}
	}
	private static void deleteRecords(Connection con) {
		String [] arr2delete;
		if(IDS_2_DELETE.size() > DELETE_BATCH_SIZE){
			arr2delete = new String[DELETE_BATCH_SIZE];
		} else if (IDS_2_DELETE.size() > 0) {
			arr2delete = new String[IDS_2_DELETE.size()];
		}else {
			return;
		}
		
		for(int i = 0; i < arr2delete.length; i++){
			if(! IDS_2_DELETE.isEmpty()){
				arr2delete[i]= IDS_2_DELETE.get(0);
				IDS_2_DELETE.remove(0);
			}
		}
		
		try {
			PreparedStatement pstmt;
			String tmp2del = "'"+String.join("', '", arr2delete)+"'";
			long startTime = System.nanoTime ();
			Duration durationSelectLoop = Duration.ofMillis (1);
			String updateStmnt = "DELETE FROM REPOSITORY.DELETED_ITEMS WHERE ITEM_UUID IN( "+tmp2del+" ) " + inlistJoin;

			pstmt = con.prepareStatement (updateStmnt);
			int totalDelCnt = pstmt.executeUpdate();
			
			pstmt.close ();
			con.commit ();

			/*System.out.println ("Deleted records: " + totalDelCnt+" Duration :" +
					durationSelectLoop.plusNanos (System.nanoTime () - startTime).toSeconds () +
					" sec");
			*/
		} catch (SQLException ex) {
			System.err.println ("SQLException information in =deleteRecords=");
			while (ex != null) {
				System.err.println ("Error msg: " + ex.getMessage ());
				System.err.println ("SQLSTATE: " + ex.getSQLState ());
				System.err.println ("Error code: " + ex.getErrorCode ());
				ex.printStackTrace ();
				ex = ex.getNextException (); // For drivers that support chained exceptions
			}
		}
		return;
	}
}


