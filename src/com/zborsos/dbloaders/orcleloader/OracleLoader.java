package com.zborsos.dbloaders.orcleloader;

import local.utils.uid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class OracleLoader {
    final Logger log = LoggerFactory.getLogger(this.getClass().getName());
    final Marker IMPORTANT = MarkerFactory.getMarker("IMPORTANT");
    final  int IDS_2_FETCH = 1000000; //Select IDs to delete
    List<String> IDS_2_DELETE = new ArrayList<>(IDS_2_FETCH);
    final  int DELETE_BATCH_SIZE = 1000 ;
    final String deleted_by = "_KGRY4CFWEdq-WY5y7lROQw";
    final String tableName = "JTSDBUSER.REPOSITORY_DELETED_ITEMS";
    final String urlPrefix = "jdbc:db2:";
    final String user = "jtsdbuser";
    final String password = "jtsdbuser";

    final  String inlistJoin = " /* <OPTGUIDELINES> <INLIST2JOIN TABLE=\""+tableName+"\" COLUMN=\"ITEM_UUID\"/> </OPTGUIDELINES> */";
    final String selectIDs = "SELECT ITEM_UUID FROM " + tableName + " ";
    final String whereWithDate = "WHERE CONTENT_DELETED = 1 AND STATES_DELETED = 1 AND DELETED_WHEN <= ? ";
    final String fetchFirstXRows = "FETCH FIRST ? ROWS ONLY";
    Instant now = Instant.now ();
    Instant yesterday = now.minus (1, ChronoUnit.DAYS);

    /**
     * Create the connection using the IBM Data Server Driver for JDBC and SQLJ
     */
    private Connection con = null;

    public OracleLoader() throws SQLException {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        try{
            //thin:jtsdbuser/{password}@//localhost:1521/PDB1
            con = DriverManager.getConnection(
                    "jdbc:oracle:thin:@localhost:1521/PDB1", user, password);
        }catch(SQLException ex){
            log.error(ex.getMessage(),ex);
        }
        if(con != null){
            con.setAutoCommit (false);
            // Connection must be on a unit-of-work boundary to allow close
            con.commit ();
        }

    }


    public long loadRandomRecords(long count2load) throws SQLException {
        long insertedRecordsCount = 0;
        long startTime;
        Timestamp ts = new Timestamp (System.currentTimeMillis ());
        Calendar cal = Calendar.getInstance();
        cal.setTime(ts);
        cal.add(Calendar.DATE, -2);
        Timestamp Yesterday = new Timestamp(cal.getTime().getTime());
        final String insertStmWithNulls = "INSERT INTO "+tableName+" " +
                "(ITEM_UUID, DELETED_WHEN, DELETED_BY, ITEM_TYPE_DBID, CONTENT_DELETED, STATES_DELETED) " +
                "VALUES (?, ?, ?, 12, 0, 0)";
        final String insertStmWithOnes = "INSERT INTO "+tableName+" " +
                "(ITEM_UUID, DELETED_WHEN, DELETED_BY, ITEM_TYPE_DBID, CONTENT_DELETED, STATES_DELETED) " +
                "VALUES (?, ?, ?, 12, 1, 1)";
        startTime = System.currentTimeMillis();
        for (int i = 0; i < count2load; i++){
            //String insertStm = (random.getRandoTrueFalse ()) ? insertStmWithNulls : insertStmWithOnes ;
            String insertStm = insertStmWithOnes ;

            PreparedStatement insertSmt = con.prepareStatement (insertStm);
            insertSmt.setString (1, uid.generate().getUuidValue());
            insertSmt.setTimestamp (2, Timestamp.from (yesterday));
            insertSmt.setString (3, deleted_by);
            insertSmt.executeUpdate ();
            insertSmt.close ();

            insertedRecordsCount = i;
            if (i % 5000 == 0) {
                con.commit ();
            }
        }
        con.commit ();
        return insertedRecordsCount;
    }

    //Delete with embedded SELECT
    public long deleteWithEmbeddedSelect() throws SQLException {
        long startTime = System.nanoTime ();
        Duration durationDeleteLoops = Duration.ofSeconds (0) ;
        ResultSet rs;
        int loopCount = (IDS_2_FETCH % DELETE_BATCH_SIZE > 0)?
                (IDS_2_FETCH/DELETE_BATCH_SIZE) + 1 : (IDS_2_FETCH/DELETE_BATCH_SIZE);
        long totalDelCnt = 0;
        log.info ("DELETEing with embedded SELECT( original way of doing it)");
        String deleteStmnt = "DELETE FROM "+tableName+" WHERE ITEM_UUID IN " +
                "("+ selectIDs + whereWithDate + fetchFirstXRows + ")";
        log.debug (deleteStmnt);
        log.info("Deleting maximum {} records in batches of {}", IDS_2_FETCH, DELETE_BATCH_SIZE);


        for(int i=0; i < loopCount; i++){
            PreparedStatement pstmt;
            pstmt = con.prepareStatement (deleteStmnt);
            pstmt.setTimestamp (1, Timestamp.from (yesterday));
            pstmt.setInt (2, DELETE_BATCH_SIZE);
            totalDelCnt += pstmt.executeUpdate();
            pstmt.close();
            con.commit();
        }
        log.info ("Deleted records: {}", totalDelCnt);
        log.info (IMPORTANT,"Duration of OLD Delete: {} sec" ,
                durationDeleteLoops.plusNanos (System.nanoTime () - startTime).toSeconds () );
        return totalDelCnt;
    }

    //SELECT uuids and DELETE them in loop
    public long deleteSelectedIDs() throws SQLException {
        long startTime;
        long totalDeletedCount =0;
        ResultSet rs;
        log.info ("SELECT uuids and DELETE them in loop");
        startTime = System.nanoTime ();
        Duration durationDeleteLoops = Duration.ofMillis (1);
        String selectStatement = selectIDs + whereWithDate + fetchFirstXRows;

        PreparedStatement pstmt = con.prepareStatement (selectStatement);
        pstmt.setTimestamp (1, Timestamp.from (yesterday));
        pstmt.setInt (2, IDS_2_FETCH);
        rs = pstmt.executeQuery ();
        ResultSetMetaData rsmd = rs.getMetaData();
        while (rs.next()) {
            String id = rs.getString (1);
            IDS_2_DELETE.add (id);
        }
        // Close the ResultSet
        rs.close();
        pstmt.close ();

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
        log.info ("Deleted records: {}", totalDeletedCount);
        log.info (IMPORTANT,"Duration of NEW Delete: {} sec" ,
                durationDeleteLoops.plusNanos (System.nanoTime () - startTime).toSeconds () );
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
        String baseDelete = "DELETE FROM "+tableName+" WHERE ITEM_UUID IN( ";
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
}