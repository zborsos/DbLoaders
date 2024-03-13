package com.zborsos.dbloaders.orcleloader;

import local.utils.uid;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class OracleLoader {
    final Logger log = Logger.getLogger(this.getClass().getName());
    final  int IDS_2_FETCH = 1000000; //Select IDs to delete
    List<String> IDS_2_DELETE = new ArrayList<>(IDS_2_FETCH);
    final  int DELETE_BATCH_SIZE = 500 ;
    final String deleted_by = "_KGRY4CFWEdq-WY5y7lROQw";
    final String tableName = "JTSDBUSER.REPOSITORY_DELETED_ITEMS";
    final String urlPrefix = "jdbc:db2:";
    final String url = urlPrefix + "//localhost:50000/JTS";
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
        log.setLevel(Level.INFO);
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
            log.severe(ex.getMessage());
            System.exit(1);
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
            log.finest(insertStm);
            try{
                PreparedStatement insertSmt = con.prepareStatement (insertStm);
                insertSmt.setString (1, uid.generate().getUuidValue());
                insertSmt.setTimestamp (2, Timestamp.from (yesterday));
                insertSmt.setString (3, deleted_by);
                insertSmt.executeUpdate ();
                insertSmt.close ();
            } catch (Exception e) {
                e.printStackTrace();
                log.severe(e.getMessage());
                throw e;
            }

            if (i % 5000 == 0) {
                con.commit ();
                insertedRecordsCount = i;
            }
        }
        Duration durationUpload = Duration.ofMillis (1);
        log.info ("Duration of uploading "+insertedRecordsCount+" into DataBase:" +
                durationUpload.plusMillis (System.currentTimeMillis () - startTime).toSeconds () +
                " sec");
        con.commit ();
        return insertedRecordsCount;
    }

    //Delete with embedded SELECT
    public long deleteWithEmbeddedSelect() throws SQLException {
        long startTime;
        ResultSet rs;
        int loopCount = (IDS_2_FETCH % DELETE_BATCH_SIZE > 0)?
                (IDS_2_FETCH/DELETE_BATCH_SIZE) + 1 : (IDS_2_FETCH/DELETE_BATCH_SIZE);
        long totalDelCnt = 0;
        log.info ("DELETE with embedded SELECT( original way of doing it)");
        String deleteStmnt = "DELETE FROM "+tableName+" WHERE ITEM_UUID IN " +
                "("+ selectIDs + whereWithDate + fetchFirstXRows + ")";
        log.finest (deleteStmnt);
        log.info("Deleting "+IDS_2_FETCH+" records in batches of "+DELETE_BATCH_SIZE);

        startTime = System.nanoTime ();
        Duration durationSelectLoop = Duration.ofSeconds (0) ;
        for(int i=0; i < loopCount; i++){
            PreparedStatement pstmt;
            pstmt = con.prepareStatement (deleteStmnt);
            pstmt.setTimestamp (1, Timestamp.from (yesterday));
            pstmt.setInt (2, DELETE_BATCH_SIZE);
            totalDelCnt += pstmt.executeUpdate();
            pstmt.close();
            con.commit();
        }
        log.info ("Deleted records: " + totalDelCnt);
        log.info ("Deleted batch size: " + DELETE_BATCH_SIZE);
        log.info ("Duration of DELETE LOOP from DataBase:" +
                durationSelectLoop.plusNanos (System.nanoTime () - startTime).toSeconds () +
                " sec");
        return totalDelCnt;
    }

    //SELECT uuids and DELETE them in loop
    public long deleteSelectedIDs() throws SQLException {
        long startTime;
        long totalDeletedCount =0;
        ResultSet rs;
        log.info ("SELECT uuids and DELETE them in loop");
        String selectStatement = selectIDs + whereWithDate + fetchFirstXRows;

        PreparedStatement pstmt = con.prepareStatement (selectStatement);
        pstmt.setTimestamp (1, Timestamp.from (yesterday));
        pstmt.setInt (2, IDS_2_FETCH);

        startTime = System.nanoTime ();
        Duration durationSelectLoop = Duration.ofMillis (1);

        rs = pstmt.executeQuery ();
        ResultSetMetaData rsmd = rs.getMetaData();
        while (rs.next()) {
            String id = rs.getString (1);
            IDS_2_DELETE.add (id);
        }
        // Close the ResultSet
        rs.close();
        pstmt.close ();

        log.info ("Selected records: " + IDS_2_DELETE.size());
        log.info ("Duration of SELECT from DataBase:" +
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

        log.info ("Total Duration of Delete: " +
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