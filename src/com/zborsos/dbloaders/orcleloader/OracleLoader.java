package com.zborsos.dbloaders.orcleloader;

import local.utils.uid;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Calendar;

public class OracleLoader {
    static Connection con;
    public static void main(String[] args) {
        Timestamp ts = new Timestamp (System.currentTimeMillis ());
        Instant now = Instant.now ();

        Calendar cal = Calendar.getInstance();
        cal.setTime(ts);
        cal.add(Calendar.DATE, -2);
        Timestamp Yesterday = new Timestamp(cal.getTime().getTime());

        Connection con;
        ResultSet rs;
        long startTime;
        try{
            Class.forName("oracle.jdbc.driver.OracleDriver");
            //thin:jtsdbuser/{password}@//localhost:1521/PDB1
            con= DriverManager.getConnection(
                    "jdbc:oracle:thin:@localhost:1521/PDB1","jtsdbuser","jtsdbuser");
            // Commit changes manually
            con.setAutoCommit (false);

            //INSERTING records into the table
            if (true) {
                final String deleted_by = "_KGRY4CFWEdq-WY5y7lROQw";
                boolean insertNull = false;
                final String insertStmWithNulls = "INSERT INTO REPOSITORY_DELETED_ITEMS " +
                        "(ITEM_UUID, DELETED_WHEN, DELETED_BY, ITEM_TYPE_DBID, CONTENT_DELETED, STATES_DELETED) " +
                        "VALUES (?, ?, ?, 12, 0, 0)";
                final String insertStmWithOnes = "INSERT INTO REPOSITORY_DELETED_ITEMS " +
                        "(ITEM_UUID, DELETED_WHEN, DELETED_BY, ITEM_TYPE_DBID, CONTENT_DELETED, STATES_DELETED) " +
                        "VALUES (?, ?, ?, 12, 1, 1)";
                final int recordCount2Insert = 100000;
                final String insertStm = (insertNull) ? insertStmWithNulls : insertStmWithOnes ;

                startTime = System.currentTimeMillis();
                for (int i = 0; i < recordCount2Insert; i++) {
                    try {
                        PreparedStatement insertSmt = con.prepareStatement (insertStm);
                        insertSmt.setString (1, uid.generate().getUuidValue());
                        insertSmt.setTimestamp (2, Yesterday);
                        insertSmt.setString (3, deleted_by);
                        insertSmt.executeUpdate ();
                        insertSmt.close ();
                        if (i % 100 == 0) {
                            con.commit ();
                        }
                    } catch (SQLException se) {
                        System.out.println ("Could not load into table");
                        System.out.println ("Exception: " + se);
                        se.printStackTrace ();
                    }
                }
                Duration durationUpload = Duration.ofMillis (1);
                System.out.println ("Duration of uploading "+recordCount2Insert+" into Oracle :" +
                        durationUpload.plusMillis (System.currentTimeMillis () - startTime).toSeconds () +
                        " sec");
                con.commit ();
            }


        }catch(Exception e){ System.out.println(e);}
        System.out.println();

    }
}