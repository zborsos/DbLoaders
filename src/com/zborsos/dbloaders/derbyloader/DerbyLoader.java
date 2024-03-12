package com.zborsos.dbloaders.derbyloader;

import local.utils.uid;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Calendar;
import java.util.Properties;
public class DerbyLoader {
    public static void main(String[] args) throws ClassNotFoundException {
        Timestamp ts = new Timestamp (System.currentTimeMillis ());
        Instant now = Instant.now ();

        Calendar cal = Calendar.getInstance();
        cal.setTime(ts);
        cal.add(Calendar.DATE, -2);
        Timestamp Yesterday = new Timestamp(cal.getTime().getTime());
        try {

            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            String dbURL3 = "jdbc:derby:/Users/zborsos/ServerDerby-M34/server/conf/jts/derby/repositoryDB";
            Properties properties = new Properties();
            properties.put("create", "true");
            properties.put("user", "");
            properties.put("password", "");

            Connection con = DriverManager.getConnection(dbURL3, properties);
            if (con != null) {
                //INSERTING records into the table
                if (true) {
                    final String deleted_by = "_KGRY4CFWEdq-WY5y7lROQw";
                    boolean insertNull = false;
                    final String insertStmWithNulls = "INSERT INTO REPOSITORY.DELETED_ITEMS " +
                            "(ITEM_UUID, DELETED_WHEN, DELETED_BY, ITEM_TYPE_DBID, CONTENT_DELETED, STATES_DELETED) " +
                            "VALUES (?, ?, ?, 12, 0, 0)";
                    final String insertStmWithOnes = "INSERT INTO REPOSITORY.DELETED_ITEMS " +
                            "(ITEM_UUID, DELETED_WHEN, DELETED_BY, ITEM_TYPE_DBID, CONTENT_DELETED, STATES_DELETED) " +
                            "VALUES (?, ?, ?, 12, 1, 1)";
                    final int recordCount2Insert = 11000;
                    final String insertStm = (insertNull) ? insertStmWithNulls : insertStmWithOnes ;

                    long startTime = System.currentTimeMillis();
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
                    System.out.println ("Duration of uploading "+recordCount2Insert+" into DB2 :" +
                            durationUpload.plusMillis (System.currentTimeMillis () - startTime).toSeconds () +
                            " sec");
                    con.commit ();
                }

            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        System.out.println("Hello world!");
    }
}