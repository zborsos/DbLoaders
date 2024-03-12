package com.zborsos.dbloaders;

import com.zborsos.dbloaders.db2loader.DB2Loader;

import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        final String db = "DB2";
        if(db.equals("DB2")){
            try {
                DB2Loader dbl = new DB2Loader();
                dbl.loadRandomRecords(50000);
                long delCount = dbl.deleteWithEmbeddedSelect();
                System.out.println("Deleted "+delCount);
                dbl.loadRandomRecords(50000);
                delCount = dbl.deleteSelectedIDs();
                System.out.println("Deleted "+delCount);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }
/*
        if(db.equals("Oracle")){
            OracleLoader dbl = new OracleLoader();
            
;        }

        if(db.equals("Derby")){
            DerbyLoader dbl = new DerbyLoader();

        }
*/
    }
    
}
