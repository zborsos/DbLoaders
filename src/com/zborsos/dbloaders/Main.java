package com.zborsos.dbloaders;

import com.zborsos.dbloaders.db2loader.DB2Loader;
import com.zborsos.dbloaders.orcleloader.OracleLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {

    final Logger log = LoggerFactory.getLogger(this.getClass().getName());
    public static void main(String[] args) {
        final String db = "Oracle";
        if(db.equals("DB2")){
            try {
                DB2Loader dbl = new DB2Loader();
                dbl.loadRandomRecords(500000);
                dbl.deleteWithEmbeddedSelect();

                dbl.loadRandomRecords(500000);
                dbl.deleteSelectedIDs();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        if(db.equals("Oracle")){
            try {
                long recordCount = 1000;
                OracleLoader dbl = new OracleLoader();
                dbl.loadRandomRecords(recordCount);
                dbl.deleteWithEmbeddedSelect();

                dbl.loadRandomRecords(recordCount);
                dbl.deleteSelectedIDs();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
    
}
