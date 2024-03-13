import com.zborsos.dbloaders.db2loader.DB2Loader;
import com.zborsos.dbloaders.derbyloader.DerbyLoader;
import com.zborsos.dbloaders.orcleloader.OracleLoader;

import java.util.logging.Logger;


public class Main {
    final static Logger log = Logger.getLogger("DbLoade.project");
    
    public static void main(String[] args) {
        final String db = "Oracle";
        if(db.equals("DB2")){
            try {
                DB2Loader dbl = new DB2Loader();
                dbl.loadRandomRecords(50000);
                long delCount = dbl.deleteWithEmbeddedSelect();
                log.info("Deleted "+delCount);
                dbl.loadRandomRecords(50000);
                delCount = dbl.deleteSelectedIDs();
                log.info("Deleted "+delCount);
            } catch (Exception e) {
                log.info(e.getMessage());
                throw new RuntimeException(e);
            }

        }

        if(db.equals("Oracle")){
            try {
                OracleLoader dbl = new OracleLoader();
                dbl.loadRandomRecords(50000);
                long delCount = dbl.deleteWithEmbeddedSelect();
                log.info("Deleted "+delCount);
                dbl.loadRandomRecords(50000);
                delCount = dbl.deleteSelectedIDs();
                log.info("Deleted "+delCount);
            } catch (Exception e) {
                log.info(e.getMessage());
                throw new RuntimeException(e);
            }

        }

        if(db.equals("Derby")){
            DerbyLoader dbl = new DerbyLoader();

        }

    }
    
}
