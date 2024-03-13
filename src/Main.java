import com.zborsos.dbloaders.db2loader.DB2Loader;
import com.zborsos.dbloaders.derbyloader.DerbyLoader;
import com.zborsos.dbloaders.orcleloader.OracleLoader;

public class Main {
    public static void main(String[] args) {
        final String db = "Oracle";
        if(db.equals("DB2")){
            try {
                DB2Loader dbl = new DB2Loader();
                dbl.loadRandomRecords(50000);
                long delCount = dbl.deleteWithEmbeddedSelect();
                System.out.println("Deleted "+delCount);
                dbl.loadRandomRecords(50000);
                delCount = dbl.deleteSelectedIDs();
                System.out.println("Deleted "+delCount);
            } catch (Exception e) {
                System.out.println(e.getMessage());
                throw new RuntimeException(e);
            }

        }

        if(db.equals("Oracle")){
            try {
                OracleLoader dbl = new OracleLoader();
                dbl.loadRandomRecords(50000);
                long delCount = dbl.deleteWithEmbeddedSelect();
                System.out.println("Deleted "+delCount);
                dbl.loadRandomRecords(50000);
                delCount = dbl.deleteSelectedIDs();
                System.out.println("Deleted "+delCount);
            } catch (Exception e) {
                System.out.println(e.getMessage());
                throw new RuntimeException(e);
            }

        }

        if(db.equals("Derby")){
            DerbyLoader dbl = new DerbyLoader();

        }

    }
    
}
