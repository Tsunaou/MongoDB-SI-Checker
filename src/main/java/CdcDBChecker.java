import Exceptions.HistoryInvalidException;
import Exceptions.RelationInvalidException;
import History.CdcDB.CdcDBHistory;
import History.CdcDB.CdcDBHistoryReader;
import History.CdcDB.CdcDBTransaction;
import History.MongoDB.MongoDBTransaction;
import IntExt.EXTChecker;
import IntExt.INTChecker;

import java.util.ArrayList;

public class CdcDBChecker {

    public static void checkSIIntExt(String instanceDir) throws HistoryInvalidException, RelationInvalidException {
        String urlHistory = instanceDir + "/history.edn";
        String urlCdcLog = instanceDir + "/cdc.json";
        long begin = System.currentTimeMillis();
        System.out.println("---------------------------------------------------------------------------------");
        System.out.println("Checking history for history.edn at " + urlHistory);
        CdcDBHistory history = CdcDBHistoryReader.readHistory(urlHistory, urlCdcLog);

        INTChecker<CdcDBTransaction> intChecker = new INTChecker<CdcDBTransaction>();
        if (intChecker.checkINT(history)) {
            System.out.println("[INFO] Checking INT Successfully");
        } else {
            System.out.println("[ERROR] Checking INT Failed");
        }

        EXTChecker<CdcDBTransaction> extChecker = new EXTChecker<CdcDBTransaction>();
        if (extChecker.checkEXT(history)) {
            System.out.println("[INFO] Checking EXT Successfully");
        } else {
            System.out.println("[ERROR] Checking EXT Failed");
        }

        long end = System.currentTimeMillis();
        System.out.println("Cost " + (end - begin) + " ms");
    }

    public static void main(String[] args) throws HistoryInvalidException, RelationInvalidException {
        String storeDir = "/Users/ouyanghongrong/github-projects/disalg.dbcdc/store";
        String[] instanceDirs = {
                storeDir + "/dbcdc rw tidb opt SI (SI) /20230328T112027.000+0800",
                storeDir + "/dbcdc rw tidb pess SI (SI) /20230328T112331.000+0800"
        };

        for (String instanceDir: instanceDirs) {
            checkSIIntExt(instanceDir);
        }
    }
}
