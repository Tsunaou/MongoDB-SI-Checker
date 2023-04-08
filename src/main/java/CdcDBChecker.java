import Exceptions.HistoryInvalidException;
import Exceptions.RelationInvalidException;
import History.CdcDB.CdcDBHistory;
import History.CdcDB.CdcDBHistoryReader;
import History.CdcDB.CdcDBTransaction;
import History.ResultReader;
import IntExt.EXTChecker;
import IntExt.INTChecker;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Objects;

public class CdcDBChecker {

    public static boolean checkFileExists(String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            System.out.println(filePath + " not exists.");
            return false;
        }
        return true;
    }

    public static void checkSIIntExt(String instanceDir) throws HistoryInvalidException, RelationInvalidException {
        String urlHistory = instanceDir + "/history.edn";
        String urlCdcLog = instanceDir + "/cdc.json";

        if(!checkFileExists(urlHistory) || !checkFileExists(urlCdcLog)) {
            return;
        }

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

    public static void checkAll(String base) throws RelationInvalidException, HistoryInvalidException {
        File store = new File(base);
        for (File file : Objects.requireNonNull(store.listFiles())) {
            if (file.isDirectory()) {
                for (File data : Objects.requireNonNull(file.listFiles())) {
                    if (data.isDirectory() && !data.getPath().contains("latest")) {
                        String urlResult = data.getPath() + "/results.edn";
                        try{
                            checkSIIntExt(data.getPath());
                        }catch (NullPointerException e) {
                            System.out.println("Nullptr");
                        }catch (HistoryInvalidException e) {
                            System.out.println("To be checked");
                        }

                        if (new File(urlResult).exists()) {
                            ResultReader.report(urlResult);
                        }
                    }
                }
            }
        }
    }

    public static void checkSample() throws HistoryInvalidException, RelationInvalidException, FileNotFoundException {
        String storeDir = "/Users/ouyanghongrong/github-projects/disalg.dbcdc/store-typical";
        String[] instanceDirs = {
                storeDir + "/dbcdc rw tidb opt SI (SI) /20230328T112027.000+0800",
                storeDir + "/dbcdc rw tidb pess SI (SI) /20230328T112331.000+0800"
        };

        for (String instanceDir: instanceDirs) {
            checkSIIntExt(instanceDir);
        }
    }


    public static void main(String[] args) throws HistoryInvalidException, RelationInvalidException {
        checkAll("/Users/ouyanghongrong/github-projects/disalg.dbcdc/store");
//        checkSample();
    }
}
