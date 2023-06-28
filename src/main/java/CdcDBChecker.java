import Exceptions.HistoryInvalidException;
import Exceptions.RelationInvalidException;
import History.CdcDB.CdcDBHistory;
import History.CdcDB.CdcDBHistoryReader;
import History.CdcDB.CdcDBTransaction;
import History.MongoDB.LogicalClock;
import History.ResultReader;
import History.Transaction;
import IntExt.EXTChecker;
import IntExt.INTChecker;
import TestUtil.Parameter;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

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

        LogicalClock initalClock = new LogicalClock(Long.MAX_VALUE, Long.MAX_VALUE);
        for(Transaction txn: history.transactions) {
            if(txn.commitTimestamp.compareTo(initalClock) == 0) {
                System.out.println(txn);
            }
        }
        System.out.println(history.transactions.size());

        INTChecker<CdcDBTransaction> intChecker = new INTChecker<CdcDBTransaction>();
        if (intChecker.checkINT(history)) {
            System.out.println("[INFO] Checking INT Successfully");
        } else {
            System.out.println("[ERROR] Checking INT Failed");
        }

        EXTChecker<CdcDBTransaction> extChecker = new EXTChecker<CdcDBTransaction>();
        if (extChecker.checkEXT(history)) {
//            System.out.println("[INFO] Checking EXT Successfully");
        } else {
            System.out.println("[ERROR] Checking EXT Failed");
        }

        long end = System.currentTimeMillis();
        System.out.println("Cost " + (end - begin) + " ms");
    }

    public static void checkLatest(String base) throws RelationInvalidException, HistoryInvalidException {
        String latest = "/data/home/tsunaouyang/github-projects/dbcdc-runner/store/dbcdc rw tidb pess num 5000 con 9 len 12 SI (SI) /latest";
        checkSIIntExt(latest);
    }

    public static void checkAll(String base) throws RelationInvalidException, HistoryInvalidException {
        File store = new File(base);

        TreeMap<String, ArrayList<Long>> results = new TreeMap<>();

        for (File file : Objects.requireNonNull(store.listFiles())) {
            if(file.getName().contains("latest") || file.getName().contains("current")) {
                continue;
            }
            if (file.isDirectory()) {
                System.out.println(file.getAbsolutePath());
                Parameter.currentParam = Parameter.parse(file.getAbsolutePath());
                String param;
                if(file.getAbsolutePath().contains("pess")) {
                    param = "Pess:" + Parameter.currentParam.toString();
                } else {
                    param = "Opt:" + Parameter.currentParam.toString();
                }
                if(!results.containsKey(param)) {
                    results.put(param, new ArrayList<>());
                }
                ArrayList<Long> times = results.get(param);
                for (File data : Objects.requireNonNull(file.listFiles())) {
                    if (data.isDirectory() && !data.getPath().contains("latest")) {
                        String urlResult = data.getPath() + "/results.edn";
                        try{
                            long start = System.currentTimeMillis();
                            checkSIIntExt(data.getPath());
                            long end = System.currentTimeMillis();
                            times.add(end-start);
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

        for(Map.Entry<String, ArrayList<Long>> entry: results.entrySet()) {
            String param = entry.getKey();
            ArrayList<Long> times = entry.getValue();
            double avgTime = times.stream().mapToLong(Long::longValue).average().orElse(0.0);
            System.out.println(param + ":    " + avgTime + "ms");
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
        String osType = System.getProperty("os.name").toLowerCase();
        System.out.println("Running CdcDBChecker in " + osType);
        String storePath;
        if(osType.contains("mac")) {
            storePath = "/Users/ouyanghongrong/github-projects/disalg.dbcdc/store-base";
        } else if(osType.contains("linux")) {
            storePath = "/data/home/tsunaouyang/github-projects/dbcdc-runner/store";
        } else{
            System.err.println("Invalid system type. Return");
            return;
        }

//        checkAll(storePath);
        // checkLatest(storePath);
        checkSIIntExt("/Users/ouyanghongrong/github-projects/disalg.dbcdc/store-base/dbcdc rw tidb opt SI (SI) /latest");
    }
}
