package History.CdcDB;

import Const.Const;
import Exceptions.HistoryInvalidException;
import History.HistoryReader;
import History.OPType;
import History.Operation;
import History.Transaction;
import TestUtil.Parameter;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONReader;
import com.alibaba.fastjson.JSON;
import us.bpsm.edn.parser.Parseable;
import us.bpsm.edn.parser.Parser;
import us.bpsm.edn.parser.Parsers;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class CdcDBHistoryReader extends HistoryReader {

    public static CdcDBHistory readHistory(String urlHistory, String urlCdcLog) throws HistoryInvalidException {
        ArrayList<CdcDBTransaction> transactions = new ArrayList<>();
        HashMap<List<Long>, Integer> KVTxnMap = new HashMap<>();
        HashSet<List<Long>> failedKV = new HashSet<>();

        Parameter param = Parameter.parse(urlHistory); // 当前的测试参数
        int concurrency = param.concurrency;
        // 创建一个长度为 concurrency 的数组，表示进程的状态，初始值均为 false
        boolean[] slotStatus = new boolean[concurrency];
        int filledCount = 0; // 已填充的槽数量
        ArrayList<CdcDBTransaction> firstTxnOnEachProcess = new ArrayList<>();

        // 1. Reading history.edn
        try {
            BufferedReader in = new BufferedReader(new FileReader(urlHistory));
            String line;
            int idx = 0;
            while ((line = in.readLine()) != null) {
                Parseable pbr = Parsers.newParseable(line);
                Parser p = Parsers.newParser(Parsers.defaultConfiguration());
                Map<?, ?> m = (Map<?, ?>) p.nextValue(pbr);
                List<?> values = (List<?>) m.get(value);

                if (m.get(type) == invoke) {
                    continue;
                }

                // 暂时记录 history.edn 中写冲突的 kv 对
                if (m.get(type) != ok) {
                    for (Object o : values) {
                        List<?> ops = (List<?>) o;
                        if (ops.get(0) == w) {
                            long key = (Long) ops.get(1);
                            long val = (Long) ops.get(2);
                            failedKV.add(Arrays.asList(key, val));
                        }
                    }
                    continue;
                }

                Long session = (Long) m.get(process);
                CdcDBTransaction txn = new CdcDBTransaction(session);
                boolean readOnly = true;
                for (Object o : values) {
                    List<?> ops = (List<?>) o;
                    long key = (Long) ops.get(1);
                    long val;
                    try {
                        val = (Long) ops.get(2);
                    } catch (NullPointerException e) {
                        val = Const.INIT_READ;
                    }
                    OPType type = null;
                    if (ops.get(0) == w) {
                        type = OPType.write;
                        readOnly = false;
                        KVTxnMap.put(Arrays.asList(key, val), idx);
                    } else {
                        type = OPType.read;
                    }
                    txn.addOperation(new Operation(type, key, val));
                }
                if (readOnly) {
                    continue;
                }
                idx++;
                transactions.add(txn);

                if (filledCount != concurrency) {
                    int soltIndex = session.intValue() % concurrency; // 计算实际对应的进程
                    if (!slotStatus[soltIndex] && !txn.writeKeySet.isEmpty()) {
                        slotStatus[soltIndex] = true;
                        firstTxnOnEachProcess.add(txn);
                        filledCount++;
                    }
                }

            }
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 2. Reading cdc.json
        JSONReader reader;
        try {
            reader = new JSONReader(new FileReader(urlCdcLog));
            JSONObject obj = (JSONObject) reader.readObject();

            long baseStartTs = Long.MAX_VALUE;
            long baseCommitTs = Long.MAX_VALUE;
            for (CdcDBTransaction txn : firstTxnOnEachProcess) {
                Operation w = txn.writes.get(txn.writes.size() - 1);
                String kvPair = String.format("{'k': %d, 'v': %d}", w.key, w.value);
                JSONObject ts = obj.getJSONObject(kvPair);
                long startTs = ts.getLong("start_ts");
                long commitTs = ts.getLong("commit_ts");
                baseStartTs = Math.min(baseStartTs, startTs);
                baseCommitTs = Math.min(baseCommitTs, commitTs);
            }

            // kvPair: 形如 {'v': 1,'k' :1} 的字符串
            for (String kvPair : obj.keySet()) {
                JSONObject kv = JSON.parseObject(kvPair);
                long key = kv.getLong("k");
                long value = kv.getLong("v");

                JSONObject ts = obj.getJSONObject(kvPair);
                long startTs = ts.getLong("start_ts");
                long commitTs = ts.getLong("commit_ts");

                // 其实还有一个 preWrite，暂时忽略
                if (commitTs < baseStartTs) {
                    continue;
                }

                if (failedKV.contains(Arrays.asList(key, value))) {
                    // 排除写入失败的 rowChangeEvent
                    System.out.println("Maybe failed rowChangedEvent of " + Arrays.asList(key, value));
                    continue;
                }

                // 给事务附上时间戳
                int idx = 0;
                try {
                    idx = KVTxnMap.get(Arrays.asList(key, value));
                } catch (NullPointerException e) {
                    // 由于 TiCDC 的逻辑是 at least once，所以可以会搜集到上一轮的写值
                    System.out.println("Maybe last round key-value: " + key + "," + value + ", guess:" + (commitTs < baseStartTs));
                    continue;
                }
                CdcDBTransaction txn = transactions.get(idx);
                try {
                    txn.setTimestamp(startTs, commitTs);
                } catch (HistoryInvalidException e) {
                    System.out.println("Invalid Timestamp of: " + key + "," + value);
                    System.out.println("Previous is" + txn.startTimestamp + "," + txn.commitTimestamp);
                    System.out.println("Current is" + startTs + "," + commitTs);
                    throw e;
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return new CdcDBHistory(transactions);
    }

    public static void main(String[] args) throws HistoryInvalidException, FileNotFoundException {
        String storeDir = "/Users/ouyanghongrong/github-projects/disalg.dbcdc/store";
        String[] instanceDirs = {
                "/dbcdc rw tidb opt SI (SI) /20230328T112027.000+0800",
                "/dbcdc rw tidb pess SI (SI) /20230328T112331.000+0800"
        };

        for (String instanceDir : instanceDirs) {
            String baseDir = storeDir + instanceDir;
            String URLHistory = baseDir + "/history.edn";
            String URLCdcLog = baseDir + "/cdc.json";

            CdcDBHistory history = CdcDBHistoryReader.readHistory(URLHistory, URLCdcLog);
            ArrayList<CdcDBTransaction> transactions = history.transactions;
            for (CdcDBTransaction txn : transactions) {
                System.out.println(txn);
            }
        }
    }
}
