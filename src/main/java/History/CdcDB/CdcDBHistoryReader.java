package History.CdcDB;

import Const.Const;
import Exceptions.HistoryInvalidException;
import History.HistoryReader;
import History.OPType;
import History.Operation;
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
                if(readOnly) {
                    continue;
                }
                idx++;
                transactions.add(txn);
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
            // kvPair: 形如 {"v":1,"k":1} 的字符串
            for(String kvPair: obj.keySet()) {
                JSONObject kv = JSON.parseObject(kvPair);
                long key = kv.getLong("k");
                long value = kv.getLong("v");

                if(failedKV.contains(Arrays.asList(key, value))) {
                    // 排除写入失败的 rowChangeEvent
                    continue;
                }

                JSONObject ts = obj.getJSONObject(kvPair);
                long startTs = ts.getLong("start_ts");
                long commitTs = ts.getLong("commit_ts");

                // 其实还有一个 preWrite，暂时忽略

                // 给事务附上时间戳
                int idx = KVTxnMap.get(Arrays.asList(key, value));
                transactions.get(idx).setTimestamp(startTs, commitTs);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return new CdcDBHistory(transactions);
    }

    public static void main(String[] args) throws HistoryInvalidException {
        String storeDir = "/Users/ouyanghongrong/github-projects/disalg.dbcdc/store";
        String[] instanceDirs = {
                "/dbcdc rw tidb opt SI (SI) /20230328T112027.000+0800",
                "/dbcdc rw tidb pess SI (SI) /20230328T112331.000+0800"
        };

        for (String instanceDir: instanceDirs) {
            String baseDir = storeDir + instanceDir;
            String URLHistory = baseDir + "/history.edn";
            String URLCdcLog = baseDir + "/cdc.json";

            CdcDBHistory history = CdcDBHistoryReader.readHistory(URLHistory, URLCdcLog);
            ArrayList<CdcDBTransaction> transactions = history.transactions;
            for(CdcDBTransaction txn: transactions) {
                System.out.println(txn);
            }
        }
    }
}
