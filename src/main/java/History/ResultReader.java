package History;

import Const.Const;
import History.MongoDB.MongoDBTransaction;
import us.bpsm.edn.parser.Parseable;
import us.bpsm.edn.parser.Parser;
import us.bpsm.edn.parser.Parsers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import History.HistoryReader;

public class ResultReader {

    static class Stats{
        public long count;
        public long okCount;
        public long failCount;
        public long infoCount;

        @Override
        public String toString() {
            return "History Status: [" +
                    "count=" + count +
                    ", okCount=" + okCount +
                    ", failCount=" + failCount +
                    ", infoCount=" + infoCount +
                    ']';
        }
    }

    public static void report(String url){
        try {
            BufferedReader in = new BufferedReader(new FileReader(url));
            String line;
            int idx = 0;
            StringBuilder result = new StringBuilder();
            while ((line = in.readLine()) != null) {
                result.append(line);
            }
            Parseable pbr = Parsers.newParseable(result.toString());
            Parser p = Parsers.newParser(Parsers.defaultConfiguration());
            Map<?, ?> m = (Map<?, ?>) p.nextValue(pbr);
            Map<?, ?> statsMap = (Map<?, ?>)m.get(HistoryReader.stats);
            Stats stats = new Stats();
            stats.count = (long) statsMap.get(HistoryReader.count);
            stats.okCount = (long) statsMap.get(HistoryReader.okCount);
            stats.failCount = (long) statsMap.get(HistoryReader.failCount);
            stats.infoCount = (long) statsMap.get(HistoryReader.infoCount);
            System.out.println(stats);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        report("D:\\Education\\Programs\\Java\\MongoDB-SI-Checker\\src\\main\\resources\\store-1120\\sharded-cluster\\20211120T080401.000Z\\results.edn");
    }
}
