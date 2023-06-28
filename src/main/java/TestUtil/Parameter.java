package TestUtil;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parameter {

    public int txnNumber;
    public int concurrency;
    public int maxTxnLength;

    public static Parameter currentParam = null;

    Parameter(int txnNumber, int concurrency, int maxTxnLength) {
        this.txnNumber = txnNumber;
        this.concurrency = concurrency;
        this.maxTxnLength = maxTxnLength;
    }

    public static Parameter parse(String path) {
        // 定义正则表达式，匹配数字参数
        String regex = "num\\s+(\\d+)\\s+con\\s+(\\d+)\\s+len\\s+(\\d+)";
        
        // 编译正则表达式，创建 Matcher 对象
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(path);
        
        // 如果匹配成功，提取数字参数并打印
        if (matcher.find()) {
            int num = Integer.parseInt(matcher.group(1));
            int con = Integer.parseInt(matcher.group(2));
            int len = Integer.parseInt(matcher.group(3));
            return new Parameter(num, con, len);
        }
//        return null;
        return new Parameter(500,50,4);
    }

    @Override
    public String toString() {
        return "txn-num:" + txnNumber + ", concurrency:" + concurrency +", max-txn-length:" + maxTxnLength;
    }

    public static void main(String[] args) {
        String path = "/data/home/tsunaouyang/github-projects/dbcdc-runner/store/dbcdc rw tidb pess num 5000 con 9 len 12 SI (SI) /latest";
        System.out.println(Parameter.parse(path));
    }
}
