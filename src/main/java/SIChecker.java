import Exceptions.DSGInvalidException;
import Exceptions.HistoryInvalidException;
import Exceptions.RelationInvalidException;

import java.util.Arrays;

public class SIChecker {
    public static void main(String[] args) throws HistoryInvalidException, RelationInvalidException, DSGInvalidException {
        System.out.println("args is " + Arrays.toString(args));
        String type = args[0];
        String base = args[1];
        switch (type) {
            case "MongoDB":
                MongoDBSIChecker.checkAll(base);
                break;
            case "WiredTiger":
                WTSIChecker.checkAll(base);
                break;
            default:
                System.out.println("Please enter a type to check(MongoDB or WiredTiger)");
        }
    }
}
