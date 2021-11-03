package History;

import us.bpsm.edn.Keyword;

import static us.bpsm.edn.Keyword.newKeyword;

public class HistoryReader {
    public static Keyword type = newKeyword("type");
    public static Keyword ok = newKeyword("ok");
    public static Keyword process = newKeyword("process");
    public static Keyword value = newKeyword("value");
    public static Keyword startTimestamp = newKeyword("start-timestamp");
    public static Keyword commitTimestamp = newKeyword("commit-timestamp");
    public static Keyword w = newKeyword("w");
    public static Keyword r = newKeyword("r");

}
