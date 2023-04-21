import axiom.*;
import history.History;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.tuple.Pair;
import reader.JSONFileReader;
import reader.Reader;

import java.io.File;

public class SIChecker {
    private static CommandLine commandLine;
    private static Reader<?, ?> reader;
    private static boolean isSI = true;
    private static boolean isSESSIONSI = true;

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        setup(args);

        String filepath = commandLine.getOptionValue("historyPath");
        System.out.println("Checking " + filepath);
        decideReader(filepath);
        boolean equalVIS = Boolean.parseBoolean(commandLine.getOptionValue("equalVIS", "false"));
        Pair<? extends History<?, ?>, Boolean> historyAndIsINT = reader.read(filepath, equalVIS);

        boolean isINT = historyAndIsINT.getRight();
        if (!isINT) {
            System.err.println("Violate INT.");
            isSI = false;
            isSESSIONSI = false;
        }

        long current = System.currentTimeMillis();
        System.out.println("Building history and checking INT: " + (current - start) / 1000.0 + "s");

        History<?, ?> history = historyAndIsINT.getLeft();
        checkAxioms(history);
        long end = System.currentTimeMillis();
        System.out.println("Total: " + (end - start) / 1000.0 + "s");
    }

    private static void setup(String[] args) {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("help").desc("usage help").build());
        options.addOption(Option.builder("historyPath").required().hasArg(true)
                .type(String.class).desc("the filepath of execution history").build());
        options.addOption(Option.builder("equalVIS").hasArg(true)
                .type(Boolean.class).desc("whether to build VIS when timestamps are equal").build());
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp("SI-Checker", options, true);
            System.exit(1);
        }
    }

    private static void decideReader(String filepath) {
        File file = new File(filepath);
        if (!file.exists() || file.isDirectory()) {
            System.err.println("Invalid history path.");
            System.exit(1);
        } else if (filepath.endsWith(".json")) {
            reader = new JSONFileReader();
        } else {
            System.err.println("Invalid history file suffix.");
            System.exit(1);
        }
    }

    private static void checkAxioms(History<?, ?> history) {
        long startEXT = System.currentTimeMillis();
        if (!EXT.check(history)) {
            System.err.println("Violate EXT.");
            isSI = false;
            isSESSIONSI = false;
        }
        long endEXT = System.currentTimeMillis();
        System.out.println("Checking EXT: " + (endEXT - startEXT) / 1000.0 + "s");
        System.out.println("No need to check PREFIX naturally.");
        if (!NOCONFLICT.check(history)) {
            System.err.println("Violate NOCONFLICT.");
            isSI = false;
            isSESSIONSI = false;
        }
        long endNOCONFLICT = System.currentTimeMillis();
        System.out.println("Checking NOCONFLICT: " + (endNOCONFLICT - endEXT) / 1000.0 + "s");
        if (!SESSION.check(history)) {
            System.err.println("Violate SESSION.");
            isSESSIONSI = false;
        }
        long endSESSION = System.currentTimeMillis();
        System.out.println("Checking SESSION: " + (endSESSION - endNOCONFLICT) / 1000.0 + "s");
        if (!isSI) {
            System.out.println("Violate SI.");
        } else if (!isSESSIONSI) {
            System.out.println("Satisfy SI but violate SESSIONSI.");
        } else {
            System.out.println("Satisfy SESSIONSI.");
        }
    }
}
