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
    private static boolean isSESSIONSI = true;

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        setup(args);

        String filepath = commandLine.getOptionValue("historyPath");
        System.out.println("Checking " + filepath);
        decideReader(filepath);
        Pair<? extends History<?, ?>, Boolean> historyAndIsINT = reader.read(filepath);

        long current = System.currentTimeMillis();
        System.out.println("Time cost on building history and checking INT: " + (current - start) / 1000.0 + "s");

        boolean isINT = historyAndIsINT.getRight();
        if (!isINT) {
            System.err.println("Violate INT.");
            isSESSIONSI = false;
        }

        History<?, ?> history = historyAndIsINT.getLeft();
        checkAxioms(history);
        long end = System.currentTimeMillis();
        System.out.println("Total time cost: " + (end - start) / 1000.0 + "s");
    }

    private static void setup(String[] args) {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("help").desc("usage help").build());
        options.addOption(Option.builder("historyPath").required().hasArg(true)
                .type(String.class).desc("the filepath of execution history").build());
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
        } else if (filepath.endsWith(".txt")) {
            // TODO
        } else {
            System.err.println("Invalid history file suffix.");
            System.exit(1);
        }
    }

    private static void checkAxioms(History<?, ?> history) {
        if (!EXT.check(history)) {
            System.err.println("Violate EXT.");
            isSESSIONSI = false;
        }
        if (!PREFIX.check(history)) {
            System.err.println("Violate PREFIX.");
            isSESSIONSI = false;
        }
        if (!NOCONFLICT.check(history)) {
            System.err.println("Violate NOCONFLICT.");
            isSESSIONSI = false;
        }
        if (!SESSION.check(history)) {
            System.err.println("Violate SESSION.");
            isSESSIONSI = false;
        }
        if (isSESSIONSI) {
            System.out.println("Satisfy SESSIONSI.");
        }
    }
}
