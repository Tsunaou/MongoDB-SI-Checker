import axiom.*;
import history.History;
import org.apache.commons.cli.*;
import reader.JSONFileReader;
import reader.Reader;

import java.io.File;

public class SIChecker {
    private static CommandLine commandLine;
    private static Reader<?, ?> reader;

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        setup(args);

        String filepath = commandLine.getOptionValue("historyPath");
        System.out.println("Checking " + filepath);
        decideReader(filepath);
        History<?, ?> history = reader.read(filepath);

        checkAxioms(history);
        long end = System.currentTimeMillis();
        System.out.println("time cost: " + (end - start) / 1000.0 + "s");
    }

    private static void setup(String[] args) {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("help").desc("usage help").build());
        options.addOption(Option.builder("historyPath").required().hasArg(true)
                .type(String.class).desc("the filepath or directory of execution history").build());
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
        if (!file.exists()) {
            System.err.println("Invalid history path.");
            System.exit(1);
        } else if (file.isDirectory()) {
            // TODO
        } else if (filepath.endsWith(".json")) {
            reader = new JSONFileReader();
        } else if (filepath.endsWith(".txt")) {
            // TODO
        } else {
            System.err.println("Invalid history format.");
            System.exit(1);
        }
    }

    private static void checkAxioms(History<?, ?> history) {
        boolean isSatisfied = true;
        if (!INT.check(history)) {
            System.err.println("Violate INT.");
            isSatisfied = false;
        }
        if (!EXT.check(history)) {
            System.err.println("Violate EXT.");
            isSatisfied = false;
        }
        if (!PREFIX.check(history)) {
            System.err.println("Violate PREFIX.");
            isSatisfied = false;
        }
        if (!NOCONFLICT.check(history)) {
            System.err.println("Violate NOCONFLICT.");
            isSatisfied = false;
        }
        if (!SESSION.check(history)) {
            System.err.println("Violate SESSION.");
            isSatisfied = false;
        }
        if (isSatisfied) {
            System.out.println("Satisfy SESSIONSI.");
        }
    }
}
