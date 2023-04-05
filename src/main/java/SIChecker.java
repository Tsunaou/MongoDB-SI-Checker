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
        setup(args);

        String filepath = commandLine.getOptionValue("historyPath");
        decideReader(filepath);
        History<?, ?> history = reader.read(filepath);

        checkAxioms(history);
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
        if (!(new INT<>(history).check())) {
            System.err.println("Violate INT.");
        }
        if (!(new EXT<>(history).check())) {
            System.err.println("Violate EXT.");
        }
        if (!(new PREFIX<>(history).check())) {
            System.err.println("Violate PREFIX.");
        }
        if (!(new NOCONFLICT<>(history).check())) {
            System.err.println("Violate NOCONFLICT.");
        }
        if (!(new SESSION<>(history).check())) {
            System.err.println("Violate SESSION.");
        }
    }
}
