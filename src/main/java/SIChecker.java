import history.Session;
import history.transaction.Transaction;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.tuple.Pair;
import reader.JSONFileReader;

import java.io.File;
import java.util.ArrayList;

public class SIChecker {
    private static CommandLine commandLine;

    public static void main(String[] args) {
        setup(args);

        String filepath = commandLine.getOptionValue("historyPath");
        File file = new File(filepath);
        if (!file.exists()) {
            System.err.println("Invalid history path.");
            System.exit(1);
        } else if (file.isDirectory()) {
            // TODO
        } else if (filepath.endsWith(".json")) {
            JSONFileReader fileReader = new JSONFileReader();
            Pair<ArrayList<Transaction<Long, Long>>, ArrayList<Session<Long, Long>>>
                    transactionsAndSessions = fileReader.read(filepath);
        } else if (filepath.endsWith(".txt")) {
            // TODO
        } else {
            System.err.println("Invalid history format.");
            System.exit(1);
        }
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
}
