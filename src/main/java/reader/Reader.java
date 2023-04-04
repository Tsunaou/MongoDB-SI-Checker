package reader;

import history.Session;
import history.transaction.Transaction;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;

public abstract class Reader {
    public abstract Pair<ArrayList<Transaction<Long, Long>>, ArrayList<Session<Long, Long>>> read(String filepath);
}
