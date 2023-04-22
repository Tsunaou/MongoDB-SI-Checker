package axiom;

import history.Edge;
import history.History;
import history.transaction.Transaction;

public class SESSION {
    public static <KeyType, ValueType> boolean check(History<KeyType, ValueType> history, boolean equalVIS) {
        if (equalVIS) {
            return checkEqual(history);
        } else {
            return checkNonEqual(history);
        }
    }

    private static <KeyType, ValueType> boolean checkNonEqual(History<KeyType, ValueType> history) {
        return history.getSO().parallelStream().allMatch(soEdge -> soEdge.getFrom().getCommitTimestamp()
                .compareTo(soEdge.getTo().getStartTimestamp()) < 0);
    }

    private static <KeyType, ValueType> boolean checkEqual(History<KeyType, ValueType> history) {
        return history.getSO().parallelStream().allMatch(soEdge -> soEdge.getFrom().getCommitTimestamp()
                .compareTo(soEdge.getTo().getStartTimestamp()) <= 0);
    }

    public static <KeyType, ValueType> boolean checkSingle(History<KeyType, ValueType> history) {
        for (Edge<Transaction<KeyType, ValueType>> soEdge : history.getSO()) {
            if (soEdge.getFrom().getCommitTimestamp().compareTo(soEdge.getTo().getStartTimestamp()) > 0) {
                return false;
            }
        }
        return true;
    }
}
