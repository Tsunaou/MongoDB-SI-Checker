package reader;

import history.History;
import org.apache.commons.lang3.tuple.Pair;

public interface Reader<KeyType, ValueType> {
    Pair<History<KeyType, ValueType>, Boolean> read(String filepath);
}
