package reader;

import history.History;

public interface Reader<KeyType, ValueType> {
    History<KeyType, ValueType> read(String filepath);
}
