package axiom;

import history.History;

public class PREFIX<KeyType, ValueType> {
    public History<KeyType, ValueType> history;

    public PREFIX(History<KeyType, ValueType> history) {
        this.history = history;
    }

    public boolean check() {
        // TODO
        return true;
    }
}
