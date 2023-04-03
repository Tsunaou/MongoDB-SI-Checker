package axiom;

import history.History;

public class SESSION<KeyType, ValueType> {
    public History<KeyType, ValueType> history;

    public SESSION(History<KeyType, ValueType> history) {
        this.history = history;
    }

    public boolean check() {
        // TODO
        return true;
    }
}
