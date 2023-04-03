package axiom;

import history.History;

public class EXT<KeyType, ValueType> {
    public History<KeyType, ValueType> history;

    public EXT(History<KeyType, ValueType> history) {
        this.history = history;
    }

    public boolean check() {
        // TODO
        return true;
    }
}
