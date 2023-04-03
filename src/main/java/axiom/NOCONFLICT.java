package axiom;

import history.History;

public class NOCONFLICT<KeyType, ValueType> {
    public History<KeyType, ValueType> history;

    public NOCONFLICT(History<KeyType, ValueType> history) {
        this.history = history;
    }

    public boolean check() {
        // TODO
        return true;
    }
}
