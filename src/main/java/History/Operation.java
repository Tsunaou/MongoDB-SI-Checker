package History;

;

public class Operation {
    public OPType type;
    public Long key;
    public Long value;

    public Operation(OPType type, Long key, Long value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "(" + type.toString().charAt(0) +
                " " + key +
                " " + value +
                ')';
    }
}
