package DSG;


import Exceptions.DSGInvalidException;
import History.Transaction;

public class Register<Txn extends Transaction> {
    public Long key;
    public Long value;
    public Register<Txn> preVersion;
    public Register<Txn> nextVersion;
    public Register<Txn> tail;
    public Txn installer; // which transaction install this version

    public Register(Long key, Long value, Txn installer) {
        this.key = key;
        this.value = value;
        this.installer = installer;
        this.preVersion = null;
        this.nextVersion = null;
        this.tail = this;
    }

    public void insert(Register<Txn> reg) throws DSGInvalidException {
        if (isHead()) {
            reg.preVersion = tail;
            tail.nextVersion = reg;
            tail = reg;
        } else {
            throw new DSGInvalidException("Only head can throw message");
        }
    }

    public boolean isHead() {
        return preVersion == null;
    }

    @Override
    public String toString() {
        return "Register{" +
                "key=" + key +
                ", value=" + value +
                ", installer=" + installer.index +
                '}';
    }
}
