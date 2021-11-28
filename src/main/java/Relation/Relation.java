package Relation;

import Exceptions.RelationInvalidException;
import History.History;
import History.Transaction;

import java.util.HashSet;

public class Relation<Txn extends Transaction> {
    public History<Txn> history;
    public BinaryRelation relation;
    public int n;

    public Relation(int n) {
        this.n = n;
        this.relation = new BitSetRelation(n);
    }

    public void addRelation(int a, int b) {
        relation.addRelation(a, b);
    }

    public void printRelations() {
        relation.printRelations();
    }

    public void calculateRelation(History<Txn> history) throws RelationInvalidException {
        this.history = history;
        System.out.println("Calculation " + this.getClass().getName());
    }

    public void union(Relation<Txn> r) throws RelationInvalidException {
        relation.union(r.relation);
    }

    public HashSet<Txn> relationLeft(Txn txn) {
        HashSet<Txn> left = new HashSet<>();
        int n = this.history.transactions.size();
        for (int i = 0; i < n; i++) {
            if (this.relation.get(i, txn.index)) {
                left.add(history.transactions.get(i));
            }
        }
        return left;
    }
}
