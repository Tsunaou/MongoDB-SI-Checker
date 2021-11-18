package Relation;

import Exceptions.RelationInvalidException;
import History.History;
import History.Transaction;

public class Relation<Txn extends Transaction> {
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
        System.out.println("Calculation " + this.getClass().getName());
    }

    public void union(Relation<Txn> r) throws RelationInvalidException {
        relation.union(r.relation);
    }
}
