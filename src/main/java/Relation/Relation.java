package Relation;

import Exceptions.RelationInvalidException;
import History.History;
import History.Transaction;
import History.WiredTiger.WiredTigerHistory;

public class Relation<Txn extends Transaction> {
    public boolean[][] relation;
    public int n;

    public Relation(int n) {
        this.n = n;
        this.relation = new boolean[n][n];
    }

    public void addRelation(int a, int b) {
        if (!relation[a][b]) {
            relation[a][b] = true;
        }
    }

    public void printRelations() {
        int count = 0;
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                if (relation[i][j]) {
                    System.out.printf("(%d, %d), ", i, j);
                    count = count + 1;
                }
            }
        }
        if (count != 0) {
            System.out.println();
        }
        System.out.println(this.getClass().getName() + " has " + count + " relations");
    }

    public void calculateRelation(History<Txn> history) throws RelationInvalidException {
        System.out.println("Calculation " + this.getClass().getName());
    }

    public void union(Relation<Txn> r) throws RelationInvalidException {
        if (this.n != r.n) {
            throw new RelationInvalidException("Mismatch size when union relations");
        }

        boolean[][] r1 = r.relation;

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                if(r1[i][j]){
                    addRelation(i, j);
                }
            }
        }
    }
    public void union(Relation<Txn> s1, Relation<Txn> s2) throws RelationInvalidException {
        if (s1.n != s2.n) {
            throw new RelationInvalidException("Mismatch size when union relations");
        }

        boolean[][] r1 = s1.relation;
        boolean[][] r2 = s2.relation;

        int n = s1.n;
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                if (r1[i][j] || r2[i][j]) {
                    addRelation(i, j);
                }
            }
        }
    }

}
