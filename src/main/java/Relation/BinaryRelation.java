package Relation;

import Exceptions.RelationInvalidException;

public abstract class BinaryRelation {
    public int size;

    public BinaryRelation(int size) {
        this.size = size;
    }

    abstract public boolean get(int i, int j);

    abstract public void set(int i, int j, boolean flag);

    void printRelations() {
        int count = 0;
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                if (get(i, j)) {
                    System.out.printf("(%d, %d), ", i, j);
                    count = count + 1;
                }
            }
        }
        if (count != 0) {
            System.out.println();
        }
    }

    void union(BinaryRelation rel) throws RelationInvalidException {
        if (this.size != rel.size) {
            throw new RelationInvalidException("Mismatch size when union relations");
        }

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                if (rel.get(i, j)) {
                    addRelation(i, j);
                }
            }
        }
    }

    void addRelation(int i, int j) {
        this.set(i, j, true);
    }

}
