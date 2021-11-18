package Relation;

import java.util.BitSet;

public class BitSetRelation extends BinaryRelation{

    public int size;
    public BitSet[] relation;

    public BitSetRelation(int size) {
        super(size);
        this.relation = new BitSet[size];

        for (int i = 0; i < size; i++) {
            relation[i] = new BitSet();
            for (int j = 0; j < size; j++) {
                relation[i].set(i, false);
            }
        }
    }

    @Override
    public boolean get(int i, int j) {
        return relation[i].get(j);
    }

    @Override
    public void set(int i, int j, boolean flag) {
        this.relation[i].set(j, flag);
    }

    public static void main(String[] args) {
        int n = 10;
        BitSetRelation relation = new BitSetRelation(n);
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                System.out.print(relation.get(i, j) + " ");
            }
            System.out.println();
        }
    }
}

