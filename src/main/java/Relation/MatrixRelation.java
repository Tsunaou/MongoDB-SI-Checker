package Relation;

import Exceptions.RelationInvalidException;

public class MatrixRelation extends BinaryRelation{

    public boolean[][] relation;

    public MatrixRelation(int size) {
        super(size);
        this.relation = new boolean[size][size];
    }

    @Override
    public boolean get(int i, int j) {
        return relation[i][j];
    }

    @Override
    public void set(int i, int j, boolean flag) {
        relation[i][j] = flag;
    }

}
