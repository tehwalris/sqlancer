package sqlancer.firebird.ast;

import sqlancer.firebird.FirebirdSchema.FirebirdColumn;

public class FirebirdColumnReference implements FirebirdExpression {

    private final FirebirdColumn c;

    public FirebirdColumnReference(FirebirdColumn c) {
        this.c = c;
    }

    public FirebirdColumn getColumn() {
        return c;
    }

}
