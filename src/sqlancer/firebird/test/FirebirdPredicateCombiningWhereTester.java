package sqlancer.firebird.test;

import java.sql.SQLException;

import sqlancer.common.ast.newast.Node;
import sqlancer.firebird.FirebirdToStringVisitor;
import sqlancer.firebird.FirebirdProvider.FirebirdGlobalState;
import sqlancer.firebird.ast.FirebirdExpression;

public class FirebirdPredicateCombiningWhereTester extends FirebirdPredicateCombiningBase {

    public FirebirdPredicateCombiningWhereTester(FirebirdGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();

        //Node<FirebirdExpression> combinedPredicate = getCombinedPredicate();

    }
}
