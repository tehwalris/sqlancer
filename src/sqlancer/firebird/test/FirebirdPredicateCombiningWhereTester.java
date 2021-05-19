package sqlancer.firebird.test;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.firebird.FirebirdToStringVisitor;
import sqlancer.firebird.FirebirdKnownPredicate;
import sqlancer.firebird.FirebirdProvider.FirebirdGlobalState;

public class FirebirdPredicateCombiningWhereTester extends FirebirdPredicateCombiningBase {

    public FirebirdPredicateCombiningWhereTester(FirebirdGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();

        FirebirdKnownPredicate combinedPredicate = getCombinedPredicate();
        select.setWhereClause(combinedPredicate.getExpression());
        String combinedQueryString = FirebirdToStringVisitor.asString(select);
        List<String> predicateStrings = predicates.stream().map(e -> FirebirdToStringVisitor.asString(e))
                .collect(Collectors.toList());

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(combinedQueryString, errors, state);
        List<String> expectedResultSet = getExpectedResultsFirstColumn(tableContent,
                combinedPredicate.getExpectedResults());
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, expectedResultSet, combinedQueryString, predicateStrings,
                state);

    }
}
