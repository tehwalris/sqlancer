package sqlancer.firebird.test;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.firebird.FirebirdKnownPredicate;
import sqlancer.firebird.FirebirdToStringVisitor;
import sqlancer.firebird.FirebirdProvider.FirebirdGlobalState;

public class FirebirdPredicateCombiningDistinctTester extends FirebirdPredicateCombiningBase {
	
	public FirebirdPredicateCombiningDistinctTester(FirebirdGlobalState state) {
		super(state);
	}
	
	@Override
	public void check() throws SQLException {
		super.check();
		
		FirebirdKnownPredicate combinedPredicate = getCombinedPredicate();
		select.setWhereClause(combinedPredicate.getExpression());
		select.setDistinct(true);
		String combinedQueryString = FirebirdToStringVisitor.asString(select);
		List<String> predicateStrings = predicates.stream().map(e -> FirebirdToStringVisitor.asString(e))
				.collect(Collectors.toList());
		
		List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(combinedQueryString, errors, state);
		List<String> expectedResultSet = getExpectedResultsNoDuplicatesFirstColumn(tableContent,
				combinedPredicate.getExpectedResults());
		ComparatorHelper.assumeResultSetsAreEqual(resultSet, expectedResultSet, combinedQueryString, predicateStrings, 
				state);
	}
}
