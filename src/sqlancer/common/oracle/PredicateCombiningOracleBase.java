package sqlancer.common.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.SQLGlobalState;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;

/**
 * @param <E>
 *            the expression type
 * @param <S>
 *            the global state type
 */
public abstract class PredicateCombiningOracleBase<E, S extends SQLGlobalState<?, ?>> implements TestOracle {

    protected List<E> predicates;
    protected List<List<Boolean>> predicateEvaluations;
    protected List<List<String>> tableContent;

    protected final S state;
    protected final ExpectedErrors errors = new ExpectedErrors();

    protected PredicateCombiningOracleBase(S state) {
        this.state = state;
    }

    protected E generatePredicate() {
        return getGen().generatePredicate();
    }

    protected void initializeInternalTables(int numPredicates, int numColumns) {
        ExpressionGenerator<E> gen = getGen();
        if (gen == null) {
            throw new IllegalStateException();
        }
        predicates = new ArrayList<>();
        predicateEvaluations = new ArrayList<>();
        for (int i = 0; i < numPredicates; i++) {
            E predicate = generatePredicate();
            if (predicate == null) {
                throw new IllegalStateException();
            }
            predicates.add(predicate);
            predicateEvaluations.add(new ArrayList<>());
        }
        tableContent = new ArrayList<>();
        for (int i = 0; i < numColumns; i++) {
            tableContent.add(new ArrayList<>());
        }
    }

    protected void generateTables(String queryString, int numColumns, int numPredicates) throws SQLException {
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors);
        SQLancerResultSet result = null;
        try {
            result = q.executeAndGet(state);
            if (result == null) {
                throw new IgnoreMeException();
            }
            while (result.next()) {
                for (int i = 1; i <= numColumns; i++) {
                    tableContent.get(i - 1).add(result.getString(i));
                }
                for (int i = 1; i <= numPredicates; i++) {
                    String predicate = result.getString(numColumns + i);
                    predicateEvaluations.get(i - 1).add(predicateEvaluationToBoolean(predicate));
                }
            }
        } catch (Exception e) {
            if (e instanceof IgnoreMeException) {
                throw e;
            }
            if (e instanceof NumberFormatException) {
                // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/57
                throw new IgnoreMeException();
            }
            if (e.getMessage() == null) {
                throw new AssertionError(queryString, e);
            }
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            }
            throw new AssertionError(queryString, e);
        } finally {
            if (result != null && !result.isClosed()) {
            	try {
            		result.close();
            	} catch (Exception e) {
            		if (e.getMessage() != null && errors.errorIsExpected(e.getMessage())) {
            			throw new IgnoreMeException();
            		}
            		throw e;
            	}
            }
        }
    }

    protected abstract ExpressionGenerator<E> getGen();

    protected Boolean predicateEvaluationToBoolean(String v) {
        if (v == null) {
            return null;
        }
        switch (v) {
        case "true":
            return true;
        case "false":
            return false;
        default:
            throw new AssertionError(v);
        }
    }

    protected static List<String> getExpectedResultsFirstColumn(List<List<String>> tableContent,
            List<Boolean> expectedResults) {
        List<List<String>> fullOutput = getExpectedResults(tableContent, expectedResults);
        int numRows = fullOutput.get(0).size();
        List<String> output = new ArrayList<>();
        for (int i = 0; i < numRows; i++) {
            output.add(fullOutput.get(0).get(i));
        }
        return output;
    }
    
    protected static List<String> getExpectedResultsNoDuplicatesFirstColumn(List<List<String>> tableContent,
    		List<Boolean> expectedResults) {
    	List<List<String>> fullOutput = getExpectedResults(tableContent, expectedResults);
    	int numRows = fullOutput.get(0).size();
    	int numColumns = fullOutput.size();
    	HashSet<List<String>> distinctRows = new HashSet<List<String>>();
    	for (int i = 0; i < numRows; i++) {
    		List<String> row = new ArrayList<>();
    		for (int j = 0; j < numColumns; j++) {
    			row.add(fullOutput.get(j).get(i));
    		}
    		distinctRows.add(row);
    	}
    	List<String> output = new ArrayList<>();
    	for (List<String> row : distinctRows) {
    		output.add(row.get(0));
    	}
    	return output;
    }

    protected static List<List<String>> getExpectedResults(List<List<String>> tableContent,
            List<Boolean> expectedResults) {
        int numRows = expectedResults.size();
        int numColumns = tableContent.size();
        List<List<String>> output = new ArrayList<>();
        for (int i = 0; i < numColumns; i++) {
            output.add(new ArrayList<>());
        }
        for (int i = 0; i < numRows; i++) {
            if (expectedResults.get(i) != null && expectedResults.get(i)) {
                for (int j = 0; j < numColumns; j++) {
                    output.get(j).add(tableContent.get(j).get(i));
                }
            }
        }
        return output;
    }

}
