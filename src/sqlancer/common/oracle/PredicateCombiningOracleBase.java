package sqlancer.common.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
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
    protected List<List<String>> predicateEvaluations;
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

    protected void generateTable(List<List<String>> table, String queryString) throws SQLException {
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors);
        SQLancerResultSet result = null;
        try {
            result = q.executeAndGet(state);
            if (result == null) {
                throw new IgnoreMeException();
            }
            int numColumns = result.getColumnCount();
            while (result.next()) {
                for (int i = 1; i <= numColumns; i++) {
                    table.get(i - 1).add(result.getString(i));
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
                result.close();
            }
        }
    }

    protected abstract ExpressionGenerator<E> getGen();

}
