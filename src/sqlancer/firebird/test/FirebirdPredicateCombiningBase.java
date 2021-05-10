package sqlancer.firebird.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.PredicateCombiningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.firebird.FirebirdErrors;
import sqlancer.firebird.FirebirdKnownPredicate;
import sqlancer.firebird.FirebirdProvider.FirebirdGlobalState;
import sqlancer.firebird.FirebirdSchema;
import sqlancer.firebird.FirebirdSchema.FirebirdColumn;
import sqlancer.firebird.FirebirdSchema.FirebirdTable;
import sqlancer.firebird.FirebirdSchema.FirebirdTables;
import sqlancer.firebird.FirebirdToStringVisitor;
import sqlancer.firebird.ast.FirebirdExpression;
import sqlancer.firebird.ast.FirebirdSelect;
import sqlancer.firebird.gen.FirebirdExpressionGenerator;
import sqlancer.firebird.gen.FirebirdExpressionGenerator.FirebirdBinaryLogicalOperator;

public class FirebirdPredicateCombiningBase
        extends PredicateCombiningOracleBase<Node<FirebirdExpression>, FirebirdGlobalState> implements TestOracle {

    FirebirdSchema s;
    FirebirdTables targetTables;
    FirebirdExpressionGenerator gen;
    List<FirebirdKnownPredicate> knownPredicates;
    int numColumns;
    int maxPredicateDepth;

    public FirebirdPredicateCombiningBase(FirebirdGlobalState state) {
        super(state);
        FirebirdErrors.addExpressionErrors(errors);
        this.maxPredicateDepth = state.getDmbsSpecificOptions().maxPredicateCombinations;
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new FirebirdExpressionGenerator(state, 1).setColumns(targetTables.getColumns());
        List<Node<FirebirdExpression>> fetchColumns = generateFetchColumns();
        initializeInternalTables(state.getDmbsSpecificOptions().numOraclePredicates, numColumns);
        List<Node<FirebirdExpression>> tableList = targetTables.getTables().stream()
                .map(t -> new TableReferenceNode<FirebirdExpression, FirebirdTable>(t)).collect(Collectors.toList());

        {
            FirebirdSelect select = new FirebirdSelect();
            // TODO: Add joins to select statement
            select.setFromList(tableList);
            select.setFetchColumns(fetchColumns);
            List<Node<FirebirdExpression>> allColumns = Stream.concat(fetchColumns.stream(), predicates.stream()).collect(Collectors.toList());
            select.setFetchColumns(allColumns);
            generateTables(FirebirdToStringVisitor.asString(select), numColumns, state.getDmbsSpecificOptions().numOraclePredicates);
        }
        
        knownPredicates = new ArrayList<>();
        for (int i = 0; i < predicates.size(); i++) {
            knownPredicates.add(new FirebirdKnownPredicate(predicates.get(i), predicateEvaluations.get(i)));
        }

        FirebirdKnownPredicate combinedPredicate = getCombinedPredicate();

        String combinedQueryString;
        {
            FirebirdSelect select = new FirebirdSelect();
            select.setFromList(tableList);
            select.setFetchColumns(fetchColumns);
            select.setWhereClause(combinedPredicate.getExpression());

            combinedQueryString = FirebirdToStringVisitor.asString(select);
        }
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(combinedQueryString, errors, state);

        List<String> expectedResultSet = getFirstColumnFilteredByExpectedResults(tableContent, combinedPredicate.getExpectedResults());
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, expectedResultSet, combinedQueryString, new ArrayList<>(), state);
    }

    List<Node<FirebirdExpression>> generateFetchColumns() {
        List<Node<FirebirdExpression>> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns = targetTables.getColumns().stream().map(c -> new ColumnReferenceNode<FirebirdExpression, FirebirdColumn>(c)).collect(Collectors.toList());
            numColumns = targetTables.getColumns().size();
        } else {
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                    .map(c -> new ColumnReferenceNode<FirebirdExpression, FirebirdColumn>(c))
                    .collect(Collectors.toList());
            numColumns = columns.size();
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<Node<FirebirdExpression>> getGen() {
        return gen;
    }

    // TODO: maybe refactor s.t. this can be done in PredicateCombiningOracleBase
    protected FirebirdKnownPredicate getCombinedPredicate() {
        return internalPredicateCombination(0);
    }

    private FirebirdKnownPredicate internalPredicateCombination(int depth) {
        if (Randomly.getBooleanWithRatherLowProbability() || depth > maxPredicateDepth) {
            return Randomly.fromList(knownPredicates);
        } else {
            FirebirdKnownPredicate leftPredicate = internalPredicateCombination(depth + 1);
            FirebirdKnownPredicate rightPredicate = internalPredicateCombination(depth + 1);
            if (Randomly.getBoolean()) {
                leftPredicate = leftPredicate.negate();
            }
            if (Randomly.getBoolean()) {
                rightPredicate = rightPredicate.negate();
            }
            return FirebirdKnownPredicate.applyBinaryOperator(leftPredicate, rightPredicate,
                    FirebirdBinaryLogicalOperator.getRandom());
        }
    }
}
