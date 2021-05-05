package sqlancer.firebird.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
            generateTable(tableContent, FirebirdToStringVisitor.asString(select));
        }

        {
            FirebirdSelect select = new FirebirdSelect();
            select.setFromList(tableList);
            select.setFetchColumns(predicates);
            generateTable(predicateEvaluations, FirebirdToStringVisitor.asString(select));
        }

        for (int i = 0; i < predicates.size(); i++) {
            this.knownPredicates.add(new FirebirdKnownPredicate(predicates.get(i),
                    predicateEvaluationsToBooleans(predicateEvaluations.get(i))));
        }

        FirebirdKnownPredicate combinedPredicate = getCombinedPredicate();

        {
            FirebirdSelect select = new FirebirdSelect();
            select.setFromList(tableList);
            select.setFetchColumns(fetchColumns);
            select.setWhereClause(combinedPredicate.getExpression());
        }
    }

    List<Node<FirebirdExpression>> generateFetchColumns() {
        List<Node<FirebirdExpression>> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new ColumnReferenceNode<>(new FirebirdColumn("*", null, false, false)));
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

    private static List<Boolean> predicateEvaluationsToBooleans(List<String> results) {
        return results.stream().map(v -> {
            switch (v) {
            case "TRUE":
                return true;
            case "FALSE":
                return false;
            case "NULL":
                return null;
            default:
                throw new AssertionError(v);
            }
        }).collect(Collectors.toList());
    }
}
