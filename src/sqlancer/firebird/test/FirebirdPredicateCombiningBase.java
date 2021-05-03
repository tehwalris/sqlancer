package sqlancer.firebird.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.PredicateCombiningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.firebird.FirebirdErrors;
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
    FirebirdSelect select;
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
        select = new FirebirdSelect();
        select.setFetchColumns(fetchColumns);
        List<FirebirdTable> tables = targetTables.getTables();
        List<TableReferenceNode<FirebirdExpression, FirebirdTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<FirebirdExpression, FirebirdTable>(t)).collect(Collectors.toList());
        // TODO: Add joins to select statement
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        select.setWhereClause(null);
        String tableQueryString = FirebirdToStringVisitor.asString(select);
        select.setFetchColumns(predicates);
        String predicateQueryString = FirebirdToStringVisitor.asString(select);
        select.setFetchColumns(fetchColumns);
        generateTable(tableContent, tableQueryString);
        generateTable(predicateEvaluations, predicateQueryString);
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
    protected Node<FirebirdExpression> getCombinedPredicate() {
        return internalPredicateCombination(0);
    }

    private Node<FirebirdExpression> internalPredicateCombination(int depth) {
        if (Randomly.getBooleanWithRatherLowProbability() || depth > maxPredicateDepth) {
            return Randomly.fromList(predicates);
        } else {
            Node<FirebirdExpression> leftPredicate = internalPredicateCombination(depth + 1);
            Node<FirebirdExpression> rightPredicate = internalPredicateCombination(depth + 1);
            if (Randomly.getBoolean()) {
                leftPredicate = gen.negatePredicate(leftPredicate);
            }
            if (Randomly.getBoolean()) {
                rightPredicate = gen.negatePredicate(rightPredicate);
            }
            return new NewBinaryOperatorNode<FirebirdExpression>(leftPredicate, rightPredicate,
                    FirebirdBinaryLogicalOperator.getRandom());
        }
    }

}
