package sqlancer.firebird.test;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.firebirdsql.jdbc.FBConnection;

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
import sqlancer.firebird.ast.FirebirdJoin;
import sqlancer.firebird.ast.FirebirdSelect;
import sqlancer.firebird.gen.FirebirdExpressionGenerator;
import sqlancer.firebird.gen.FirebirdExpressionGenerator.FirebirdBinaryLogicalOperator;

public class FirebirdPredicateCombiningBase
        extends PredicateCombiningOracleBase<Node<FirebirdExpression>, FirebirdGlobalState> implements TestOracle {

    FirebirdSchema s;
    FirebirdTables targetTables;
    FirebirdExpressionGenerator gen;
    Map<List<Boolean>, List<FirebirdKnownPredicate>> knownPredicates;
    List<List<Boolean>> evaluationPatterns;
    FirebirdSelect select;
    int maxPredicateDepth;

    public FirebirdPredicateCombiningBase(FirebirdGlobalState state) {
        super(state);
        FirebirdErrors.addExpressionErrors(errors);
        FirebirdErrors.addUnstableErrors(errors);
        this.maxPredicateDepth = state.getDmbsSpecificOptions().maxPredicateCombinations;
    }

    @Override
    public void check() throws SQLException {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            freePreparedStatements();
        }

        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new FirebirdExpressionGenerator(state, 1).setColumns(targetTables.getColumns());
        gen.setLogicInPredicates(false);
        List<Node<FirebirdExpression>> fetchColumns = generateFetchColumns();
        initializeInternalTables(state.getDmbsSpecificOptions().numOraclePredicates, fetchColumns.size());
        List<TableReferenceNode<FirebirdExpression, FirebirdTable>> tableList = targetTables.getTables().stream()
                .map(t -> new TableReferenceNode<FirebirdExpression, FirebirdTable>(t)).collect(Collectors.toList());
        List<Node<FirebirdExpression>> joins = FirebirdJoin.getJoins(tableList, state);
        select = new FirebirdSelect();
        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        List<Node<FirebirdExpression>> allColumns = Stream.concat(fetchColumns.stream(), predicates.stream())
                .collect(Collectors.toList());
        select.setFetchColumns(allColumns);
        generateTables(FirebirdToStringVisitor.asString(select), fetchColumns.size(),
                state.getDmbsSpecificOptions().numOraclePredicates);

        knownPredicates = new HashMap<>();
        evaluationPatterns = new ArrayList<>();
        for (int i = 0; i < predicates.size(); i++) {
            if (!knownPredicates.containsKey(predicateEvaluations.get(i))) {
            	knownPredicates.put(predicateEvaluations.get(i), new ArrayList<>());
            }
            FirebirdKnownPredicate predicate = new FirebirdKnownPredicate(predicates.get(i), predicateEvaluations.get(i));
            knownPredicates.get(predicateEvaluations.get(i)).add(predicate);
        }
        evaluationPatterns.addAll(knownPredicates.keySet());

        select.setFetchColumns(fetchColumns);
    }
    
    List<Node<FirebirdExpression>> generateFetchColumns() {
        List<Node<FirebirdExpression>> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns = targetTables.getColumns().stream()
                    .map(c -> new ColumnReferenceNode<FirebirdExpression, FirebirdColumn>(c))
                    .collect(Collectors.toList());
        } else {
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                    .map(c -> new ColumnReferenceNode<FirebirdExpression, FirebirdColumn>(c))
                    .collect(Collectors.toList());
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<Node<FirebirdExpression>> getGen() {
        return gen;
    }

    protected FirebirdKnownPredicate getCombinedPredicate() {
        return internalPredicateCombination(0);
    }

    private FirebirdKnownPredicate internalPredicateCombination(int depth) {
        if (Randomly.getBooleanWithRatherLowProbability() || depth >= maxPredicateDepth) {
        	List<Boolean> pattern = Randomly.fromList(evaluationPatterns);
        	return Randomly.fromList(knownPredicates.get(pattern));
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

    /**
     * HACK Firebird does not free prepared statements until the connection is closed. We could not find a method or
     * configuration option to free prepared statements "correctly", so this frees them all occasionally.
     */
    private void freePreparedStatements() throws SQLException {
        try {
            FBConnection connection = ((FBConnection) state.getConnection().getConnection());
            Method freeStatements = connection.getClass().getDeclaredMethod("freeStatements");
            freeStatements.setAccessible(true);
            freeStatements.invoke(connection);
        } catch (Exception e) {
            System.out.println(e);
            throw new RuntimeException("failed to free prepared statements");
        }
    }

}
