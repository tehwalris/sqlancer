package sqlancer.firebird.gen;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.firebird.FirebirdProvider.FirebirdGlobalState;
import sqlancer.firebird.FirebirdSchema.FirebirdColumn;
import sqlancer.firebird.FirebirdSchema.FirebirdDataType;
import sqlancer.firebird.ast.FirebirdConstant;
import sqlancer.firebird.ast.FirebirdExpression;

public final class FirebirdExpressionGenerator
        extends TypedExpressionGenerator<Node<FirebirdExpression>, FirebirdColumn, FirebirdDataType> {

    private final int maxDepth;

    private final Randomly r;

    private List<FirebirdColumn> columns;

    private final FirebirdGlobalState globalState;

    public FirebirdExpressionGenerator(FirebirdGlobalState globalState) {
        this.r = globalState.getRandomly();
        this.maxDepth = globalState.getOptions().getMaxExpressionDepth();
        this.globalState = globalState;
    }

    @Override
    public Node<FirebirdExpression> generateExpression(FirebirdDataType dataType, int depth) {
        return generateExpressionInternal(dataType, depth);
    }

    private Node<FirebirdExpression> generateExpressionInternal(FirebirdDataType dataType, int depth) {
        if (Randomly.getBooleanWithRatherLowProbability() || depth > maxDepth) {
            if (Randomly.getBooleanWithRatherLowProbability()) {
                return generateConstant(dataType);
            } else {
                if (filterColumns(dataType).isEmpty()) {
                    return generateConstant(dataType);
                } else {
                    return generateColumn(dataType);
                }
            }
        } else {
            throw new RuntimeException("not implemented");
        }
    }

    @Override
    protected boolean canGenerateColumnOfType(FirebirdDataType type) {
        return !filterColumns(type).isEmpty();
    }

    @Override
    protected Node<FirebirdExpression> generateColumn(FirebirdDataType type) {
        FirebirdColumn column = Randomly.fromList(filterColumns(type));
        return new ColumnReferenceNode<FirebirdExpression, FirebirdColumn>(column);
    }

    @Override
    public Node<FirebirdExpression> generateConstant(FirebirdDataType type) {
        if (Randomly.getBooleanWithSmallProbability()) {
            return FirebirdConstant.createNullConstant();
        }
        switch (type) {
        case INTEGER:
            return FirebirdConstant.createIntConstant(r.getInteger());
        case FLOAT:
            return FirebirdConstant.createFloatConstant(r.getDouble());
        case BOOLEAN:
            return FirebirdConstant.createBooleanConstant(Randomly.getBoolean());
        case TIMESTAMP:
            return FirebirdConstant.createTimestampConstant(r.getInteger());
        case DATE:
            return FirebirdConstant.createDateConstant(r.getInteger());
        default:
            throw new AssertionError(type);
        }
    }

    @Override
    protected FirebirdDataType getRandomType() {
        return FirebirdDataType.getRandom();
    }

    private List<FirebirdColumn> filterColumns(FirebirdDataType type) {
        if (columns == null) {
            return Collections.emptyList();
        } else {
            return columns.stream().filter(c -> c.getType() == type).collect(Collectors.toList());
        }
    }

    @Override
    public Node<FirebirdExpression> generatePredicate() {
        return generateExpression(FirebirdDataType.BOOLEAN);
    }

    @Override
    public Node<FirebirdExpression> negatePredicate(Node<FirebirdExpression> predicate) {
        throw new RuntimeException("not implemented");
        // return new FirebirdPrefixOperation(predicate,
        // FirebirdPrefixOperation.PrefixOperator.NOT);
    }

    @Override
    public Node<FirebirdExpression> isNull(Node<FirebirdExpression> expr) {
        throw new RuntimeException("not implemented");
        // return new FirebirdPostfixOperation(expr, PostfixOperator.IS_NULL);
    }
}
