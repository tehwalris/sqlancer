package sqlancer.firebird.gen;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewOrderingTerm;
import sqlancer.common.ast.newast.NewOrderingTerm.Ordering;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
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

    private final FirebirdGlobalState globalState;

    public FirebirdExpressionGenerator(FirebirdGlobalState globalState) {
        this.r = globalState.getRandomly();
        this.maxDepth = globalState.getOptions().getMaxExpressionDepth();
        this.globalState = globalState;
    }

    public FirebirdExpressionGenerator(FirebirdGlobalState globalState, int maxDepth) {
        this.r = globalState.getRandomly();
        this.maxDepth = maxDepth;
        this.globalState = globalState;
    }

    @Override
    public Node<FirebirdExpression> generateExpression(FirebirdDataType dataType, int depth) {
        return generateExpressionInternal(dataType, depth);
    }

    private Node<FirebirdExpression> generateExpressionInternal(FirebirdDataType dataType, int depth) {
        if (Randomly.getBooleanWithRatherLowProbability() || depth > maxDepth) {
            return generateLeafNode(dataType);
        } else {
            switch (dataType) {
            case BOOLEAN:
                return generateBooleanExpression(depth);
            case INTEGER:
            case FLOAT:
                return generateArithmeticExpression(depth);
            case TIMESTAMP:
            case DATE:
                return generateConstant(dataType);
            default:
                throw new AssertionError(dataType);
            }
        }
    }

    private enum BooleanExpression {
        NOT, BINARY_LOGICAL_OPERATOR, POSTFIX_OPERATOR, BINARY_COMPARISON;
    }

    private Node<FirebirdExpression> generateBooleanExpression(int depth) {
        BooleanExpression option = Randomly.fromOptions(BooleanExpression.values());
        switch (option) {
        case NOT:
            return new NewUnaryPrefixOperatorNode<FirebirdExpression>(
                    generateExpression(FirebirdDataType.BOOLEAN, depth + 1), FirebirdUnaryPrefixOperator.NOT);
        case BINARY_LOGICAL_OPERATOR:
            return new NewBinaryOperatorNode<FirebirdExpression>(
                    generateExpression(FirebirdDataType.BOOLEAN, depth + 1),
                    generateExpression(FirebirdDataType.BOOLEAN, depth + 1), FirebirdBinaryLogicalOperator.getRandom());
        case POSTFIX_OPERATOR:
            FirebirdUnaryPostfixOperator operator = FirebirdUnaryPostfixOperator.getRandom();
            FirebirdDataType inputType = Randomly.fromOptions(operator.getInputDataTypes());
            return new NewUnaryPostfixOperatorNode<FirebirdExpression>(generateExpression(inputType, depth + 1),
                    operator);
        case BINARY_COMPARISON:
            FirebirdDataType type = getMeaningfulType();
            return new NewBinaryOperatorNode<FirebirdExpression>(generateExpression(type, depth + 1),
                    generateExpression(type, depth + 1), FirebirdBinaryComparisonOperator.getRandom());
        default:
            throw new AssertionError();
        }
    }

    private FirebirdDataType getMeaningfulType() {
        if (Randomly.getBooleanWithSmallProbability() || columns == null || columns.isEmpty()) {
            return FirebirdDataType.getRandom();
        } else {
            return Randomly.fromList(columns).getType();
        }
    }

    private enum ArithmeticExpression {
        UNARY_OPERATION, BINARY_ARITHMETIC;
    }

    private Node<FirebirdExpression> generateArithmeticExpression(int depth) {
        ArithmeticExpression option = Randomly.fromOptions(ArithmeticExpression.values());
        switch (option) {
        case UNARY_OPERATION:
            return new NewUnaryPrefixOperatorNode<FirebirdExpression>(
                    generateExpression(getRandomArithmeticType(), depth + 1),
                    FirebirdUnaryPrefixOperator.getRandomArithmeticExpression());
        case BINARY_ARITHMETIC:
            FirebirdDataType leftType = getRandomArithmeticType();
            FirebirdDataType rightType = getRandomArithmeticType();
            return new NewBinaryOperatorNode<FirebirdExpression>(generateExpression(leftType, depth + 1),
                    generateExpression(rightType, depth + 1), FirebirdBinaryArithmeticOperator.getRandom());

        default:
            throw new AssertionError();
        }
    }

    private FirebirdDataType getRandomArithmeticType() {
        return Randomly.fromOptions(FirebirdDataType.INTEGER, FirebirdDataType.FLOAT);
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
        return new NewUnaryPrefixOperatorNode<>(predicate, FirebirdUnaryPrefixOperator.NOT);
    }

    @Override
    public Node<FirebirdExpression> isNull(Node<FirebirdExpression> expr) {
        return new NewUnaryPostfixOperatorNode<>(expr, FirebirdUnaryPostfixOperator.IS_NULL);
    }

    @Override
    public List<Node<FirebirdExpression>> generateOrderBys() {
        List<Node<FirebirdExpression>> expr = super.generateOrderBys();
        List<Node<FirebirdExpression>> newExpr = new ArrayList<>(expr.size());
        for (Node<FirebirdExpression> curExpr : expr) {
            if (Randomly.getBoolean()) {
                curExpr = new NewOrderingTerm<>(curExpr, Ordering.getRandom());
            }
            newExpr.add(curExpr);
        }
        return newExpr;
    }

    public enum FirebirdUnaryPrefixOperator implements Operator {

        NOT("NOT"), UNARY_PLUS("+"), UNARY_MINUS("-");

        private String textRepr;

        FirebirdUnaryPrefixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static FirebirdUnaryPrefixOperator getRandomArithmeticExpression() {
            return Randomly.getBoolean() ? UNARY_PLUS : UNARY_MINUS;
        }

    }

    public enum FirebirdBinaryLogicalOperator implements Operator {

        AND("AND"), OR("OR");

        private String textRepr;

        FirebirdBinaryLogicalOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static FirebirdBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum FirebirdUnaryPostfixOperator implements Operator {

        IS_NULL("IS NULL") {

            @Override
            public FirebirdDataType[] getInputDataTypes() {
                return FirebirdDataType.values();
            }

        },
        IS_NOT_NULL("IS NOT NULL") {

            @Override
            public FirebirdDataType[] getInputDataTypes() {
                return FirebirdDataType.values();
            }

        },
        IS_UNKNOWN("IS UNKNOWN") {

            @Override
            public FirebirdDataType[] getInputDataTypes() {
                return new FirebirdDataType[] { FirebirdDataType.BOOLEAN };
            }

        },
        IS_NOT_UNKNOWN("IS NOT UNKNOWN") {

            @Override
            public FirebirdDataType[] getInputDataTypes() {
                return new FirebirdDataType[] { FirebirdDataType.BOOLEAN };
            }

        },
        IS_TRUE("IS TRUE") {

            @Override
            public FirebirdDataType[] getInputDataTypes() {
                return new FirebirdDataType[] { FirebirdDataType.BOOLEAN };
            }
        },
        IS_FALSE("IS FALSE") {

            @Override
            public FirebirdDataType[] getInputDataTypes() {
                return new FirebirdDataType[] { FirebirdDataType.BOOLEAN };
            }

        };

        private String textRepr;

        FirebirdUnaryPostfixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static FirebirdUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        public abstract FirebirdDataType[] getInputDataTypes();

    }

    public enum FirebirdBinaryComparisonOperator implements Operator {

        EQUALS("="), NOT_EQUALS("!="), SMALLER("<"), SMALLER_EQUALS("<="), GREATER(">"), GREATER_EQUALS(">="),
        LIKE("LIKE"), NOT_LIKE("NOT LIKE");

        private String textRepr;

        FirebirdBinaryComparisonOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static FirebirdBinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum FirebirdBinaryArithmeticOperator implements Operator {

        ADD("+"), SUB("-"), MULT("*"), DIV("/");

        private String textRepr;

        FirebirdBinaryArithmeticOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static FirebirdBinaryArithmeticOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

}
