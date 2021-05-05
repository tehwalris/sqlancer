package sqlancer.firebird;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.firebird.ast.FirebirdExpression;
import sqlancer.firebird.gen.FirebirdExpressionGenerator.FirebirdBinaryLogicalOperator;
import sqlancer.firebird.gen.FirebirdExpressionGenerator.FirebirdUnaryPrefixOperator;

// FirebirdKnownPredicate represents a SQL expression for a predicate combined with expected truth values
// for that predicate over a fixed list of rows.
public class FirebirdKnownPredicate {
  protected Node<FirebirdExpression> expression;
  protected List<Boolean> expectedResults; // may contain null values to represent SQL null

  public FirebirdKnownPredicate(Node<FirebirdExpression> expression, List<Boolean> expectedResults) {
    this.expression = expression;
    this.expectedResults = expectedResults;
  }

  public Node<FirebirdExpression> getExpression() {
    return expression;
  }

  public List<Boolean> getExpectedResults() {
    return expectedResults;
  }

  public FirebirdKnownPredicate negate() {
    Node<FirebirdExpression> negatedExpression = new NewUnaryPrefixOperatorNode<>(expression,
        FirebirdUnaryPrefixOperator.NOT);
    List<Boolean> negatedExpectedResults = expectedResults.stream().map(v -> v == null ? null : !v)
        .collect(Collectors.toList());
    return new FirebirdKnownPredicate(negatedExpression, negatedExpectedResults);
  }

  public static FirebirdKnownPredicate applyBinaryOperator(FirebirdKnownPredicate left, FirebirdKnownPredicate right,
      FirebirdBinaryLogicalOperator operator) {

    Node<FirebirdExpression> expression = new NewBinaryOperatorNode<>(left.expression, right.expression, operator);

    List<Boolean> expectedResults = new ArrayList<>();
    assert left.expectedResults.size() == right.expectedResults.size();
    for (int i = 0; i < left.expectedResults.size(); i++) {
      Boolean l = left.expectedResults.get(i);
      Boolean r = right.expectedResults.get(i);

      switch (operator) {
      case AND:
        expectedResults.add(ternaryAnd(l, r));
        break;
      case OR:
        expectedResults.add(ternaryOr(l, r));
        break;
      default:
        throw new AssertionError(operator);
      }
    }

    return new FirebirdKnownPredicate(expression, expectedResults);
  }

  private static Boolean ternaryAnd(Boolean l, Boolean r) {
    if (l == null && r == null) {
      return null;
    } else if (l != null && r != null) {
      return l && r;
    } else if ((l != null && l == true) || (r != null && r == true)) {
      return null;
    } else if ((l != null && l == false) || (r != null && r == false)) {
      return false;
    } else {
      throw new AssertionError("unreachable");
    }
  }

  private static Boolean ternaryOr(Boolean l, Boolean r) {
    if (l == null && r == null) {
      return null;
    } else if (l != null && r != null) {
      return l || r;
    } else if ((l != null && l == true) || (r != null && r == true)) {
      return true;
    } else if ((l != null && l == false) || (r != null && r == false)) {
      return null;
    } else {
      throw new AssertionError("unreachable");
    }
  }
}
