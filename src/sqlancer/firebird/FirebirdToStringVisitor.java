package sqlancer.firebird;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.Node;
import sqlancer.firebird.ast.FirebirdConstant;
import sqlancer.firebird.ast.FirebirdExpression;

public class FirebirdToStringVisitor extends NewToStringVisitor<FirebirdExpression> {

  @Override
  public void visitSpecific(Node<FirebirdExpression> expr) {
    if (expr instanceof FirebirdConstant) {
      visit((FirebirdConstant) expr);
    } else {
      throw new AssertionError(expr.getClass());
    }
  }

  private void visit(FirebirdConstant constant) {
    sb.append(constant.toString());
  }

  public static String asString(Node<FirebirdExpression> expr) {
    FirebirdToStringVisitor visitor = new FirebirdToStringVisitor();
    visitor.visit(expr);
    return visitor.get();
  }

}
