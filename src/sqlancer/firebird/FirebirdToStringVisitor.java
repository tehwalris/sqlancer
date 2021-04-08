package sqlancer.firebird;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.Node;
import sqlancer.firebird.ast.FirebirdConstant;
import sqlancer.firebird.ast.FirebirdExpression;
import sqlancer.firebird.ast.FirebirdSelect;

public class FirebirdToStringVisitor extends NewToStringVisitor<FirebirdExpression> {

    @Override
    public void visitSpecific(Node<FirebirdExpression> expr) {
        if (expr instanceof FirebirdConstant) {
            visit((FirebirdConstant) expr);
        } else if (expr instanceof FirebirdSelect) {
        	visit((FirebirdSelect) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(FirebirdConstant constant) {
        sb.append(constant.toString());
    }
    
    private void visit(FirebirdSelect select) {
    	sb.append("SELECT ");
    	if (select.isDistinct()) {
    		sb.append("DISTINCT ");
    	}
    	visit(select.getFetchColumns());
    	sb.append(" FROM ");
    	visit(select.getFromList());
    	if (select.getWhereClause() != null) {
    		sb.append(" WHERE ");
    		visit(select.getWhereClause());
    	}
    }

    public static String asString(Node<FirebirdExpression> expr) {
        FirebirdToStringVisitor visitor = new FirebirdToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
