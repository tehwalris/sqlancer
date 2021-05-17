package sqlancer.firebird;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.Node;
import sqlancer.firebird.ast.FirebirdConstant;
import sqlancer.firebird.ast.FirebirdExpression;
import sqlancer.firebird.ast.FirebirdJoin;
import sqlancer.firebird.ast.FirebirdSelect;

public class FirebirdToStringVisitor extends NewToStringVisitor<FirebirdExpression> {

    @Override
    public void visitSpecific(Node<FirebirdExpression> expr) {
        if (expr instanceof FirebirdConstant) {
            visit((FirebirdConstant) expr);
        } else if (expr instanceof FirebirdSelect) {
            visit((FirebirdSelect) expr);
        } else if (expr instanceof FirebirdJoin) {
        	visit((FirebirdJoin) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }
    
    private void visit(FirebirdJoin join) {
    	visit(join.getLeftTable());
    	sb.append(" ");
    	sb.append(join.getJoinType());
    	sb.append(" ");
    	if (join.getOuterType() != null) {
    		sb.append(join.getOuterType());
    	}
    	sb.append(" JOIN ");
    	visit(join.getRightTable());
    	if (join.getOnCondition() != null) {
    		sb.append(" ON ");
    		visit(join.getOnCondition());
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
        if (!select.getFromList().isEmpty() && !select.getJoinList().isEmpty()) {
        	sb.append(", ");
        }
        if (!select.getJoinList().isEmpty()) {
        	visit(select.getJoinList());
        }
        if (select.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(select.getWhereClause());
        }
        if (!select.getOrderByExpressions().isEmpty()) {
        	sb.append(" ORDER BY ");
        	visit(select.getOrderByExpressions());
        }
        if (select.getLimitClause() != null) {
        	sb.append(" ROWS ");
        	visit(select.getLimitClause());
        }
    }

    public static String asString(Node<FirebirdExpression> expr) {
        FirebirdToStringVisitor visitor = new FirebirdToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
