package sqlancer.firebird.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.firebird.FirebirdProvider.FirebirdGlobalState;
import sqlancer.firebird.FirebirdSchema.FirebirdColumn;
import sqlancer.firebird.FirebirdSchema.FirebirdTable;
import sqlancer.firebird.gen.FirebirdExpressionGenerator;

public class FirebirdJoin implements Node<FirebirdExpression> {
	
	private final TableReferenceNode<FirebirdExpression, FirebirdTable> leftTable;
	private final TableReferenceNode<FirebirdExpression, FirebirdTable> rightTable;
	private final JoinType joinType;
	private final Node<FirebirdExpression> onCondition;
	private OuterType outerType;
	
	public enum JoinType {
		INNER, NATURAL, LEFT, RIGHT;
		
		public static JoinType getRandom() {
			return Randomly.fromOptions(values());
		}
	}
	
	public enum OuterType {
		FULL, LEFT, RIGHT;
		
		public static OuterType getRandom() {
			return Randomly.fromOptions(values());
		}
	}
	
	public FirebirdJoin(TableReferenceNode<FirebirdExpression, FirebirdTable> leftTable,
			TableReferenceNode<FirebirdExpression, FirebirdTable> rightTable, JoinType joinType,
			Node<FirebirdExpression> whereCondition) {
		this.leftTable = leftTable;
		this.rightTable = rightTable;
		this.joinType = joinType;
		this.onCondition = whereCondition;
	}
	
	public TableReferenceNode<FirebirdExpression, FirebirdTable> getLeftTable() {
        return leftTable;
    }

    public TableReferenceNode<FirebirdExpression, FirebirdTable> getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Node<FirebirdExpression> getOnCondition() {
        return onCondition;
    }

    private void setOuterType(OuterType outerType) {
        this.outerType = outerType;
    }

    public OuterType getOuterType() {
        return outerType;
    }
    
    public static List<Node<FirebirdExpression>> getJoins(
    		List<TableReferenceNode<FirebirdExpression, FirebirdTable>> tableList, FirebirdGlobalState globalState) {
    	List<Node<FirebirdExpression>> joinExpressions = new ArrayList<>();
    	while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
    		TableReferenceNode<FirebirdExpression, FirebirdTable> leftTable = tableList.remove(0);
    		TableReferenceNode<FirebirdExpression, FirebirdTable> rightTable = tableList.remove(0);
    		List<FirebirdColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
    		columns.addAll(rightTable.getTable().getColumns());
    		FirebirdExpressionGenerator joinGen = new FirebirdExpressionGenerator(globalState).setColumns(columns);
    		switch (FirebirdJoin.JoinType.getRandom()) {
    		case INNER:
    			joinExpressions.add(FirebirdJoin.createInnerJoin(leftTable, rightTable, joinGen.generatePredicate()));
    			break;
    		case NATURAL:
    			joinExpressions.add(FirebirdJoin.createNaturalJoin(leftTable, rightTable, OuterType.getRandom()));
    			break;
    		case LEFT:
    			joinExpressions.add(FirebirdJoin.createLeftOuterJoin(leftTable, rightTable, joinGen.generatePredicate()));
    			break;
    		case RIGHT:
    			joinExpressions.add(FirebirdJoin.createRightOuterJoin(leftTable, rightTable, joinGen.generatePredicate()));
    			break;
    		default:
    			throw new AssertionError();
    		}
    	}
    	return joinExpressions;
    }
    
    public static FirebirdJoin createRightOuterJoin(TableReferenceNode<FirebirdExpression, FirebirdTable> left,
    		TableReferenceNode<FirebirdExpression, FirebirdTable> right, Node<FirebirdExpression> predicate) {
    	return new FirebirdJoin(left, right, JoinType.RIGHT, predicate);
    }
    
    public static FirebirdJoin createLeftOuterJoin(TableReferenceNode<FirebirdExpression, FirebirdTable> left,
    		TableReferenceNode<FirebirdExpression, FirebirdTable> right, Node<FirebirdExpression> predicate) {
    	return new FirebirdJoin(left, right, JoinType.LEFT, predicate);
    }
    
    public static FirebirdJoin createInnerJoin(TableReferenceNode<FirebirdExpression, FirebirdTable> left,
    		TableReferenceNode<FirebirdExpression, FirebirdTable> right, Node<FirebirdExpression> predicate) {
    	return new FirebirdJoin(left, right, JoinType.INNER, predicate);
    }
    
    public static FirebirdJoin createNaturalJoin(TableReferenceNode<FirebirdExpression, FirebirdTable> left,
    		TableReferenceNode<FirebirdExpression, FirebirdTable> right, OuterType naturalJoinType) {
    	FirebirdJoin join = new FirebirdJoin(left, right, JoinType.NATURAL, null);
    	join.setOuterType(naturalJoinType);
    	return join;
    }
    
}
