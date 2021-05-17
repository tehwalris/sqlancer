package sqlancer.firebird.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.firebird.FirebirdProvider.FirebirdGlobalState;
import sqlancer.firebird.FirebirdSchema.FirebirdDataType;
import sqlancer.firebird.FirebirdSchema.FirebirdTable;
import sqlancer.firebird.FirebirdSchema.FirebirdTables;
import sqlancer.firebird.ast.FirebirdConstant;
import sqlancer.firebird.ast.FirebirdExpression;
import sqlancer.firebird.ast.FirebirdJoin;
import sqlancer.firebird.ast.FirebirdSelect;

public final class FirebirdRandomQuerySynthesizer {

    private FirebirdRandomQuerySynthesizer() {
    }

    public static FirebirdSelect generateSelect(FirebirdGlobalState globalState, int nrColumns) {
        FirebirdSelect select = new FirebirdSelect();
        FirebirdTables targetTables = globalState.getSchema().getRandomTableNonEmptyTables();
        FirebirdExpressionGenerator gen = new FirebirdExpressionGenerator(globalState)
                .setColumns(targetTables.getColumns());
        List<Node<FirebirdExpression>> columns = new ArrayList<>();
        for (int i = 0; i < nrColumns; i++) {
            FirebirdDataType columnType = FirebirdDataType.getRandom();
            columns.add(gen.generateExpression(columnType));
        }
        select.setFetchColumns(columns);
        List<FirebirdTable> tables = targetTables.getTables();
        List<TableReferenceNode<FirebirdExpression, FirebirdTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<FirebirdExpression, FirebirdTable>(t)).collect(Collectors.toList());
        List<Node<FirebirdExpression>> joins = FirebirdJoin.getJoins(tableList, globalState);
        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        select.setDistinct(Randomly.getBoolean());
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generatePredicate());
        }
        if (Randomly.getBoolean()) {
            select.setLimitClause(
                    FirebirdConstant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }
        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }

        // TODO: add Group By, Having clause

        return select;
    }

}
