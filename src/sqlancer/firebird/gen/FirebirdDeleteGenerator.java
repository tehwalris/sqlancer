package sqlancer.firebird.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.firebird.FirebirdErrors;
import sqlancer.firebird.FirebirdProvider.FirebirdGlobalState;
import sqlancer.firebird.FirebirdSchema.FirebirdDataType;
import sqlancer.firebird.FirebirdSchema.FirebirdTable;
import sqlancer.firebird.FirebirdToStringVisitor;

public final class FirebirdDeleteGenerator {

    private FirebirdDeleteGenerator() {
    }

    public static SQLQueryAdapter getQuery(FirebirdGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ");
        FirebirdTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(FirebirdToStringVisitor.asString(new FirebirdExpressionGenerator(globalState)
                    .setColumns(table.getColumns()).generateExpression(FirebirdDataType.BOOLEAN)));
        }
        FirebirdErrors.addInsertErrors(errors);
        FirebirdErrors.addUnstableErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }
}
