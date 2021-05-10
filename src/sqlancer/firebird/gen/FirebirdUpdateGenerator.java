package sqlancer.firebird.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.firebird.FirebirdErrors;
import sqlancer.firebird.FirebirdProvider.FirebirdGlobalState;
import sqlancer.firebird.FirebirdSchema.FirebirdColumn;
import sqlancer.firebird.FirebirdSchema.FirebirdDataType;
import sqlancer.firebird.FirebirdSchema.FirebirdTable;
import sqlancer.firebird.FirebirdToStringVisitor;
import sqlancer.firebird.ast.FirebirdExpression;

public final class FirebirdUpdateGenerator {

    private FirebirdUpdateGenerator() {
    }

    public static SQLQueryAdapter getQuery(FirebirdGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        FirebirdTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        sb.append(" SET ");
        FirebirdExpressionGenerator gen = new FirebirdExpressionGenerator(globalState).setColumns(table.getColumns());
        List<FirebirdColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append("=");
            FirebirdDataType columnType = columns.get(i).getType();
            Node<FirebirdExpression> expr = Randomly.getBooleanWithSmallProbability()
                    ? gen.generateExpression(columnType) : gen.generateConstant(columnType);
            sb.append(FirebirdToStringVisitor.asString(expr));
        }
        FirebirdErrors.addInsertErrors(errors);
        FirebirdErrors.addExpressionErrors(errors);
        FirebirdErrors.addUnstableErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
