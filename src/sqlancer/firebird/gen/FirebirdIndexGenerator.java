package sqlancer.firebird.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.firebird.FirebirdProvider.FirebirdGlobalState;
import sqlancer.firebird.FirebirdSchema.FirebirdColumn;
import sqlancer.firebird.FirebirdSchema.FirebirdTable;

public final class FirebirdIndexGenerator {

    private FirebirdIndexGenerator() {
    }

    public static SQLQueryAdapter getQuery(FirebirdGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (Randomly.getBoolean()) {
            sb.append("UNIQUE ");
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(Randomly.fromOptions("ASC ", "DESC "));
        }
        sb.append("INDEX ");
        FirebirdTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        String indexName = globalState.getSchema().getFreeIndexName();
        sb.append(indexName);
        sb.append(" ON ");
        sb.append(table.getName());
        sb.append("(");
        List<FirebirdColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
        }
        sb.append(")");
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
