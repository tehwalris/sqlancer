package sqlancer.firebird.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.firebird.FirebirdErrors;
import sqlancer.firebird.FirebirdProvider.FirebirdGlobalState;
import sqlancer.firebird.FirebirdToStringVisitor;

public final class FirebirdViewGenerator {

    private FirebirdViewGenerator() {
    }

    public static SQLQueryAdapter getQuery(FirebirdGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE VIEW ");
        sb.append(globalState.getSchema().getFreeViewName());
        sb.append("(");
        int nrColumns = Randomly.smallNumber() + 1;
        for (int i = 0; i < nrColumns; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append("c");
            sb.append(i);
        }
        sb.append(") AS ");
        sb.append(FirebirdToStringVisitor
                .asString(FirebirdRandomQuerySynthesizer.generateSelect(globalState, nrColumns)));

        FirebirdErrors.addInsertErrors(errors);
        FirebirdErrors.addUnstableErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }
}
