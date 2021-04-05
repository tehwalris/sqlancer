package sqlancer.firebird.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.firebird.FirebirdErrors;
import sqlancer.firebird.FirebirdProvider.FirebirdGlobalState;
import sqlancer.firebird.FirebirdSchema.FirebirdColumn;
import sqlancer.firebird.FirebirdSchema.FirebirdDataType;

public class FirebirdTableGenerator {

    public SQLQueryAdapter getQuery(FirebirdGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        String tableName = globalState.getSchema().getFreeTableName();
        sb.append("CREATE TABLE ");
        sb.append(tableName);
        sb.append("(");
        List<FirebirdColumn> columns = getNewColumns();

        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append(" ");
            sb.append(columns.get(i).getType());

            if (globalState.getDmbsSpecificOptions().testIndexes && Randomly.getBooleanWithRatherLowProbability()) {
                errors.add("Attempt to define a second PRIMARY KEY for the same table");
                if (Randomly.getBoolean()) {
                    sb.append(" UNIQUE");
                } else {
                    sb.append(" PRIMARY KEY");
                }
            }
            if (globalState.getDmbsSpecificOptions().testNotNullConstraints
                    && Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" NOT NULL");
            }

            // TODO: add collate, check, default

        }
        if (globalState.getDmbsSpecificOptions().testIndexes && Randomly.getBoolean()) {
            errors.add("Attempt to define a second PRIMARY KEY for the same table");
            List<FirebirdColumn> primaryKeyColumns = Randomly.nonEmptySubset(columns);
            sb.append(", PRIMARY KEY(");
            sb.append(primaryKeyColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
            sb.append(")");
        }

        sb.append(")");

        FirebirdErrors.addTableErrors(errors);

        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private static List<FirebirdColumn> getNewColumns() {
        List<FirebirdColumn> columns = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String columnName = String.format("c%d", i);
            FirebirdDataType columnType = FirebirdDataType.getRandom();
            columns.add(new FirebirdColumn(columnName, columnType, false, false));
        }
        return columns;
    }
}
