package sqlancer.firebird.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.firebird.FirebirdProvider.FirebirdGlobalState;
import sqlancer.firebird.FirebirdSchema.FirebirdColumn;
import sqlancer.firebird.FirebirdSchema.FirebirdTable;
import sqlancer.firebird.FirebirdErrors;
import sqlancer.firebird.FirebirdToStringVisitor;

public class FirebirdInsertGenerator {
  private final FirebirdGlobalState globalState;
  private final ExpectedErrors errors = new ExpectedErrors();
  private final StringBuilder sb = new StringBuilder();

  public FirebirdInsertGenerator(FirebirdGlobalState globalState) {
    this.globalState = globalState;
  }

  public static SQLQueryAdapter getQuery(FirebirdGlobalState globalState) {
    return new FirebirdInsertGenerator(globalState).generate();
  }

  public SQLQueryAdapter generate() {

    sb.append("INSERT INTO ");
    FirebirdTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
    List<FirebirdColumn> columns = table.getRandomNonEmptyColumnSubset();
    sb.append(table.getName());
    sb.append(" ");

    if (Randomly.getBooleanWithSmallProbability()) {
      sb.append("DEFAULT VALUES");
    } else {
      sb.append("(");
      sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
      sb.append(")");
      sb.append(" VALUES ");

      // Firebird only supports inserting a single row using VALUES
      sb.append("(");
      for (int nrColumn = 0; nrColumn < columns.size(); nrColumn++) {
        if (nrColumn != 0) {
          sb.append(", ");
        }
        insertValue(columns.get(nrColumn));
      }
      sb.append(")");
    }

    FirebirdErrors.addInsertErrors(errors);

    return new SQLQueryAdapter(sb.toString(), errors);
  }

  protected void insertValue(FirebirdColumn column) {
    sb.append(FirebirdToStringVisitor
        .asString(new FirebirdExpressionGenerator(globalState).generateConstant(column.getType())));
  }
}
