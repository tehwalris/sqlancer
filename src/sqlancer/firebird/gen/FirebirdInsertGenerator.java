package sqlancer.firebird.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.firebird.FirebirdToStringVisitor;
import sqlancer.firebird.FirebirdProvider.FirebirdGlobalState;
import sqlancer.firebird.FirebirdSchema.FirebirdColumn;
import sqlancer.firebird.FirebirdSchema.FirebirdTable;

public class FirebirdInsertGenerator extends AbstractInsertGenerator<FirebirdColumn> {
  private final FirebirdGlobalState globalState;
  private final ExpectedErrors errors = new ExpectedErrors();

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
    sb.append("(");
    sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
    sb.append(")");
    sb.append(" VALUES ");
    if (Randomly.getBooleanWithSmallProbability()) {
      sb.append("DEFAULT VALUES");
    } else {
      insertColumns(columns);
    }

    // TODO(voinovp)
    // FirebirdErrors.addInsertErrors(errors);

    return new SQLQueryAdapter(sb.toString(), errors);
  }

  @Override
  protected void insertValue(FirebirdColumn column) {
    sb.append(FirebirdToStringVisitor
        .asString(new FirebirdExpressionGenerator(globalState).generateConstant(column.getType())));
  }
}
