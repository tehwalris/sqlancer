package sqlancer.firebird;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.firebirdsql.management.FBManager;

import sqlancer.AbstractAction;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.firebird.FirebirdProvider.FirebirdGlobalState;
import sqlancer.firebird.gen.FirebirdDeleteGenerator;
import sqlancer.firebird.gen.FirebirdIndexGenerator;
import sqlancer.firebird.gen.FirebirdInsertGenerator;
import sqlancer.firebird.gen.FirebirdUpdateGenerator;
import sqlancer.firebird.gen.FirebirdViewGenerator;

public class FirebirdProvider extends SQLProviderAdapter<FirebirdGlobalState, FirebirdOptions> {

    public FirebirdProvider() {
        super(FirebirdGlobalState.class, FirebirdOptions.class);
    }

    public enum Action implements AbstractAction<FirebirdGlobalState> {
        INSERT(FirebirdInsertGenerator::getQuery), //
        CREATE_INDEX(FirebirdIndexGenerator::getQuery), //
        DELETE(FirebirdDeleteGenerator::getQuery), //
        UPDATE(FirebirdUpdateGenerator::getQuery), //
        CREATE_VIEW(FirebirdViewGenerator::getQuery);

        private final SQLQueryProvider<FirebirdGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<FirebirdGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(FirebirdGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapActions(FirebirdGlobalState globalState, Action action) {
        Randomly rand = globalState.getRandomly();
        switch (action) {
        case INSERT:
            return rand.getInteger(0, globalState.getOptions().getMaxNumberInserts() + 1);
        case CREATE_INDEX:
            return globalState.getDmbsSpecificOptions().testIndexes
                    ? rand.getInteger(0, globalState.getDmbsSpecificOptions().maxNumIndexes + 1)
                    : 0;
        case DELETE:
            return rand.getInteger(0, globalState.getDmbsSpecificOptions().maxNumDeletes + 1);
        case UPDATE:
            return rand.getInteger(0, globalState.getDmbsSpecificOptions().maxNumUpdates + 1);
        case CREATE_VIEW:
            return rand.getInteger(0, globalState.getDmbsSpecificOptions().maxNumViews + 1);
        default:
            throw new AssertionError(action);
        }
    }

    public static class FirebirdGlobalState extends SQLGlobalState<FirebirdOptions, FirebirdSchema> {

        @Override
        protected FirebirdSchema readSchema() throws SQLException {
            return FirebirdSchema.fromConnection(getConnection(), getDatabaseName());
        }
    }

    @Override
    public void generateDatabase(FirebirdGlobalState globalState) throws Exception {
        // for (int i = 0; i < Randomly.fromOptions(1,
        // globalState.getDmbsSpecificOptions().maxNumTables + 1); i++) {
        // boolean success;
        // do {
        // SQLQueryAdapter qt = new FirebirdTableGenerator().getQuery(globalState);
        // success = globalState.executeStatement(qt);
        // } while (!success);
        // }
        // StatementExecutor<FirebirdGlobalState, Action> se = new
        // StatementExecutor<>(globalState, Action.values(),
        // FirebirdProvider::mapActions, (q) -> {
        // if (globalState.getSchema().getDatabaseTables().isEmpty()) {
        // throw new IgnoreMeException();
        // }
        // });
        // se.executeStatements();

        ExpectedErrors tableExpectedErrors = new ExpectedErrors();
        FirebirdErrors.addTableErrors(tableExpectedErrors);
        ExpectedErrors indexExpectedErrors = new ExpectedErrors();
        FirebirdErrors.addIndexErrors(indexExpectedErrors);
        ExpectedErrors insertExpectedErrors = new ExpectedErrors();
        FirebirdErrors.addInsertErrors(insertExpectedErrors);

        List<SQLQueryAdapter> queries = new ArrayList<SQLQueryAdapter>();
        queries.add(new SQLQueryAdapter("CREATE TABLE t0(c0 INTEGER NOT NULL, c1 INTEGER, PRIMARY KEY(c0));",
                tableExpectedErrors, true));
        queries.add(new SQLQueryAdapter("CREATE TABLE t1(c0 FLOAT, c1 INTEGER, PRIMARY KEY(c1, c0));",
                tableExpectedErrors, true));
        queries.add(new SQLQueryAdapter("CREATE UNIQUE INDEX i0 ON T1(C0);", indexExpectedErrors, true));
        queries.add(new SQLQueryAdapter("INSERT INTO T1 (C0) VALUES (-1.823638703E9);", insertExpectedErrors, false));
        queries.add(new SQLQueryAdapter("INSERT INTO T1 (C1, C0) VALUES (436315175, 0.7288186586246848);",
                insertExpectedErrors, false));
        queries.add(new SQLQueryAdapter("INSERT INTO T1 (C1, C0) VALUES (1278918755, 0.8345087226113943);",
                insertExpectedErrors, false));
        queries.add(
                new SQLQueryAdapter("INSERT INTO T1 (C0) VALUES (0.6784440174830495);", insertExpectedErrors, false));
        queries.add(new SQLQueryAdapter("INSERT INTO T1 (C1, C0) VALUES (546268574, 0.6190884744047549);",
                insertExpectedErrors, false));
        queries.add(new SQLQueryAdapter("INSERT INTO T1 (C1) VALUES (662560035);", insertExpectedErrors, false));
        queries.add(new SQLQueryAdapter("INSERT INTO T0 (C0, C1) VALUES (151221046, 138801703);", insertExpectedErrors,
                false));
        queries.add(new SQLQueryAdapter("INSERT INTO T1 (C0, C1) VALUES (0.575933333230997, 973502404);",
                insertExpectedErrors, false));

        for (SQLQueryAdapter query : queries) {
            boolean success = globalState.executeStatement(query);
            if (query.couldAffectSchema()) {
                globalState.updateSchema();
            }
            System.out.print("success ");
            System.out.println(success);
        }
    }

    @Override
    public SQLConnection createDatabase(FirebirdGlobalState globalState) throws Exception {
        String databaseName = globalState.getDatabaseName();
        String username = "SYSDBA";
        String password = "masterkey";
        String host = globalState.getDmbsSpecificOptions().host;
        int port = globalState.getDmbsSpecificOptions().port;

        FBManager manager = new FBManager();
        manager.setUserName(username);
        manager.setPassword(password);
        manager.start();
        if (manager.isDatabaseExists(databaseName, username, password)) {
            manager.dropDatabase(databaseName, username, password);
        }
        manager.createDatabase(databaseName, username, password);
        manager.stop();

        String url = String.format("jdbc:firebirdsql://%s:%d/%s?charSet=utf-8", host, port, databaseName);
        return new SQLConnection(DriverManager.getConnection(url, username, password));
    }

    @Override
    public String getDBMSName() {
        return "firebird";
    }
}
