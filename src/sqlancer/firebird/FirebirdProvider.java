package sqlancer.firebird;

import java.io.File;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.firebirdsql.management.FBManager;

import sqlancer.AbstractAction;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.firebird.FirebirdProvider.FirebirdGlobalState;
import sqlancer.firebird.gen.FirebirdTableGenerator;

public class FirebirdProvider extends SQLProviderAdapter<FirebirdGlobalState, FirebirdOptions> {

    public FirebirdProvider() {
        super(FirebirdGlobalState.class, FirebirdOptions.class);
    }

    public enum Action implements AbstractAction<FirebirdGlobalState> {
        INSERT(null), //
        CREATE_INDEX(null), //
        DELETE(null), //
        UPDATE(null), //
        CREATE_VIEW(null);

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
                    ? rand.getInteger(0, globalState.getDmbsSpecificOptions().maxNumIndexes + 1) : 0;
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
        for (int i = 0; i < Randomly.fromOptions(1, globalState.getDmbsSpecificOptions().maxNumTables + 1); i++) {
            boolean success;
            do {
                SQLQueryAdapter qt = new FirebirdTableGenerator().getQuery(globalState);
                success = globalState.executeStatement(qt);
            } while (!success);
        }
        StatementExecutor<FirebirdGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                FirebirdProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
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
