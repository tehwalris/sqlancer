package sqlancer.firebird;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.firebird.FirebirdOptions.FirebirdOracleFactory;
import sqlancer.firebird.FirebirdProvider.FirebirdGlobalState;

@Parameters
public class FirebirdOptions implements DBMSSpecificOptions<FirebirdOracleFactory> {

    // TODO: add options here

    @Parameter(names = "--test-not-null", description = "Allow generating NOT NULL constraints in tables", arity = 1)
    public boolean testNotNullConstraints = true;

    @Parameter(names = "--test-indexes", description = "Allow explicit (i.e. CREATE INDEX) and implicit (i.e., UNIQUE and PRIMARY KEY) indexes", arity = 1)
    public boolean testIndexes = true;

    @Parameter(names = "--max-num-deletes", description = "The maximum number of DELETE statements that are issued for a database", arity = 1)
    public int maxNumDeletes = 1;

    @Parameter(names = "--max-num-indexes", description = "The maximum number of indexes that can be generated for a database", arity = 1)
    public int maxNumIndexes = 5;

    @Parameter(names = "--max-num-tables", description = "The maximum number of tables that can be generated for a database", arity = 1)
    public int maxNumTables = 1;

    @Parameter(names = "--max-num-updates", description = "The maximum number of UPDATE statements that are issued for a database", arity = 1)
    public int maxNumUpdates = 5;

    @Parameter(names = "--max-num-views", description = "The maximum number of views that can be generated for a database", arity = 1)
    public int maxNumViews = 1;

    @Parameter(names = "--oracle")
    public List<FirebirdOracleFactory> oracles = Arrays.asList(); // TODO: replace with default oracle option

    public enum FirebirdOracleFactory implements OracleFactory<FirebirdGlobalState> {

        // TODO: add oracles of Firebird here
        DUMMY {
            @Override
            public TestOracle create(FirebirdGlobalState globalState) throws SQLException {
                return null;
            }
        };

    }

    @Override
    public List<FirebirdOracleFactory> getTestOracleFactory() {
        return oracles;
    }
}
