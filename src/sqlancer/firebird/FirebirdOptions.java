package sqlancer.firebird;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.firebird.FirebirdOptions.FirebirdOracleFactory;
import sqlancer.firebird.FirebirdProvider.FirebirdGlobalState;
import sqlancer.firebird.test.FirebirdPredicateCombiningWhereTester;

@Parameters
public class FirebirdOptions implements DBMSSpecificOptions<FirebirdOracleFactory> {

    // TODO: add options here

    @Parameter(names = "--host", description = "Specifies the host for connecting to the Firebird Server", arity = 1)
    public String host = "localhost";

    @Parameter(names = "--port", description = "Specifies the port for connecting to the Firebird Server", arity = 1)
    public int port = 3050;

    @Parameter(names = "--test-default-values", description = "Allow generating DEFAULT values in tables", arity = 1)
    public boolean testDefaultValues = true;

    @Parameter(names = "--test-indexes", description = "Allow explicit (i.e. CREATE INDEX) and implicit (i.e., UNIQUE and PRIMARY KEY) indexes", arity = 1)
    public boolean testIndexes = true;

    @Parameter(names = "--test-not-null", description = "Allow generating NOT NULL constraints in tables", arity = 1)
    public boolean testNotNullConstraints = true;

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

    @Parameter(names = "--num-oracle-predicates", description = "The number of predicates used in the predicate combining oracle", arity = 1)
    public int numOraclePredicates = 10;

    @Parameter(names = "--oracle")
    public List<FirebirdOracleFactory> oracles = Arrays.asList(FirebirdOracleFactory.PREDICATE_COMBINING);

    public enum FirebirdOracleFactory implements OracleFactory<FirebirdGlobalState> {

        WHERE {
            @Override
            public TestOracle create(FirebirdGlobalState globalState) throws SQLException {
                return new FirebirdPredicateCombiningWhereTester(globalState);
            }
        },
        PREDICATE_COMBINING {
            @Override
            public TestOracle create(FirebirdGlobalState globalState) throws SQLException {
                List<TestOracle> oracles = new ArrayList<>();
                oracles.add(new FirebirdPredicateCombiningWhereTester(globalState));
                return new CompositeTestOracle(oracles, globalState);
            }
        };
    }

    @Override
    public List<FirebirdOracleFactory> getTestOracleFactory() {
        return oracles;
    }
}
