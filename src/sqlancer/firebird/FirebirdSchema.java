package sqlancer.firebird;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.firebird.FirebirdProvider.FirebirdGlobalState;
import sqlancer.firebird.FirebirdSchema.FirebirdTable;

public class FirebirdSchema extends AbstractSchema<FirebirdGlobalState, FirebirdTable> {

    // Boolean is only supported for Firebird version 3.0 and later
    public enum FirebirdDataType {

        INTEGER, FLOAT, BOOLEAN, TIMESTAMP, DATE, CHAR;

        public static FirebirdDataType getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public static class FirebirdColumn extends AbstractTableColumn<FirebirdTable, FirebirdDataType> {

        private final boolean isPrimaryKey;
        private final boolean isNullable;

        public FirebirdColumn(String name, FirebirdDataType columnType, boolean isPrimaryKey, boolean isNullable) {
            super(name.replaceAll("\\s+", ""), null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.isNullable = isNullable;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public boolean isNullable() {
            return isNullable;
        }

    }

    public static class FirebirdTables extends AbstractTables<FirebirdTable, FirebirdColumn> {

        public FirebirdTables(List<FirebirdTable> tables) {
            super(tables);
        }

    }

    public FirebirdSchema(List<FirebirdTable> databaseTables) {
        super(databaseTables);
    }

    public FirebirdTables getRandomTableNonEmptyTables() {
        return new FirebirdTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    private static FirebirdDataType getColumnType(String typeCode) {
        FirebirdDataType columnType;
        switch (typeCode) {
        case "14": // CHAR
        	columnType = FirebirdDataType.CHAR;
        	break;
        case "8": // INTEGER
        case "16": // BIG INTEGER
            columnType = FirebirdDataType.INTEGER;
            break;
        case "10": // FLOAT
        case "27": // DOUBLE PRECISION
            columnType = FirebirdDataType.FLOAT;
            break;
        case "23": // BOOLEAN
            columnType = FirebirdDataType.BOOLEAN;
            break;
        case "35": // TIMESTAMP
            columnType = FirebirdDataType.TIMESTAMP;
            break;
        case "12": // DATE
            columnType = FirebirdDataType.DATE;
            break;
        default:
            throw new AssertionError(typeCode);
        }

        return columnType;
    }

    public static class FirebirdTable
            extends AbstractRelationalTable<FirebirdColumn, FirebirdIndex, FirebirdGlobalState> {

        public FirebirdTable(String tableName, List<FirebirdColumn> columns, List<FirebirdIndex> indexes,
                boolean isView) {
            super(tableName.replaceAll("\\s+", ""), columns, indexes, isView);
        }

    }

    public static class FirebirdIndex extends TableIndex {

        public FirebirdIndex(String indexName) {
            super(indexName.replaceAll("\\s+", ""));
        }

    }

    public static FirebirdSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<FirebirdTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        List<String> viewNames = getViewNames(con);

        databaseTables.addAll(getDatabaseTables(con, tableNames, false));
        databaseTables.addAll(getDatabaseTables(con, viewNames, true));

        return new FirebirdSchema(databaseTables);
    }

    private static List<FirebirdTable> getDatabaseTables(SQLConnection con, List<String> tableNames, boolean isView)
            throws SQLException {
        List<FirebirdTable> databaseTables = new ArrayList<>();
        for (String tableName : tableNames) {
            List<FirebirdColumn> databaseColumns = getTableColumns(con, tableName);
            List<FirebirdIndex> indexes = getIndexes(con, tableName);
            FirebirdTable t = new FirebirdTable(tableName, databaseColumns, indexes, isView);
            for (FirebirdColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);
        }

        return databaseTables;
    }

    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT RDB$RELATION_NAME " + "FROM RDB$RELATIONS "
                    + "WHERE RDB$VIEW_BLR IS NULL AND (RDB$SYSTEM_FLAG IS NULL OR RDB$SYSTEM_FLAG = 0);")) {

                while (rs.next()) {
                    tableNames.add(rs.getString("RDB$RELATION_NAME"));
                }
            }
        }
        return tableNames;
    }

    private static List<String> getViewNames(SQLConnection con) throws SQLException {
        List<String> viewNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT RDB$RELATION_NAME " + "FROM RDB$RELATIONS "
                    + "WHERE RDB$VIEW_BLR IS NOT NULL AND (RDB$SYSTEM_FLAG IS NULL OR RDB$SYSTEM_FLAG = 0);")) {

                while (rs.next()) {
                    viewNames.add(rs.getString("RDB$RELATION_NAME"));
                }
            }
        }
        return viewNames;
    }

    private static List<FirebirdColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<String> primaryKeys = getPrimaryKeys(con, tableName);

        List<FirebirdColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT rf.RDB$FIELD_NAME, f.RDB$FIELD_TYPE, rf.RDB$NULL_FLAG "
                    + "FROM RDB$RELATION_FIELDS rf " + "JOIN RDB$FIELDS f ON f.RDB$FIELD_NAME = rf.RDB$FIELD_SOURCE "
                    + "WHERE rf.RDB$RELATION_NAME = '" + tableName + "';")) {

                while (rs.next()) {
                    String columnName = rs.getString("RDB$FIELD_NAME");
                    String dataType = rs.getString("RDB$FIELD_TYPE");
                    boolean isNullable = Objects.equals(rs.getString("RDB$NULL_FLAG"), "null");
                    boolean isPrimaryKey = primaryKeys.contains(columnName.replaceAll("\\s+", ""));
                    FirebirdColumn c = new FirebirdColumn(columnName, getColumnType(dataType), isPrimaryKey,
                            isNullable);
                    columns.add(c);
                }
            }
        }
        return columns;
    }

    // May return multiple primary keys constituting a composite primary key
    private static List<String> getPrimaryKeys(SQLConnection con, String tableName) throws SQLException {
        List<String> primaryKeys = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT seg.RDB$FIELD_NAME " + "FROM RDB$INDICES ix "
                    + "JOIN RDB$INDEX_SEGMENTS seg ON seg.RDB$INDEX_NAME = ix.RDB$INDEX_NAME "
                    + "JOIN RDB$RELATION_CONSTRAINTS constr ON constr.RDB$INDEX_NAME = ix.RDB$INDEX_NAME "
                    + "WHERE ix.RDB$RELATION_NAME = '" + tableName
                    + "' AND constr.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY';")) {

                while (rs.next()) {
                    primaryKeys.add(rs.getString("RDB$FIELD_NAME").replaceAll("\\s+", ""));
                }
            }
        }
        return primaryKeys;
    }

    private static List<FirebirdIndex> getIndexes(SQLConnection con, String tableName) throws SQLException {
        List<FirebirdIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(
                    "SELECT RDB$INDEX_NAME FROM RDB$INDICES WHERE RDB$RELATION_NAME = '" + tableName + "';")) {
                while (rs.next()) {
                    String indexName = rs.getString("RDB$INDEX_NAME");
                    indexes.add(new FirebirdIndex(indexName));
                }
            }
        }
        return indexes;
    }

}
