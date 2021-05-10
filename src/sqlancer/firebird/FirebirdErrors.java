package sqlancer.firebird;

import sqlancer.common.query.ExpectedErrors;

public final class FirebirdErrors {

    private FirebirdErrors() {
    }

    /*
     * public static void addExpressionErrors(ExpectedErrors errors) { errors.add("conversion error from string");
     * errors.add("arithmetic exception, numeric overflow, or string truncation; numeric value is out of range"); }
     */

    public static void addTableErrors(ExpectedErrors errors) {
        errors.add(
                "Same set of columns cannot be used in more than one PRIMARY KEY and/or UNIQUE constraint definition");
        errors.add("Attempt to define a second PRIMARY KEY for the same table");
        errors.add("can not define a not null column with NULL as default value");
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        errors.add(", value \"*** null ***\"");
        errors.add("violation of PRIMARY or UNIQUE KEY constraint");
        errors.add("attempt to store duplicate value (visible to active transactions) in unique index");
        errors.add("conversion error from string");
        errors.add("arithmetic exception, numeric overflow, or string truncation");
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.add(", value \"*** null ***\"");
        errors.add("violation of PRIMARY or UNIQUE KEY constraint");
        errors.add("attempt to store duplicate value (visible to active transactions) in unique index");
        errors.add("conversion error from string");
        errors.add("arithmetic exception, numeric overflow, or string truncation");
        errors.add("Integer overflow");
        errors.add("expression evaluation not supported");
        errors.add("The result of an integer operation caused the most significant bit of the result to carry.");
        errors.add("numeric value is out of range");
    }

    public static void addIndexErrors(ExpectedErrors errors) {
        errors.add("attempt to store duplicate value (visible to active transactions) in unique index");
    }

    public static void addUnstableErrors(ExpectedErrors errors) {
        errors.add("too many open handles to database");
        errors.add("Error writing data to the connection.");
        errors.add("Connection reset by peer: socket write error");
        errors.add("invalid request handle");
    }
}
