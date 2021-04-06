package sqlancer.firebird;

import sqlancer.common.query.ExpectedErrors;

public final class FirebirdErrors {

    private FirebirdErrors() {
    }

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
    }

    public static void addIndexErrors(ExpectedErrors errors) {
        errors.add("attempt to store duplicate value (visible to active transactions) in unique index");
    }

}
