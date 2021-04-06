package sqlancer.firebird;

import sqlancer.common.query.ExpectedErrors;

public final class FirebirdErrors {

    private FirebirdErrors() {
    }

    public static void addTableErrors(ExpectedErrors errors) {
        errors.add(
                "Same set of columns cannot be used in more than one PRIMARY KEY and/or UNIQUE constraint definition");
        errors.add("Attempt to define a second PRIMARY KEY for the same table");
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        errors.add(", value \"*** null ***\"");
        errors.add("violation of PRIMARY or UNIQUE KEY constraint");
    }

}
