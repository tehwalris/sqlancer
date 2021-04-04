package sqlancer.firebird.ast;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import sqlancer.common.ast.newast.Node;

public class FirebirdConstant implements Node<FirebirdExpression> {

  private FirebirdConstant() {
  }

  public static class FirebirdNullConstant extends FirebirdConstant {

    @Override
    public String toString() {
      return "NULL";
    }

  }

  public static class FirebirdIntConstant extends FirebirdConstant {

    private final long value;

    public FirebirdIntConstant(long value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

    public long getValue() {
      return value;
    }

  }

  public static class FirebirdDoubleConstant extends FirebirdConstant {

    private final double value;

    public FirebirdDoubleConstant(double value) {
      this.value = value;
    }

    public double getValue() {
      return value;
    }

    @Override
    public String toString() {
      if (value == Double.POSITIVE_INFINITY) {
        return "'+Inf'";
      } else if (value == Double.NEGATIVE_INFINITY) {
        return "'-Inf'";
      }
      return String.valueOf(value);
    }

  }

  public static class FirebirdTextConstant extends FirebirdConstant {

    private final String value;

    public FirebirdTextConstant(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return "'" + value.replace("'", "''") + "'";
    }

  }

  public static class FirebirdDateConstant extends FirebirdConstant {

    public String textRepr;

    public FirebirdDateConstant(long val) {
      Timestamp timestamp = new Timestamp(val);
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
      textRepr = dateFormat.format(timestamp);
    }

    public String getValue() {
      return textRepr;
    }

    @Override
    public String toString() {
      return String.format("DATE '%s'", textRepr);
    }

  }

  public static class FirebirdTimestampConstant extends FirebirdConstant {

    public String textRepr;

    public FirebirdTimestampConstant(long val) {
      Timestamp timestamp = new Timestamp(val);
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      textRepr = dateFormat.format(timestamp);
    }

    public String getValue() {
      return textRepr;
    }

    @Override
    public String toString() {
      return String.format("TIMESTAMP '%s'", textRepr);
    }

  }

  public static class FirebirdBooleanConstant extends FirebirdConstant {

    private final boolean value;

    public FirebirdBooleanConstant(boolean value) {
      this.value = value;
    }

    public boolean getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

  }

  public static Node<FirebirdExpression> createStringConstant(String text) {
    return new FirebirdTextConstant(text);
  }

  public static Node<FirebirdExpression> createFloatConstant(double val) {
    return new FirebirdDoubleConstant(val);
  }

  public static Node<FirebirdExpression> createIntConstant(long val) {
    return new FirebirdIntConstant(val);
  }

  public static Node<FirebirdExpression> createNullConstant() {
    return new FirebirdNullConstant();
  }

  public static Node<FirebirdExpression> createBooleanConstant(boolean val) {
    return new FirebirdBooleanConstant(val);
  }

  public static Node<FirebirdExpression> createDateConstant(long integer) {
    return new FirebirdDateConstant(integer);
  }

  public static Node<FirebirdExpression> createTimestampConstant(long integer) {
    return new FirebirdTimestampConstant(integer);
  }

}
