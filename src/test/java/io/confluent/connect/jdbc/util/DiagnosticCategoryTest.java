package io.confluent.connect.jdbc.util;
import org.junit.Assert; import org.junit.Test;
public class DiagnosticCategoryTest {
  @Test public void exactAllowlistedStateIsEmittable() {
    DiagnosticCategory.Classification c = DiagnosticCategory.classify("23505");
    Assert.assertEquals(DiagnosticCategory.UNIQUE_VIOLATION, c.category);
    Assert.assertEquals("23505", c.canonicalSqlState);
  }
  @Test public void fakeStateInKnownClassEmitsNoState() {
    DiagnosticCategory.Classification c = DiagnosticCategory.classify("23PII");
    Assert.assertEquals(DiagnosticCategory.INTEGRITY_CONSTRAINT_VIOLATION, c.category);
    Assert.assertNull(c.canonicalSqlState);
  }
  @Test public void unknownIsSqlError() {
    Assert.assertEquals(DiagnosticCategory.SQL_ERROR, DiagnosticCategory.classify("ZZZZZ").category);
    Assert.assertNull(DiagnosticCategory.classify("ZZZZZ").canonicalSqlState);
    Assert.assertEquals(DiagnosticCategory.SQL_ERROR, DiagnosticCategory.classify(null).category);
  }
  @Test public void labelIsSnake() {
    Assert.assertEquals("not_null_violation", DiagnosticCategory.NOT_NULL_VIOLATION.label());
  }
}
