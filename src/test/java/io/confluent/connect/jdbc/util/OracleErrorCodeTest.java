package io.confluent.connect.jdbc.util;
import org.junit.Assert; import org.junit.Test;
public class OracleErrorCodeTest {
  @Test public void standardAllowlistedCodeIsFormatted() {
    Assert.assertEquals("ORA-00001", OracleErrorCode.tag(1));
    Assert.assertEquals("ORA-12899", OracleErrorCode.tag(12899));
  }
  @Test public void customRangeCollapsesToConstant() {
    Assert.assertEquals("application_error", OracleErrorCode.tag(20001));
    Assert.assertEquals("application_error", OracleErrorCode.tag(20999));
  }
  @Test public void nonAllowlistedNonCustomIsNull() {
    Assert.assertNull(OracleErrorCode.tag(942));
    Assert.assertNull(OracleErrorCode.tag(0));
    Assert.assertNull(OracleErrorCode.tag(19999));
    Assert.assertNull(OracleErrorCode.tag(21000));
  }
}
