package io.confluent.connect.jdbc.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

public class ColumnNameTest {

  @Test
  public void aComplexQualifiedNameIsCorrectlyQuoted() {
    // given
    String name = "name";
    String qualifier = "a.complex.qualifier";

    // when
    ColumnName actual = new ColumnName(name, qualifier);

    // then
    assertEquals("'a'.'complex'.'qualifier'.'name'", actual.getQuotedQualifiedName("'"));
  }

  @Test
  public void aSimpleQualifiedNameIsCorrectlyQuoted() {
    // given
    String name = "name";
    String qualifier = "asimplequalifier";

    // when
    ColumnName actual = new ColumnName(name, qualifier);

    // then
    assertEquals("'asimplequalifier'.'name'", actual.getQuotedQualifiedName("'"));
  }

  @Test
  public void aNonQualifiedNameIsCorrectlyQuoted() {
    // given
    String name = "name";

    // when
    ColumnName actual = new ColumnName(name, null);

    // then
    assertEquals("'name'", actual.getQuotedQualifiedName("'"));
  }

  @Test
  public void nullNameCausesIsEmpty() {

    // when
    ColumnName actual = new ColumnName(null, "a.qualifier");

    // then
    assertTrue(actual.isEmpty());
  }

  @Test
  public void whitespaceOnlyNameCausesIsEmpty() {

    // when
    ColumnName actual = new ColumnName("\t\n  ", "a.qualifier");

    // then
    assertTrue(actual.isEmpty());
  }

  @Test
  public void emptyNameCausesIsEmpty() {
    // when
    ColumnName actual = new ColumnName("", "a.qualifier");

    // then
    assertTrue(actual.isEmpty());
  }

  @Test
  public void emptyQualifierDoesNotCauseIsEmpty() {

    // when
    ColumnName actual = new ColumnName("name", null);

    // then
    assertFalse(actual.isEmpty());
  }

  @Test
  public void nameAndQualifierAreConsideredForEquals() {
    // given
    ColumnName base = new ColumnName("name", "a.qualifier");
    ColumnName equal = new ColumnName("name", "a.qualifier");
    ColumnName notEqualByName = new ColumnName("anotherName", "a.qualifier");
    ColumnName notEqualByNullName = new ColumnName(null, "a.qualifier");
    ColumnName notEqualByQualifier = new ColumnName("name", "another.qualifier");
    ColumnName notEqualByNullQualifier = new ColumnName("name", null);

    // then
    assertEquals(base, equal);
    assertNotEquals(base, notEqualByName);
    assertNotEquals(base, notEqualByNullName);
    assertNotEquals(base, notEqualByQualifier);
    assertNotEquals(base, notEqualByNullQualifier);
  }

  @Test
  public void nameAndQualifierAreConsideredForHashCode() {

    // given
    ColumnName base = new ColumnName("name", "a.qualifier");
    ColumnName equal = new ColumnName("name", "a.qualifier");
    ColumnName notEqualByName = new ColumnName("anotherName", "a.qualifier");
    ColumnName notEqualByNullName = new ColumnName(null, "a.qualifier");
    ColumnName notEqualByQualifier = new ColumnName("name", "another.qualifier");
    ColumnName notEqualByNullQualifier = new ColumnName("name", null);

    // when
    final List<ColumnName> columnNames = listOf(base, equal, notEqualByName, notEqualByNullName, notEqualByQualifier, notEqualByNullQualifier);
    Set<ColumnName> uniques = new HashSet<>(columnNames);

    // then
    assertTrue(uniques.size() == columnNames.size() - 1);
    assertTrue(uniques.containsAll(columnNames));
  }

  @Test
  public void emptyFactoryGivesEmptyInstance() {
    // given
    ColumnName emptyInstance = ColumnName.empty();

    // when
    boolean actual = emptyInstance.isEmpty();

    // then
    assertTrue(actual);
  }

  @SafeVarargs
  private final <T> List<T> listOf(T... objects) {
    return new ArrayList<>(Arrays.asList(objects));
  }
}