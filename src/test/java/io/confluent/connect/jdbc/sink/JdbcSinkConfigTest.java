package io.confluent.connect.jdbc.sink;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JdbcSinkConfigTest {
    public static class FakeDialect {}
    @Test
    public void connectionParams() {

        final Map<String, String> props = new HashMap<>();
        final String fakeDialectClass = FakeDialect.class.getName();
        final String fakeConnectionUrl = "custom://fake_url";
        final String fakeUsername = "fake_username";
        final String fakePassword = "fake_password";
        props.put("connection.dialect", fakeDialectClass);
        props.put("connection.url", fakeConnectionUrl);
        props.put("connection.user", fakeUsername);
        props.put("connection.password", fakePassword);
        JdbcSinkConfig sut = new JdbcSinkConfig(props);
        assertEquals(FakeDialect.class, sut.connectionDialect);
        assertEquals(fakeConnectionUrl, sut.connectionUrl);
        assertEquals(fakeUsername, sut.connectionUser);
        assertEquals(fakePassword, sut.connectionPassword);

        props.remove("connection.dialect");
        sut = new JdbcSinkConfig(props);
        assertNull(sut.connectionDialect);
        assertEquals(fakeConnectionUrl, sut.connectionUrl);
        assertEquals(fakeUsername, sut.connectionUser);
        assertEquals(fakePassword, sut.connectionPassword);
    }
}
