
package io.confluent.connect.jdbc.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreparedStatementProxy implements java.lang.reflect.InvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(PreparedStatementProxy.class);

    private PreparedStatement ps;

    public static PreparedStatement newInstance(PreparedStatement stmt) {
        return (PreparedStatement) java.lang.reflect.Proxy.newProxyInstance(
                stmt.getClass().getClassLoader(),
                stmt.getClass().getInterfaces(),
                new PreparedStatementProxy(stmt));
    }

    private PreparedStatementProxy(PreparedStatement ps) {
        this.ps = ps;
    }

    public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
        log.trace("proxy called {}", m.getName());

        Object result;
        try {
            result = m.invoke(ps, args);

            if (m.getName().equals("close")) {
                tryCommit();
            }

        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        } catch (Exception e) {
            log.error("{} during transaction commit: {}",
                      e.getClass().getName(),
                      e.getMessage());

            throw new RuntimeException("unexpected invocation exception: " + e.getMessage());
        }

        return result;
    }

    private void tryCommit() {
        try {
            ps.getConnection().commit();
        } catch (SQLException e) {
            log.warn("proxy failed to commit on statement close, {}", e.getMessage());
            log.trace("{}", e);
            // swallow exception
        }
    }

}