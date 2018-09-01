package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.ConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.connect.connector.ConnectorContext;

public class NoOpTableMonitorThread extends TableMonitorThread {

    AtomicBoolean running = new AtomicBoolean(true);

    public NoOpTableMonitorThread(DatabaseDialect dialect,
                                  ConnectionProvider connectionProvider,
                                  ConnectorContext context,
                                  long pollMs,
                                  Set<String> whitelist,
                                  Set<String> blacklist) {
        super(dialect, connectionProvider, context, pollMs, whitelist, blacklist);
    }

    @Override
    public void run() {
        while (running.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    @Override
    public synchronized List<TableId> tables() {
        return Collections.emptyList();
    }

    @Override
    public void shutdown() {
        running.set(false);
    }
}
