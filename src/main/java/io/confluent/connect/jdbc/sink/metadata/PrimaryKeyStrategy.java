package io.confluent.connect.jdbc.sink.metadata;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface PrimaryKeyStrategy {

    class PrimaryKeyFields {
        public final Set<String> keyFieldNames;
        public final Map<String, SinkRecordField> keyFields;

        public PrimaryKeyFields(Set<String> keyFieldNames, Map<String, SinkRecordField> keyFields) {
            this.keyFieldNames = keyFieldNames;
            this.keyFields = keyFields;
        }
    }

    PrimaryKeyFields get(List<String> configuredPkFields, String tableName);

}
