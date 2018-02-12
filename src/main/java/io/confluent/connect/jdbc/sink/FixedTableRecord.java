package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.sink.metadata.MetadataExtractor;
import io.confluent.connect.jdbc.sink.metadata.PrimaryKeyFromValueSchemaMetadataExtractor;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Set;

public class FixedTableRecord implements WriteableRecord {

    public final SinkRecord sinkRecord;

    public final String tableName;

    public final Set<String> primaryKeys;


    public static FixedTableRecord newInstance(SinkRecord sinkRecord, String tableName, Set<String> primaryKeys){
        if(sinkRecord.valueSchema() == null){
            throw new IllegalArgumentException("Cannot create a fixed table record for " + tableName  + " with no schema value");
        }
        if(primaryKeys==null || primaryKeys.isEmpty()){
            throw new IllegalArgumentException("Cannot create a fixed table record for " + tableName  + " with no primary keys");
        }
        for(String primaryKey:primaryKeys){
            if(sinkRecord.valueSchema().field(primaryKey)==null){
                throw new IllegalArgumentException(("Cannot create fixed table record for " + tableName + " because " + primaryKey + " is not a part of the value schema"));
            }
        }
        return new FixedTableRecord(sinkRecord,tableName,primaryKeys);
    }

    private FixedTableRecord(SinkRecord sinkRecord, String tableName, Set<String> primaryKeys) {
        this.sinkRecord = sinkRecord;
        this.tableName = tableName;
        this.primaryKeys = primaryKeys;
    }

    @Override
    public SinkRecord getRecord() {
        return this.sinkRecord;
    }

    @Override
    public String getTableName(JdbcSinkConfig config) {
        return this.tableName;
    }

    @Override
    public MetadataExtractor getMetadataExtractor() {
            return new PrimaryKeyFromValueSchemaMetadataExtractor(primaryKeys);
    }
}
