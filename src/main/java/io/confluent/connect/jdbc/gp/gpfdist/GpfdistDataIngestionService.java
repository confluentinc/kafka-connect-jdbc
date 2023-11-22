package io.confluent.connect.jdbc.gp.gpfdist;

import api.*;
import com.google.protobuf.ByteString;
import io.confluent.connect.jdbc.gp.GpDataIngestionService;
import io.confluent.connect.jdbc.gp.gpfdist.framweork.GpfdistServer;
import io.confluent.connect.jdbc.gp.gpfdist.framweork.GpfdistSinkProperties;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.util.ConnectionURLParser;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.hadoop.util.net.HostInfoDiscovery;
import org.springframework.scheduling.TaskScheduler;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class GpfdistDataIngestionService extends GpDataIngestionService {

    private static final Logger log = LoggerFactory.getLogger(GpfdistDataIngestionService.class);
    ManagedChannel channel = null;
    Session mSession = null;
    GpssGrpc.GpssBlockingStub bStub = null;
    public GpfdistDataIngestionService(JdbcSinkConfig config, TableDefinition tabDef, FieldsMetadata fieldsMetadata) {
        super(config, tabDef, fieldsMetadata);
    }

    public GpfdistDataIngestionService(JdbcSinkConfig config, String tableName, FieldsMetadata fieldsMetadata) {
        super(config, tableName, fieldsMetadata);
    }

    private void setupGpfDistServer(){



            GpfdistMessageHandler handler = new GpfdistMessageHandler(config.getGpfdistPort(), config.getFlushCount(),
                    properties.getFlushTime(), properties.getBatchTimeout(), properties.getBatchCount(), properties.getBatchPeriod(),
                    properties.getDelimiter(), hostInfoDiscovery);
            handler.setRateInterval(properties.getRateInterval());
            handler.setGreenplumLoad(greenplumLoad);
            handler.setSqlTaskScheduler(sqlTaskScheduler);
            return handler;
        }


    }

    @Override
    public void ingest(List<SinkRecord> records) {

        try {

        }catch (Exception e) {
            log.error("Exception while writing to GP table", e);
            //throw new RuntimeException("Exception while writing to GP table", e);
        }
    }

}
