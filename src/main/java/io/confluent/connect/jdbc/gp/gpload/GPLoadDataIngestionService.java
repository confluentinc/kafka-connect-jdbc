package io.confluent.connect.jdbc.gp.gpload;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import de.siegmar.fastcsv.writer.CsvWriter;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.gp.GpDataIngestionService;
import io.confluent.connect.jdbc.gp.gpload.config.GPloadConfig;
import io.confluent.connect.jdbc.sink.GPBinder;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.util.CommonUtils;
import io.confluent.connect.jdbc.util.ConnectionURLParser;
import io.confluent.connect.jdbc.util.TableDefinition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.data.Struct;

import java.io.File;
import java.io.FileWriter;
import java.util.*;

public class GPLoadDataIngestionService extends GpDataIngestionService {
    private static final Logger log = LoggerFactory.getLogger(GPBinder.class);
    private final String tempDir;
    public GPLoadDataIngestionService(JdbcSinkConfig config, DatabaseDialect dialect, TableDefinition tableDefinition, FieldsMetadata fieldsMetadata) {
       super(config,dialect, tableDefinition, fieldsMetadata);
        tempDir = System.getProperty("java.io.tmpdir") + "/gpload/";
        new File(tempDir).mkdirs();
    }

    @Override
    public void ingest(List<SinkRecord> records) {
        super.ingest(records);
        try {

            String suffix = ".csv";
            File csvFile = File.createTempFile(tableName, suffix, new File(tempDir));
            FileWriter writer = new FileWriter(csvFile);
            String absolutePath = csvFile.toString();
            log.info("Writing to file {}", absolutePath);
            try (CsvWriter csv = CsvWriter.builder().build(writer)) {
                csv.writeRow(allColumns);
                data.forEach(record -> {
                    csv.writeRow(record);
                });
                log.info("Rows count", records.size());
            } catch (Exception e) {
                log.error("Error while writing to file {}", absolutePath, e);
            }

            String localIpOrHost = getGpfDistHost();
            log.info("gpfdist running on {}", localIpOrHost);

            GPloadConfig.Source.Builder sourceBuilder = new GPloadConfig.Source.Builder()
                    .localHostname(Arrays.asList(localIpOrHost))
                    .file(Arrays.asList(csvFile.getAbsolutePath()));
            if (config.portRange.size() > 1) {
                sourceBuilder.portRange(config.portRange);
            } else if (config.portRange.size() == 1) {
                sourceBuilder.port(config.portRange.get(0));
            }
            GPloadConfig.Source source = sourceBuilder.build();

            GPloadConfig.Input input = new GPloadConfig.Input.Builder()
                    .format("csv")
                    .columns(createColumnNameDataTypeMapList())
                    .delimiter(config.delimiter)
                    .errorLimit(config.gpErrorsLimit)
                    .header(true)
                    .quote(config.csvQuote)
                    .encoding(config.csvEncoding)
                    .source(source)
                    .nullAs(config.nullString)
                    .logErrors(config.gpLogErrors)
                    .maxLineLength(config.gpMaxLineLength)
                    .build();

            GPloadConfig.Output output = new GPloadConfig.Output.Builder()
                    .table(dbConnection.getSchema() + "." + tableName)
                    .mode(config.insertMode.toString().toLowerCase())
                    //.mapping(fieldsMap) one to one mapping of source and destination fields
                    .updateColumns(nonKeyColumns)
                    .matchColumns(keyColumns).build();

            GPloadConfig.External external = new GPloadConfig.External.Builder()
                    .schema(dbConnection.getSchema()).build();

            GPloadConfig.Preload preload = new GPloadConfig.Preload.Builder().fastMatch(true).reuseTables(true).build();


            GPloadConfig.GPload gpload = new GPloadConfig.GPload.Builder()
                    .input(Arrays.asList(input))
                    .output(Arrays.asList(output))
                    .external(Arrays.asList(external))
                    .preload(Arrays.asList(preload)).build();
            GPloadConfig gPloadConfig = new GPloadConfig.Builder().gpload(gpload)
                    .user(dbConnection.getUsername())
                    .password(dbConnection.getPassword())
                    .host(dbConnection.getHost())
                    .port(dbConnection.getPort())
                    .database(dbConnection.getDatabase()).version("1.0.0.1").build();
            class UpperCaseStrategy extends PropertyNamingStrategy.PropertyNamingStrategyBase {

                @Override
                public String translate(String propertyName) {

                    return propertyName.toUpperCase();
                }
            }
            File yamlFile = File.createTempFile(tableName, ".yml", new File(tempDir));
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.setPropertyNamingStrategy(new UpperCaseStrategy());
            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            mapper.writeValue(yamlFile, gPloadConfig);

            File logFile = File.createTempFile(tableName, ".log", new File(tempDir));
            String gploadBinary = "gpload";
            if (config.greenplumHome != null) {
                gploadBinary = config.greenplumHome + "/bin/gpload";
            }

            String gploadCommand = gploadBinary + " -l " + logFile.getAbsolutePath() + " -f " + yamlFile.getAbsolutePath();
            log.info("Running gpload command {}", gploadCommand);

            ArrayList<String> cmdOutput = CommonUtils.executeCommand(gploadCommand);
            log.info("gpload output: {}", cmdOutput);

            String errors = checkGPloadOutputForErrors(cmdOutput);
            if (errors.length() > 0) {
                log.error("Errors in GPLoad:{}", errors);
                log.error("Yaml: {}", yamlFile.getAbsolutePath());
                log.error("Command: {}", gploadCommand);
                log.error("Keeping files for further analysis");
            } else {
                log.info("GPload finished successfully");
                if (!config.keepGpFiles) {
                    log.info("Deleting GP files");
                    CommonUtils.deleteFile(yamlFile);
                    CommonUtils.deleteFile(logFile);
                    CommonUtils.deleteFile(csvFile);
                } else {
                    log.info("Keeping GP files");
                }
            }
        } catch (Exception e) {
            log.error("Error running gpload", e);
            e.printStackTrace();
        }
    }

    public static String checkGPloadOutputForErrors(ArrayList<String> output) {
        String errMsg = "";

        for (int i = 0; i < output.size(); i++) {
            String line = output.get(i);
            String[] splitLine = line.split("\\|");
            if (splitLine.length > 1) {
                if (splitLine[1].contains("ERROR")) {
                    errMsg += splitLine[2];
                }
            }
        }
        return errMsg;
    }

}
