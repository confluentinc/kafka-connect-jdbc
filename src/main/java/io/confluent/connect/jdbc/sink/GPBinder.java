package io.confluent.connect.jdbc.sink;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import de.siegmar.fastcsv.writer.CsvWriter;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialect.StatementBinder;
import io.confluent.connect.jdbc.gpload.GPloadConfig;
import io.confluent.connect.jdbc.gpload.GPloadConfigObj;
import io.confluent.connect.jdbc.gpload.GPloadRunner;
import io.confluent.connect.jdbc.gpload.YAMLConfig;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.ConnectionURLParser;
import io.confluent.connect.jdbc.util.TableDefinition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.*;
import java.net.Inet4Address;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static java.util.Objects.isNull;

public class GPBinder implements StatementBinder {

    private final JdbcSinkConfig.PrimaryKeyMode pkMode;
    private final SchemaPair schemaPair;
    private final FieldsMetadata fieldsMetadata;
    private final JdbcSinkConfig.InsertMode insertMode;
    private final DatabaseDialect dialect;
    private final TableDefinition tabDef;
    private final JdbcSinkConfig config;

    private List<Map<String, Object>> dataRows;

    private List<SinkRecord> records;

    @Deprecated
    public GPBinder(
            DatabaseDialect dialect,
            JdbcSinkConfig.PrimaryKeyMode pkMode,
            SchemaPair schemaPair,
            FieldsMetadata fieldsMetadata,
            JdbcSinkConfig.InsertMode insertMode
    ) {
        this(
                dialect,
                pkMode,
                schemaPair,
                fieldsMetadata,
                null,
                insertMode,
                null
        );
    }

    public GPBinder(
            DatabaseDialect dialect,
            JdbcSinkConfig.PrimaryKeyMode pkMode,
            SchemaPair schemaPair,
            FieldsMetadata fieldsMetadata,
            TableDefinition tabDef,
            JdbcSinkConfig.InsertMode insertMode,
            JdbcSinkConfig config
    ) {
        this.dialect = dialect;
        this.pkMode = pkMode;
        this.schemaPair = schemaPair;
        this.fieldsMetadata = fieldsMetadata;
        this.insertMode = insertMode;
        this.tabDef = tabDef;
        this.dataRows = new ArrayList<>();
        this.records = new ArrayList<>();
        this.config = config;
    }

    @Override
    public void bindRecord(SinkRecord record) throws SQLException {
        final Struct valueStruct = (Struct) record.value();
        final boolean isDelete = isNull(valueStruct);

        if (!isDelete) { // TODO pending delete case and update case

//            Map<String, Object> row = new HashMap<>();
//            tabDef.columnNames().forEach(field -> {
//                row.put(field, valueStruct.get(field));
//            });
//            dataRows.add(row);
            records.add(record);
        }
    }


    public void flush() {

        try {

            if(records.isEmpty()){
                return;
            }
            String tableName = tabDef.id().tableName();
            String suffix = ".csv";
            String directory = System.getProperty("java.io.tmpdir") + "/gpload/";
            new File(directory).mkdirs();
            File csvFile = File.createTempFile(tableName, suffix, new File(directory));
            FileWriter writer = new FileWriter(csvFile);
            String absolutePath = csvFile.toString();
            System.out.println("Temp CSV file : " + absolutePath);
            final List fields = Arrays.asList(fieldsMetadata.allFields.keySet().toArray()); // optimization
            try (CsvWriter csv = CsvWriter.builder().build(writer)) {

//                final List fields = Arrays.asList(tabDef.columnNames().toArray());
//                csv.writeRow(fields);
//                dataRows.forEach(dataMap -> {
//                    List data = new ArrayList(fields.size());
//                    for (int i = 0; i < fields.size(); i++) {
//                        data.add(i, dataMap.get(fields.get(i)).toString());
//                    }
//                    csv.writeRow(data);
//                });
                // upload using gpload

                csv.writeRow(fields);
                records.forEach(record -> {
                    List data = new ArrayList(fields.size());
                    final Struct valueStruct = (Struct) record.value();
                    for (int i = 0; i < fields.size(); i++) {
                        String value = String.valueOf(valueStruct.get(fields.get(i).toString()));
                        data.add(i, value);
                    }
                    csv.writeRow(data);
                });

            } catch (Exception e) {
                e.printStackTrace();
            }

            Connection connection = dialect.getConnection();
            String jdbcUrl = connection.getMetaData().getURL();
            if (jdbcUrl != null && jdbcUrl.startsWith("jdbc:")) {
                jdbcUrl = jdbcUrl.substring(5);
            }

            ConnectionURLParser parser = new ConnectionURLParser(jdbcUrl);

            if (parser.getSchema() == null) {
                parser.setSchema("public");
            }

            String localIpOrHost = null;
            try {
                localIpOrHost = Inet4Address.getLocalHost().getHostAddress();
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (localIpOrHost == null) {
                try {
                    localIpOrHost = Inet4Address.getLocalHost().getHostName();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            GPloadConfig.Source.Builder sourceBuilder = new GPloadConfig.Source.Builder()
                    .localHostname(Arrays.asList(localIpOrHost))
                    .file(Arrays.asList(csvFile.getAbsolutePath()));
            if (config.portRange.size() > 1) {
                sourceBuilder.portRange(config.portRange);
            } else if (config.portRange.size() == 1) {
                sourceBuilder.port(config.portRange.get(0));
            }
            GPloadConfig.Source source = sourceBuilder.build();
//            // convert fields to Map - column name and data type
            Map<String, String> fieldsMap = new HashMap<>();
            fields.forEach(field -> {
                fieldsMap.put(field.toString(), field.toString());
            });

            GPloadConfig.Input input = new GPloadConfig.Input.Builder()
                    .format("csv")
//                    .columns(Arrays.asList(fieldsMap))
                    .delimiter(config.csvDelimiter)
                    .errorLimit(config.gpErrorsLimit)
                    .header(true)
                    .quote(config.csvQuote)
                    .encoding(config.csvEncoding)
                    .source(source)
                    .logErrors(config.gpLogErrors)
                    .build();

            GPloadConfig.Output output = new GPloadConfig.Output.Builder()
                    .table(parser.getSchema() + "." + tableName)
                    .mode(config.insertMode.toString().toLowerCase())
                    .mapping(fieldsMap)
                    .updateColumns(new ArrayList<>(fieldsMetadata.nonKeyFieldNames))
                    .matchColumns(new ArrayList<>(fieldsMetadata.keyFieldNames)).build();

            GPloadConfig.External external = new GPloadConfig.External.Builder()
                    .schema(parser.getSchema()).build();

            GPloadConfig.Preload preload = new GPloadConfig.Preload.Builder().fastMatch(true).reuseTables(true).build();


            GPloadConfig.GPload gpload = new GPloadConfig.GPload.Builder()
                    .input(Arrays.asList(input))
                    .output(Arrays.asList(output))
                    .external(Arrays.asList(external))
                    .preload(Arrays.asList(preload)).build();
            GPloadConfig gPloadConfig = new GPloadConfig.Builder().gpload(gpload)
                    .user(parser.getUsername())
                    .password(parser.getPassword())
                    .host(parser.getHost())
                    .port(parser.getPort())
                    .database(parser.getPath()).version("1.0.0.1").build();


//            GPloadConfigObj gPloadConfig = new GPloadConfigObj();
//            gPloadConfig.setUrl(config.connectionUrl);
//            gPloadConfig.setSchema(connection.getSchema());
//            gPloadConfig.setPort(port);
//            gPloadConfig.setHost(host);
//            gPloadConfig.setDatabase(database);
//            gPloadConfig.setTable(tabDef.id().tableName());
//            gPloadConfig.setFormat("csv");
//            gPloadConfig.setInput(csvFile.getAbsolutePath());

//
            //YAMLConfig yamlConfig = new YAMLConfig(gPloadConfig);
//            yamlConfig.create();

            class UpperCaseStrategy extends PropertyNamingStrategy.PropertyNamingStrategyBase {

                @Override
                public String translate(String propertyName) {

                    return propertyName.toUpperCase();
                }
            }

            File yamlFile = File.createTempFile(tableName, ".yml", new File(directory));
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.setPropertyNamingStrategy(new UpperCaseStrategy());
            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

            mapper.writeValue(yamlFile, gPloadConfig);


            File logFile = File.createTempFile(tableName, ".log", new File(directory));

            String gploadBinary = "gpload";
            if (config.greenplumHome != null) {
                gploadBinary = config.greenplumHome + "/bin/gpload";
            }


            String gploadCommand = gploadBinary + " -l " + logFile.getAbsolutePath() + " -f " + yamlFile.getAbsolutePath();

            ArrayList<String> cmdOutput = GPloadRunner.executeCommand(gploadCommand);


            String errors = GPloadRunner.checkGPloadOutputForErrors(cmdOutput);
            if (errors.length() > 0) {
                System.out.println("Errors in GPLoad:"+ errors+"      \n Yaml: "+yamlFile.getAbsolutePath());
                System.out.println("Command: "+gploadCommand);
                System.out.println("Skipping file delete");
            }else

            if(errors.length() > 0) {

            }else if (!config.keepGpFiles) {
                deleteFile(yamlFile);
                deleteFile(logFile);
                deleteFile(csvFile);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void deleteFile(File file) {
        try {
            file.delete();
        } catch (Exception e) {

            e.printStackTrace();
        }

    }

//    public static int runGploadWithSourcing(File logFile, File yamlFile) {
//        String gploadCommand = "gpload.py -l " + logFile.getAbsolutePath() + " -f " + yamlFile.getAbsolutePath();
//        String sourceScript = "source /usr/local/gpdb/greenplum_path.sh";
//        String fullCommand = sourceScript + " && " + gploadCommand;
//
//        try {
//            ProcessBuilder processBuilder = new ProcessBuilder("bash", "-c", fullCommand);
//
//            // Redirect error stream to output to capture any error messages
//            processBuilder.redirectErrorStream(true);
//
//            Process process = processBuilder.start();
//
//            // Read and print the output of the process (if needed)
//            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
//            String line;
//            while ((line = reader.readLine()) != null) {
//                System.out.println(line);
//            }
//
//            // Wait for the process to finish
//            int exitCode = process.waitFor();
//            return exitCode;
//        } catch (IOException | InterruptedException e) {
//            e.printStackTrace();
//            return -1;
//        }
//    }
}
