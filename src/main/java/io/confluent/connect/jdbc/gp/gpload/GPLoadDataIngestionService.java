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
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.CommonUtils;
import io.confluent.connect.jdbc.util.TableDefinition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class GPLoadDataIngestionService extends GpDataIngestionService {
    private static final Logger log = LoggerFactory.getLogger(GPBinder.class);
    private static ConcurrentHashMap<String, String> gpFiles = new ConcurrentHashMap<>();
    private final String tempDir;
    public GPLoadDataIngestionService(JdbcSinkConfig config, DatabaseDialect dialect, TableDefinition tableDefinition, FieldsMetadata fieldsMetadata, SchemaPair schemaPair){
       super(config,dialect, tableDefinition, fieldsMetadata, schemaPair);
        tempDir = System.getProperty("java.io.tmpdir") + "/gpload/";
        new File(tempDir).mkdirs();
    }


    @Override
    public void ingest(List<SinkRecord> records) {
        super.ingest(records);
        try {

            String suffix = ".csv";
            String fileName = tableName;

            File csvFile = File.createTempFile(fileName, suffix, new File(tempDir));
            File yamlFile = File.createTempFile(fileName, ".yml", new File(tempDir));
            File logFile = File.createTempFile(fileName, ".log", new File(tempDir));

            FileWriter writer = new FileWriter(csvFile);
            String absolutePath = csvFile.toString();
            log.info("Writing to file {}", absolutePath);
            try (CsvWriter csv = CsvWriter.builder().build(writer)) {
                csv.writeRow(insertColumnsList);
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
                    .columns(columnsWithDataType)
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
                    .updateColumns(updateColumnsList)
                    .matchColumns(keyColumns).build();

            GPloadConfig.External external = new GPloadConfig.External.Builder()
                    .schema(dbConnection.getSchema()).build();

            GPloadConfig.Preload preload = new GPloadConfig.Preload.Builder().fastMatch(config.gpFastMatch).reuseTables(config.gpReuseTable).build();


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


            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.setPropertyNamingStrategy(new UpperCaseStrategy());
            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            mapper.writeValue(yamlFile, gPloadConfig);

            String gploadBinary = "gpload";
            if (config.greenplumHome != null) {
                gploadBinary = config.greenplumHome + "/bin/gpload";
            }

            // check if there are any pending files

            loadFile(gploadBinary, yamlFile, csvFile, logFile);

            //if(!config.keepGpFiles) {
                loadPendingFiles(gploadBinary);
            //}
        } catch (Exception e) {
            log.error("Error running gpload", e);
            e.printStackTrace();
        }
    }

    private void loadPendingFiles(String gploadBinary) {
        log.info("Checking for pending files");
        File[] files = new File(tempDir).listFiles();
        if (files == null || files.length == 0) {
            log.info("No pending files found");
            return;
        }

        for (File file : files) {
            if (file.getName().endsWith(".yml")) {
                log.info("Pending file {}", file.getAbsolutePath());

                try {

                    // load file only if it is there for more than 2 minutes
// check if file is already opened by another thread

                    if(file.lastModified() > System.currentTimeMillis() - config.gploadRetryLoadInitialInterval){
                        log.info("File {} is not old enough to be re-loaded", file.getName());
                        continue;
                    }

                    String name = file.getName().replace(".yml", "");

                    if(gpFiles.putIfAbsent(name, name) == null) {
                        // log thread id
                        log.info("Loading pending file {} by thread id {}", name, Thread.currentThread().getId());

                        // extract name and create csv and log file with same names
                        File csvFile = new File(tempDir + name + ".csv");
                        File logFile = new File(tempDir + name + ".log");
                        //log
                        loadFile(gploadBinary, file, csvFile, logFile);

                    }else{
                        log.info("File {} is already being processed", name);
                    }

                }catch (Exception e){
                    log.error("Error while loading pending files", e);
                }
            }
        }

    }
    private void loadFile(String gploadBinary, File yamlFile, File csvFile, File logFile) throws Exception{
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
            // rename to failed (after 5 minutes) because if failed once, it is posibility that it will fail again

            // check if file is more than 5 minutes old
            if( config.gploadFileRetentionTime != -1 && yamlFile.lastModified() < System.currentTimeMillis() - config.gploadFileRetentionTime) {
                String name = yamlFile.getName().replace(".yml", "");
                yamlFile.renameTo(new File(tempDir + name + ".yml.failed"));
                logFile.renameTo(new File(tempDir + name + ".log.failed"));
                csvFile.renameTo(new File(tempDir + name + ".csv.failed"));
            }

        } else {
            log.info("GPload finished successfully");
            if (!config.keepGpFiles) {
                log.info("Deleting GP files");
                CommonUtils.deleteFile(yamlFile);
                CommonUtils.deleteFile(logFile);
                CommonUtils.deleteFile(csvFile);
            } else {
                log.info("Keeping GP files");
                // TODO move files to other backup dir for analysis purpose
                // rename all files to avoid conflicts
                String name = yamlFile.getName().replace(".yml", "");
                yamlFile.renameTo(new File(tempDir + name + ".yml.done"));
                logFile.renameTo(new File(tempDir + name + ".log.done"));
                csvFile.renameTo(new File(tempDir + name + ".csv.done"));

            }
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

    public static Boolean checkForGploadBinariesInPath() {
        String command = "which gpload";
        boolean isInPath = false;

        try {
            List<String> output = CommonUtils.executeCommand(command);

            isInPath = output.stream().anyMatch(line -> line.contains("no gpload") || line.contains("gpload not found"));

        } catch (Exception e) {
            log.error("Error while executing command to find PATH", e);
        }

        if (isInPath) {
            log.info("Gpload binaries are in PATH");
        } else {
            log.warn("Gpload binaries are not in PATH");
        }

        return isInPath;
    }

}
