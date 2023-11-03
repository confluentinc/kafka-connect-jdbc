//package io.confluent.connect.jdbc.sink;
//
//import com.fasterxml.jackson.annotation.JsonInclude;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.PropertyNamingStrategy;
//import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
//import de.siegmar.fastcsv.writer.CsvWriter;
//import io.confluent.connect.jdbc.gpload.GPloadConfig;
//import io.confluent.connect.jdbc.gpload.GPloadRunner;
//import io.confluent.connect.jdbc.util.ConnectionURLParser;
//
//
//import java.io.File;
//import java.io.FileWriter;
//import java.net.Inet4Address;
//import java.net.MalformedURLException;
//import java.net.URL;
//import java.sql.Connection;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//
//public class Main {
//    public static void main(String[] args) {
//
//        try {
//
//            String tableName = "employee";
//            String suffix = ".csv";
//            String directory = "/Users/madnan01/Documents/confluent-7.4.1/gpload/yml/";
//            new File(directory).mkdirs();
//            File csvFile = File.createTempFile(tableName, suffix, new File(directory));
//            FileWriter writer = new FileWriter(csvFile);
//            String absolutePath = csvFile.toString();
//            System.out.println("Temp CSV file : " + absolutePath);
//
//            try (CsvWriter csv = CsvWriter.builder().build(writer)) {
//
////                final List fields = Arrays.asList(tabDef.columnNames().toArray());
////                csv.writeRow(fields);
////                dataRows.forEach(dataMap -> {
////                    List data = new ArrayList(fields.size());
////                    for (int i = 0; i < fields.size(); i++) {
////                        data.add(i, dataMap.get(fields.get(i)).toString());
////                    }
////                    csv.writeRow(data);
////                });
//                // upload using gpload
//                final List fields = Arrays.asList("id","first_name", "last_name", "age", "email", "address", "city", "state", "zip_code", "salary"); // optimization
//                csv.writeRow(fields);
//                csv.writeRow("12","John",	"Doe",	"30",	"john.doe@example.com","John","John","John","John",	"50000.00");
//
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//
//            String jdbcUrl = "jdbc:postgresql://localhost:7000/cards?user=madnan01&password=madnan01";
//            if(jdbcUrl!=null && jdbcUrl.startsWith("jdbc:")) {
//                jdbcUrl = jdbcUrl.substring(5);
//            }
//
//            ConnectionURLParser parser = new ConnectionURLParser(jdbcUrl);
//
//            String localIpOrHost = null;
//            try {
//                localIpOrHost = Inet4Address.getLocalHost().getHostAddress();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//            if (localIpOrHost == null) {
//                try {
//                    localIpOrHost = Inet4Address.getLocalHost().getHostName();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//
//            GPloadConfig.Source source = new GPloadConfig.Source.Builder()
//                    .localHostname(localIpOrHost)
//                    .port(4500) //TODO define port in config
//                    .file(Arrays.asList(csvFile.getAbsolutePath())).build();
//
//
//            GPloadConfig.Input input = new GPloadConfig.Input.Builder()
//                    .format("csv")
//                    .delimiter(",")
//                    .errorLimit(999999999)
//                    .header(true)
//                    .quote("\"")
//                    .encoding("UTF-8")
//                    .source(source)
//                    .logErrors(true)
//                    .build();
//
//            GPloadConfig.Output output = new GPloadConfig.Output.Builder()
//                    .table("public" + "." + tableName)
//                    .mode("merge")
//                    .updateColumns(Arrays.asList("first_name", "last_name", "age", "email", "address", "city", "state", "zip_code", "salary"))
//                    .matchColumns(new ArrayList<>(Arrays.asList("id"))).build();
//
//            GPloadConfig.External external = new GPloadConfig.External.Builder()
//                    .schema("public").build();
//
//            GPloadConfig.Preload preload = new GPloadConfig.Preload.Builder().build();
//
//            GPloadConfig.GPload gpload = new GPloadConfig.GPload.Builder()
//                    .input(Arrays.asList(input))
//                    .output(output)
//                    .external(external)
//                    .preload(preload).build();
//            GPloadConfig gPloadConfig = new GPloadConfig.Builder().gpload(gpload)
//                    .user(parser.getUsername())
//                    .password(parser.getPassword())
//                    .host(parser.getHost())
//                    .port(parser.getPort())
//                    .database(parser.getPath()).version("1.0.0.1").build();
//
//            class UpperCaseStrategy extends PropertyNamingStrategy.PropertyNamingStrategyBase {
//
//                @Override
//                public String translate(String propertyName) {
//
//                    return propertyName.toUpperCase();
//                }
//            }
//
//            File yamlFile = File.createTempFile(tableName, ".yml", new File(directory));
//            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
//            mapper.setPropertyNamingStrategy(new UpperCaseStrategy());
//            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
//
//            mapper.writeValue(yamlFile, gPloadConfig);
//
//
//
//
//            File logFile = File.createTempFile(tableName, ".log", new File(directory));
//
//            String gploadCommand = "gpload -l " + logFile.getAbsolutePath() + " -f " +yamlFile.getAbsolutePath() ;
//
//            ArrayList<String> cmdOutput = GPloadRunner.executeCommand(gploadCommand);
//
//
//            String errors = GPloadRunner.checkGPloadOutputForErrors(cmdOutput);
//            if (errors.length() > 0) {
//                System.out.println(errors);
//            }
//
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }
//}
