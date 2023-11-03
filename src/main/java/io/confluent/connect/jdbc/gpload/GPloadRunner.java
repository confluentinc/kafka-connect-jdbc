
package io.confluent.connect.jdbc.gpload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;


/**
 * @author Muji Qadri
 * Created by qadrim on 15-02-25.
 *
 * Class for running GPload to bulkload data into HAWQ and Greenplum
 * =================================================================================
 * args = --input=resources/inputfile.csv --table=demo300.SXDTable --url=192.168.1.46:8080/pivotal --user=gpadmin --delimiter=,
 * optional agrs    --timeout {seconds} (default 30 sec)
 *                  --truncate {true|false} (truncates destination table before loading)
 *                  --reuse {true|false} (reuse existing external tables if they exists, minimized catalog use)
 *                  --format {text|csv} (defaults to text if not specified)
 *                  --escape {char} (specifies a single ASCII character such as \n, \t, \100) for escapaing data chars
 *                          which might otherwise be taken as row or column delimiters, use char which is not used anywhere
 *                          in your actual column data. Default escape is a \ (backslash) for text-formatted files and
 *                          " (double quotes) for csv.)
 *                  --null_as {string} (specifies the string that represents a null value. default is \N in text mode,
 *                          and an empty value with no quotations in csv mode
 *                  --quote {char} default is double-quote(")
 *                  --header {true|false} (Specifies that the first line in the data file(s) is a header row and should
 *                          not be included in the data, default is false
 *                  --encoding {database_encoding} (Character encoding of the source data, such as 'SQL_ASCII', an integer
 *                          encoding number of 'DEFAULT' to use the default client encoding.
 *                  --error_limit {int} (Input rows that have format errors will be discarded provided that the error
 *                          limit count is not reached on a segment, default is set to 1000 rows
 * 1) Download Greenplum Loaders for the specific O/S from https://network.pivotal.io/products/pivotal-gpdb
 * 2) Greenplum loaders should be configured on all spring-xd nodes and greenplum_loaders_path.sh should be added to bash profile
 * 3) User's home directory should have ~/.pgpass file which should have the password to connect to the database
 *    .pgpass sample content = 192.168.1.46:5432:*:gpadmin:password
 * 4) Alternative to .pgpass is setting $PGPASSWORD in the env
 */


public class GPloadRunner {
    private static final Logger logger = LoggerFactory.getLogger(GPloadRunner.class);

    private static GPloadConfigObj gploadConfig = new GPloadConfigObj();
    private static YAMLConfig yamlConfig;

    private static final String WORKING_DIR = "./";

    public static void main(String[] args) {
        if (args == null  || args.length < 1){
            throw new IllegalArgumentException("Missing arguments and/or configuration options for Gpload");
        }

        parseConfigOptions(args,gploadConfig);

        String errParams = gploadConfig.validateParams();

        if(errParams.length()>0){
            throw new IllegalArgumentException(errParams.toString());
        }

        logger.info("GPload Using args:" + gploadConfig.toString());

        yamlConfig = new YAMLConfig(gploadConfig);
        yamlConfig.create();

        ArrayList<String> output = executeCommand(yamlConfig.getKeyGploadCommand());

        logger.info("GPload output:" + output.toString());
        System.out.println(output.toString());

        String errors = checkGPloadOutputForErrors(output);
        if ( errors.length() > 0) {
            throw new RuntimeException("Gpload failed - Error Message  " + errors.toString());
        }

    }

    public static void parseConfigOptions(String[] args, GPloadConfigObj configOptions) {

        String s_args = "";
        for(int i = 0; i < args.length; i++) {

            s_args += args[i].toString() + " ";
            String[] arrParam = args[i].split("=");
            String key = arrParam[0].substring(2);
            String value = arrParam[1];

            configOptions.loadParameter(key,value);

        }
        configOptions.setArgs(s_args);
    }


    public static ArrayList<String> executeCommand(String command) {

        ArrayList<String> output = new ArrayList<String>();

        Process p;

        try {

                p = Runtime.getRuntime().exec(command);
                p.waitFor();
                BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

                String line = "";
                while (( line = reader.readLine()) != null) {
                    output.add(line + "\n" );
                }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return output;
    }


    public static String checkGPloadOutputForErrors(ArrayList<String> output) {
        String errMsg = "";

        for(int i=0; i<output.size(); i++){
            String line = output.get(i);
            String[] splitLine = line.split("\\|");
            if (splitLine.length >1) {
                if (splitLine[1].contains("ERROR")) {
                    errMsg += splitLine[2];
                }
            }
        }
        return errMsg;
    }

}
