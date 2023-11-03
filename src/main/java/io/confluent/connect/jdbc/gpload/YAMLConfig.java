package io.confluent.connect.jdbc.gpload;

import java.io.*;
import java.net.Inet4Address;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Random;

/**
 * Created by qadrim on 15-02-25.
 */
public class YAMLConfig {

    private GPloadConfigObj params;

    private static String KEY_GPLOAD_COMMAND = "";  // GPLOAD Command to execute in shell

    private static String START_PORT_RANGE = "49152";
    private static String END_PORT_RANGE = "65535";
    private static String YAML_DIR = "/Users/madnan01/Documents/confluent-7.4.1/gpload/yml/";
    private static String LOGS_DIR = "/Users/madnan01/Documents/confluent-7.4.1/gpload/logs/";

    private static String TARGET_TABLE = "";
    private static String ERROR_TABLE = "";
    private static String LOCAL_HOST_IP = "";
    private static String LOCAL_HOST_NAME = "";
    private static String EXTERNAL_TABLE = "";
    private static String RANDOM_VAL = "";
    private static String NEW_LINE = "";
    private static String ERR_TBL_PREFIX = "err_";
    private static String EXT_TBL_PREFIX = "ext_";
    private static String LOAD_ACTION = "INSERT";
    private static String LOG_FILE = "";
    private static String CONTROL_FILE = "";

    public static String getKeyGploadCommand() {
        return KEY_GPLOAD_COMMAND;
    }

    public YAMLConfig(GPloadConfigObj gpconfig){

        this.params = gpconfig;
        this.NEW_LINE = System.getProperty("line.separator");

        try {
            this.LOCAL_HOST_IP = Inet4Address.getLocalHost().getHostAddress();
            this.LOCAL_HOST_NAME = Inet4Address.getLocalHost().getHostName();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        this.RANDOM_VAL = this.getRandomValue();

        this.TARGET_TABLE = gpconfig.getTable();
        this.ERROR_TABLE = "public." +  ERR_TBL_PREFIX + gpconfig.getTableOnly();
        this.EXTERNAL_TABLE = EXT_TBL_PREFIX + TARGET_TABLE + RANDOM_VAL;

        this.CONTROL_FILE = YAML_DIR + TARGET_TABLE + ".yml";

        this.LOG_FILE = LOGS_DIR+"/" + getTodaysDate() + "_" + TARGET_TABLE + ".log";
        File file = new File(this.LOG_FILE);
        file.getParentFile().mkdirs();

        this.KEY_GPLOAD_COMMAND = "gpload -l " + this.LOG_FILE + " -f " + this.CONTROL_FILE ;

    }
    public String getTodaysDate(){
        String DATE_FORMAT = "yyyy-mm-dd";
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        return sdf.format(cal.getTime());
    }

    // Create YAML File using parameters and get column names and types from database
    public void create()
    {
        StringBuffer contents = new StringBuffer(1500);

        //
        contents.append("VERSION: 1.0.0.1").append(NEW_LINE);
        contents.append("DATABASE: ").append(params.getDatabase()).append(NEW_LINE);
        contents.append("USER: ").append(params.getUsername()).append(NEW_LINE);
        contents.append("HOST: ").append(params.getHost()).append(NEW_LINE);
        contents.append("PORT: ").append(params.getPort()).append(NEW_LINE);
        contents.append("GPLOAD:").append(NEW_LINE);
        contents.append("   INPUT:").append(NEW_LINE);

        contents.append("    - SOURCE: ").append(NEW_LINE);

        // TODO: Stream to a temporary file and then bulk load OR optionally stream to a named pipe

        // Specify the Local Host IP or HostName
        contents.append("        LOCAL_HOSTNAME: ").append(NEW_LINE);
        contents.append("           - ").append(LOCAL_HOST_NAME).append(NEW_LINE);
        //contents.append("           - ").append(LOCAL_HOST_IP).append(NEW_LINE);

        // allow /PORT/PORT_RANGE to be specified
        contents.append("        PORT_RANGE: ").append('[').append(START_PORT_RANGE).append(',').append(END_PORT_RANGE).append(']').append(NEW_LINE);

        contents.append("        FILE: ").append('[').append(params.getInput()).append(']').append(NEW_LINE);


        // COLUMNS is optional, takes the existing fields in the table
        // contents.append("    - COLUMNS:").append(NEW_LINE);

        contents.append("    - FORMAT: ").append(params.getFormat().toUpperCase()).append(NEW_LINE);
        contents.append("    - DELIMITER: '").append(params.getDelimiter()).append("'").append(NEW_LINE);
        contents.append("    - NULL_AS: '").append(params.getNull_as()).append("'").append(NEW_LINE);

        if (params.getFormat().contains("TEXT") ) {
            contents.append("    - ESCAPE: '").append(params.getEscape()).append("'").append(NEW_LINE);
        }
        contents.append("    - QUOTE: '").append(params.getQuote()).append("'").append(NEW_LINE);
        contents.append("    - HEADER: ").append(Boolean.toString(params.isHeader()).toUpperCase()).append(NEW_LINE);
        contents.append("    - ENCODING: '").append(params.getEncoding()).append("'").append(NEW_LINE);
        contents.append("    - ERROR_LIMIT: ").append(params.getError_limit()).append(NEW_LINE);

        contents.append("    - ERROR_TABLE: ").append(ERROR_TABLE).append(NEW_LINE);

        contents.append("   OUTPUT:").append(NEW_LINE);

        String tableName = params.getSchema() + "." + params.getTable();

        contents.append("    - TABLE: ").append(TARGET_TABLE).append(NEW_LINE);
        contents.append("    - MODE: ").append(LOAD_ACTION).append(NEW_LINE);

        contents.append("   PRELOAD:").append(NEW_LINE);
        contents.append("    - TRUNCATE: ").append(Boolean.toString(params.isTruncate()).toUpperCase()).append(NEW_LINE);
        contents.append("    - REUSE_TABLES: ").append(Boolean.toString(params.isReuse()).toUpperCase()).append(NEW_LINE);

        // TODO: add support for MATCH_COLUMNS, UPDATE_COLUMN, UPDATE_CONDITION, MAPPING
        // TODO: add suport for BEFORE and AFTER SQL

        // CREATE YAML FILE
        try {
            clean();
            File file = new File(this.CONTROL_FILE);
            file.getParentFile().mkdirs();
            BufferedWriter bwr = new BufferedWriter(new FileWriter(file));
            bwr.write(contents.toString());
            bwr.flush();
            bwr.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        System.out.println("Control File: " + CONTROL_FILE + " is created");
    }

    public void clean(){
        // Clean up, delete YAML file
        try {
            boolean success = (new File(CONTROL_FILE)).delete();
            if (success) {
                System.out.println("Control File: " + CONTROL_FILE + " is cleaned");
            }
        }
        catch(Exception e)
        {
            System.out.println("Control File:" + CONTROL_FILE + " does not exist");
        }

    }

    private String getRandomValue(){
        Random r = new Random();
        int Low  = 10000;
        int High = 99999;
        Integer R = r.nextInt(High-Low) + Low;
        return (R.toString());
    }
}
