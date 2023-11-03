package io.confluent.connect.jdbc.gpload;

/**
 * Created by Muji Qadri on 15-02-25.
 */
public class GPloadConfigObj {

    // <editor-fold desc="GPLOAD CONFIG VARIABLES">
    private String input = "";
    private String database = "";
    private String table = "";
    private String url = "";     // URL argument will be in the format hostname:port/database e.g 192.168.1.1:5432/mydatabase

    private String schema = "";  // Schema is part of the Table argument, split with '.' and populates schema
    private String host = "";
    private String port = "";
    private String username = "";
    private String delimiter = "";
    private String args = "";

    private boolean truncate = false;   // Truncate destination table prior to load
    private boolean reuse = false;       // Reuse previously created external table for multiple files
    private String format = "TEXT";     // TEXT or CSV input file format
    private String escape = "OFF";       // Escape any characters from input, default is OFF
    private String null_as = "\\N";     // Specifies the null value is input, default is \N in TEXT, empty value in CSV
    private String quote = "\"";        // Used when format is CSV, default is double quote
    private boolean header = false;     // Set to true if the input file(s) have a header line
    private String encoding = "UTF8";   // Database encoding to use for input file, uses default client_encoding
    private int error_limit = 1000;     // Error limit for gpload for errors found in input file, sends line to ERROR_TABLE


    // </editor-fold>

    // <editor-fold desc="KEYS REQUIRED ARGUMENTS">
    private final String KEY_INPUT = "input";
    private final String KEY_DATABASE = "database";
    private final String KEY_TABLE = "table";
    private final String KEY_HOST = "host";
    private final String KEY_PORT = "port";
    private final String KEY_USERNAME = "user";
    private final String KEY_DELIMITER = "delimiter";
    private final String KEY_URL = "url";
    // </editor-fold>

    // <editor-fold desc="KEYS OPTIONAL ARGUMENTS">
    private final String KEY_TRUNCATE = "truncate";
    private final String KEY_REUSE = "reuse";
    private final String KEY_FORMAT = "format";
    private final String KEY_ESCAPE = "escape";
    private final String KEY_NULL_AS = "null_as";
    private final String KEY_QUOTE = "quote";
    private final String KEY_HEADER = "header";
    private final String KEY_ENCODING = "encoding";
    private final String KEY_ERROR_LIMIT = "error_limit";
    // </editor-fold>




    public void loadParameter(String key, String value){

        try {

            if (key.contains(KEY_INPUT)) {
                setInput(value);
            }
            if (key.contains(KEY_DATABASE)) {
                setDatabase(value);
            }
            if (key.contains(KEY_TABLE)) {
                setTable(value);
            }
            if (key.contains(KEY_HOST)) {
                setHost(value);
            }
            if (key.contains(KEY_PORT)) {
                setPort(value);
            }
            if (key.contains(KEY_USERNAME)) {
                setUsername(value);
            }
            if (key.contains(KEY_DELIMITER)) {
                setDelimiter(value);
            }
            if (key.contains(KEY_URL)) {
                setUrl(value);
            }

            // OPTIONAL PARAMETERS

            if (key.contains(KEY_TRUNCATE)) {
                setTruncate(Boolean.parseBoolean(value));
            }
            if (key.contains(KEY_REUSE)) {
                setReuse(Boolean.parseBoolean(value));
            }
            if (key.contains(KEY_FORMAT)) {
                setFormat(value);
            }
            if (key.contains(KEY_ESCAPE)) {
                setEscape(value);
            }
            if (key.contains(KEY_NULL_AS)) {
                setNull_as(value);
            }
            if (key.contains(KEY_QUOTE)) {
                setQuote(value);
            }
            if (key.contains(KEY_HEADER)) {
                setHeader(Boolean.parseBoolean(value));
            }
            if (key.contains(KEY_ENCODING)) {
                setEncoding(value);
            }
            if (key.contains(KEY_ERROR_LIMIT)) {
                setError_limit(Integer.parseInt(value));
            }

        }
        catch(Exception e) {
            Exception argsException = new Exception("Exception in arguments - check usage below\n" +
                    " * Class for running GPload to bulkload data into HAWQ and Greenplum\n" +
                    " * =================================================================================\n" +
                    " * args = --input=resources/inputfile.csv --table=demo300.SXDTable --url=192.168.1.46:8080/pivotal --user=gpadmin --delimiter=,\n" +
                    " * optional agruments    " +
                    " *                  --truncate {true|false} (truncates destination table before loading)\n" +
                    " *                  --reuse {true|false} (reuse existing external tables if they exists, minimized catalog use)\n" +
                    " *                  --format {text|csv} (defaults to text if not specified)\n" +
                    " *                  --escape {char} (specifies a single ASCII character such as \\n, \\t, \\100) for escapaing data chars\n" +
                    " *                          which might otherwise be taken as row or column delimiters, use char which is not used anywhere\n" +
                    " *                          in your actual column data. Default escape is a \\ (backslash) for text-formatted files and\n" +
                    " *                          \" (double quotes) for csv.)\n" +
                    " *                  --null_as {string} (specifies the string that represents a null value. default is \\N in text mode,\n" +
                    " *                          and an empty value with no quotations in csv mode\n" +
                    " *                  --quote {char} default is double-quote(\")\n" +
                    " *                  --header {true|false} (Specifies that the first line in the data file(s) is a header row and should\n" +
                    " *                          not be included in the data, default is false\n" +
                    " *                  --encoding {database_encoding} (Character encoding of the source data, such as 'SQL_ASCII', an integer\n" +
                    " *                          encoding number of 'UTF8' to use the default client encoding.\n" +
                    " *                  --error_limit {int} (Input rows that have format errors will be discarded provided that the error\n" +
                    " *                          limit count is not reached on a segment, default is set to 1000 rows");
            System.out.println(argsException.toString());
        }

    }

    public String validateParams(){
        String errMsg = "";

        if (input.length()==0) { errMsg += "Param: input not specified, either specify a single file or a directory with wildcard\n"; }
        if (database.length()==0) { errMsg += "Param: database not specified\n"; }
        if (table.length()==0) { errMsg += "Param: destination schema.table not specified\n"; }
        if (host.length()==0) { errMsg += "Param: destination host not specified\n"; }
        if (port.length()==0) { errMsg += "Param: destination port not specified\n"; }
        if (username.length()==0) { errMsg += "Param: username not specified\n"; }
        if (delimiter.length()==0) { errMsg += "Param: delimiter not specified\n"; }

        return errMsg;
    }

    public String getSchema() {
        return schema;
    }
    public void setUrl(String url) {
        try {
            String[] host_arr = url.split(":");
            this.host = host_arr[0];
            String[] port_arr = host_arr[1].split("/");
            this.port = port_arr[0];
            this.database = port_arr[1];
            this.url = url;
        }catch(Exception e){
            System.out.println("Unable to parse argument 'url");
        }
    }
    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String toString(){
        return (args);
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getUsername() {

        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPort() {

        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getHost() {

        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }


    public void setTable(String table) {
        try {
            String[] s_arr = table.split("\\.");
            this.schema = s_arr[0];
            this.table = s_arr[1];
        }
        catch(Exception e)
        {
            System.out.println("Params: Argument format 'schema.tablename'");
        }
    }
    public String getTable() {
        return (this.schema + "." + this.table );
    }

    public String getTableOnly(){
        return this.table;
    }

    public String getInput() {

        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }
    public void setArgs(String args) {
        this.args = args;
    }

    public String getArgs() {
        return args;
    }


    public String getDatabase() {

        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUrl() {
        return url;
    }

    public int getError_limit() {

        return error_limit;
    }

    public void setError_limit(int error_limit) {
        this.error_limit = error_limit;
    }

    public String getEncoding() {

        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public boolean isHeader() {

        return header;
    }

    public void setHeader(boolean header) {
        this.header = header;
    }

    public String getQuote() {

        return quote;
    }

    public void setQuote(String quote) {
        this.quote = quote;
    }

    public String getNull_as() {

        return null_as;
    }

    public void setNull_as(String null_as) {
        this.null_as = null_as;
    }

    public String getEscape() {

        return escape;
    }

    public void setEscape(String escape) {
        this.escape = escape;
    }

    public String getFormat() {

        return format;
    }

    public void setFormat(String format) {
        this.format = format;
        if (format =="csv" )  // Set default double quote
        {
            this.setQuote("\"");
            //this.setEscape(""); // Cannot set escape for CSV Files
        }
    }

    public boolean isReuse() {

        return reuse;
    }

    public void setReuse(boolean reuse) {
        this.reuse = reuse;
    }

    public boolean isTruncate() {

        return truncate;
    }

    public void setTruncate(boolean truncate) {
        this.truncate = truncate;
    }

}
