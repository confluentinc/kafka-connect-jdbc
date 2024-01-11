package io.confluent.connect.jdbc.gp.gpload.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class GPloadConfig {
    private String VERSION;
    private String DATABASE;
    private String USER;
    private String HOST;
    private int PORT;
    private GPload GPLOAD;
    private String PASSWORD;

    public static class Builder {

        private GPloadConfig gPloadConfig;

        public Builder() {
            gPloadConfig = new GPloadConfig();
        }
        public Builder version(String VERSION) {
            gPloadConfig.VERSION = VERSION;
            return this;
        }

        public Builder database(String DATABASE) {
            gPloadConfig.DATABASE = DATABASE;
            return this;
        }
        public Builder user(String USER) {
            gPloadConfig.USER = USER;
            return this;
        }
        public Builder host(String HOST) {
            gPloadConfig.HOST = HOST;
            return this;
        }
        public Builder port(int PORT) {
            gPloadConfig.PORT = PORT;
            return this;
        }
        public Builder gpload(GPload GPLOAD) {
            gPloadConfig.GPLOAD = GPLOAD;
            return this;
        }
        public Builder password(String PASSWORD) {
            gPloadConfig.PASSWORD = PASSWORD;
            return this;
        }

        public GPloadConfig build() {
            return gPloadConfig;
        }
    }


    public String getVERSION() {
        return VERSION;
    }

    public void setVERSION(String VERSION) {
        this.VERSION = VERSION;
    }

    public String getDATABASE() {
        return DATABASE;
    }

    public void setDATABASE(String DATABASE) {
        this.DATABASE = DATABASE;
    }

    public String getUSER() {
        return USER;
    }

    public void setUSER(String USER) {
        this.USER = USER;
    }

    public String getHOST() {
        return HOST;
    }

    public void setHOST(String HOST) {
        this.HOST = HOST;
    }

    public int getPORT() {
        return PORT;
    }

    public void setPORT(int PORT) {
        this.PORT = PORT;
    }

    public GPload getGPLOAD() {
        return GPLOAD;
    }

    public void setGPLOAD(GPload GPLOAD) {
        this.GPLOAD = GPLOAD;
    }

    public String getPASSWORD() {
        return PASSWORD;
    }

    public void setPASSWORD(String PASSWORD) {
        this.PASSWORD = PASSWORD;
    }


    public static class GPload {
        private List<Input> INPUT;
        private List<External> EXTERNAL;
        private List<Output> OUTPUT;
        private  List<Preload> PRELOAD;
        private SQL SQL;

        public  static class Builder{
            private GPload gPload;

            public Builder() {
                gPload = new GPload();
            }

            public Builder input(List<Input> INPUT) {
                gPload.INPUT = INPUT;
                return this;
            }

            public Builder external(List<External> EXTERNAL) {
                gPload.EXTERNAL = EXTERNAL;
                return this;
            }

            public Builder output(List<Output> OUTPUT) {
                gPload.OUTPUT = OUTPUT;
                return this;
            }

            public Builder preload(List<Preload> PRELOAD) {
                gPload.PRELOAD = PRELOAD;
                return this;
            }

            public Builder sql(SQL SQL) {
                gPload.SQL = SQL;
                return this;
            }

            public GPload build() {
                return gPload;
            }
        }

        public List<Input> getINPUT() {
            return INPUT;
        }

        public void setINPUT(List<Input> INPUT) {
            this.INPUT = INPUT;
        }

        public List<External> getEXTERNAL() {
            return EXTERNAL;
        }

        public void setEXTERNAL(List<External> EXTERNAL) {
            this.EXTERNAL = EXTERNAL;
        }

        public List<Output> getOUTPUT() {
            return OUTPUT;
        }

        public void setOUTPUT(List<Output> OUTPUT) {
            this.OUTPUT = OUTPUT;
        }

        public List<Preload> getPRELOAD() {
            return PRELOAD;
        }

        public void setPRELOAD(List<Preload> PRELOAD) {
            this.PRELOAD = PRELOAD;
        }

        public SQL getSQL() {
            return SQL;
        }

        public void setSQL(SQL SQL) {
            this.SQL = SQL;
        }
    }

    public static class Input {
        private Source SOURCE;
        private boolean FULLY_QUALIFIED_DOMAIN_NAME;
        private List<Map<String, String>> COLUMNS;
        private String TRANSFORM;
        private String TRANSFORM_CONFIG;
        private long MAX_LINE_LENGTH;
        private String FORMAT;
        private String DELIMITER;
        private String ESCAPE;
        private String NULL_AS;
        private List<Boolean> FORCE_NOT_NULL;
        private String QUOTE;
        private boolean HEADER;
        private String ENCODING;
        private int ERROR_LIMIT;
        private boolean LOG_ERRORS;

        public static class Builder {
            private Input input;

            public Builder() {
                input = new Input();
            }

            public Builder source(Source source) {
                input.SOURCE = source;
                return this;
            }

            public Builder fullyQualifiedDomainName(boolean fullyQualifiedDomainName) {
                input.FULLY_QUALIFIED_DOMAIN_NAME = fullyQualifiedDomainName;
                return this;
            }

            public Builder columns(List<Map<String, String>> columns) {
                input.COLUMNS = columns;
                return this;
            }

            public Builder transform(String transform) {
                input.TRANSFORM = transform;
                return this;
            }

            public Builder transformConfig(String transformConfig) {
                input.TRANSFORM_CONFIG = transformConfig;
                return this;
            }

            /**
             * -m max_length
             * Sets the maximum allowed data row length in bytes. Default is 32768. Should be used when user data includes very wide rows (or when line too long error message occurs). Should not be used otherwise as it increases resource allocation. Valid range is 32K to 256MB. (The upper limit is 1MB on Windows systems.)
             * @param maxLineLength
             * @return
             */
            public Builder maxLineLength(long maxLineLength) {
                input.MAX_LINE_LENGTH = maxLineLength;
                return this;
            }

            public Builder format(String format) {
                input.FORMAT = format;
                return this;
            }

            public Builder delimiter(String delimiter) {
                input.DELIMITER = delimiter;
                return this;
            }

            public Builder escape(String escape) {
                input.ESCAPE = escape;
                return this;
            }

            public Builder nullAs(String nullAs) {
                input.NULL_AS = nullAs;
                return this;
            }

            public Builder forceNotNull(List<Boolean> forceNotNull) {
                input.FORCE_NOT_NULL = forceNotNull;
                return this;
            }

            public Builder quote(String quote) {
                input.QUOTE = quote;
                return this;
            }

            public Builder header(boolean header) {
                input.HEADER = header;
                return this;
            }

            public Builder encoding(String encoding) {
                input.ENCODING = encoding;
                return this;
            }

            public Builder errorLimit(int errorLimit) {
                input.ERROR_LIMIT = errorLimit;
                return this;
            }

            public Builder logErrors(boolean logErrors) {
                input.LOG_ERRORS = logErrors;
                return this;
            }

            public Input build() {
                return input;
            }
        }

        public Source getSOURCE() {
            return SOURCE;
        }

        public void setSOURCE(Source SOURCE) {
            this.SOURCE = SOURCE;
        }

        public boolean isFULLY_QUALIFIED_DOMAIN_NAME() {
            return FULLY_QUALIFIED_DOMAIN_NAME;
        }

        public void setFULLY_QUALIFIED_DOMAIN_NAME(boolean FULLY_QUALIFIED_DOMAIN_NAME) {
            this.FULLY_QUALIFIED_DOMAIN_NAME = FULLY_QUALIFIED_DOMAIN_NAME;
        }

        public List<Map<String, String>> getCOLUMNS() {
            return COLUMNS;
        }

        public void setCOLUMNS(List<Map<String, String>> COLUMNS) {
            this.COLUMNS = COLUMNS;
        }

        public String getTRANSFORM() {
            return TRANSFORM;
        }

        public void setTRANSFORM(String TRANSFORM) {
            this.TRANSFORM = TRANSFORM;
        }

        public String getTRANSFORM_CONFIG() {
            return TRANSFORM_CONFIG;
        }

        public void setTRANSFORM_CONFIG(String TRANSFORM_CONFIG) {
            this.TRANSFORM_CONFIG = TRANSFORM_CONFIG;
        }

        public long getMAX_LINE_LENGTH() {
            return MAX_LINE_LENGTH;
        }

        public void setMAX_LINE_LENGTH(int MAX_LINE_LENGTH) {
            this.MAX_LINE_LENGTH = MAX_LINE_LENGTH;
        }

        public String getFORMAT() {
            return FORMAT;
        }

        public void setFORMAT(String FORMAT) {
            this.FORMAT = FORMAT;
        }

        public String getDELIMITER() {
            return DELIMITER;
        }

        public void setDELIMITER(String DELIMITER) {
            this.DELIMITER = DELIMITER;
        }

        public String getESCAPE() {
            return ESCAPE;
        }

        public void setESCAPE(String ESCAPE) {
            this.ESCAPE = ESCAPE;
        }

        public String getNULL_AS() {
            return NULL_AS;
        }

        public void setNULL_AS(String NULL_AS) {
            this.NULL_AS = NULL_AS;
        }

        public List<Boolean> isFORCE_NOT_NULL() {
            return FORCE_NOT_NULL;
        }

        public void setFORCE_NOT_NULL(List<Boolean> FORCE_NOT_NULL) {
            this.FORCE_NOT_NULL = FORCE_NOT_NULL;
        }

        public String getQUOTE() {
            return QUOTE;
        }

        public void setQUOTE(String QUOTE) {
            this.QUOTE = QUOTE;
        }

        public boolean isHEADER() {
            return HEADER;
        }

        public void setHEADER(boolean HEADER) {
            this.HEADER = HEADER;
        }

        public String getENCODING() {
            return ENCODING;
        }

        public void setENCODING(String ENCODING) {
            this.ENCODING = ENCODING;
        }

        public int getERROR_LIMIT() {
            return ERROR_LIMIT;
        }

        public void setERROR_LIMIT(int ERROR_LIMIT) {
            this.ERROR_LIMIT = ERROR_LIMIT;
        }

        public boolean isLOG_ERRORS() {
            return LOG_ERRORS;
        }

        public void setLOG_ERRORS(boolean LOG_ERRORS) {
            this.LOG_ERRORS = LOG_ERRORS;
        }
    }

    public static class Source {
        private List<String> LOCAL_HOSTNAME;
        private int PORT;
        private List<Integer> PORT_RANGE;
        private List<String> FILE;
        private boolean SSL;
        private String CERTIFICATES_PATH;
        public static class Builder {
            private Source source;

            public Builder() {
                source = new Source();
            }

            public Builder localHostname(List<String> localHostname) {
                source.LOCAL_HOSTNAME = localHostname;
                return this;
            }

            public Builder port(int port) {
                source.PORT = port;
                return this;
            }

            public Builder portRange(List<Integer> portRange) {
                source.PORT_RANGE = portRange;
                return this;
            }

            public Builder file(List<String> file) {
                source.FILE = file;
                return this;
            }

            public Builder ssl(boolean ssl) {
                source.SSL = ssl;
                return this;
            }

            public Builder certificatesPath(String certificatesPath) {
                source.CERTIFICATES_PATH = certificatesPath;
                return this;
            }

            public Source build() {
                return source;
            }
        }
        public List<String>  getLOCAL_HOSTNAME() {
            return LOCAL_HOSTNAME;
        }

        public void setLOCAL_HOSTNAME(List<String>  LOCAL_HOSTNAME) {
            this.LOCAL_HOSTNAME = LOCAL_HOSTNAME;
        }

        public int getPORT() {
            return PORT;
        }

        public void setPORT(int PORT) {
            this.PORT = PORT;
        }

        public List<Integer> getPORT_RANGE() {
            return PORT_RANGE;
        }

        public void setPORT_RANGE(List<Integer> PORT_RANGE) {
            this.PORT_RANGE = PORT_RANGE;
        }

        public List<String> getFILE() {
            return FILE;
        }

        public void setFILE(List<String> FILE) {
            this.FILE = FILE;
        }

        public boolean isSSL() {
            return SSL;
        }

        public void setSSL(boolean SSL) {
            this.SSL = SSL;
        }

        public String getCERTIFICATES_PATH() {
            return CERTIFICATES_PATH;
        }

        public void setCERTIFICATES_PATH(String CERTIFICATES_PATH) {
            this.CERTIFICATES_PATH = CERTIFICATES_PATH;
        }
    }

    public static class External {
        private String SCHEMA;
        public static class Builder {
            private External external;

            public Builder() {
                external = new External();
            }

            public Builder schema(String schema) {
                external.SCHEMA = schema;
                return this;
            }

            public External build() {
                return external;
            }
        }
        public String getSCHEMA() {
            return SCHEMA;
        }

        public void schema(String SCHEMA) {
            this.SCHEMA = SCHEMA;
        }
    }

    public static class Output {
        private String TABLE;
        private String MODE;
        private List<String> MATCH_COLUMNS;
        private List<String> UPDATE_COLUMNS;
        private String UPDATE_CONDITION;
        private Map<String, String> MAPPING;

        public static class Builder {
            private Output output;

            public Builder() {
                output = new Output();
            }

            public Builder table(String table) {
                output.TABLE = table;
                return this;
            }

            public Builder mode(String mode) {
                output.MODE = mode;
                return this;
            }

            public Builder matchColumns(List<String> matchColumns) {
                output.MATCH_COLUMNS = matchColumns;
                return this;
            }

            public Builder updateColumns(List<String> updateColumns) {
                output.UPDATE_COLUMNS = updateColumns;
                return this;
            }

            public Builder updateCondition(String updateCondition) {
                output.UPDATE_CONDITION = updateCondition;
                return this;
            }

            public Builder mapping(Map<String, String> mapping) {
                output.MAPPING = mapping;
                return this;
            }

            public Output build() {
                return output;
            }
        }

        public String getTABLE() {
            return TABLE;
        }

        public void setTABLE(String TABLE) {
            this.TABLE = TABLE;
        }

        public String getMODE() {
            return MODE;
        }

        public void setMODE(String MODE) {
            this.MODE = MODE;
        }

        public List<String> getMATCH_COLUMNS() {
            return MATCH_COLUMNS;
        }

        public void setMATCH_COLUMNS(List<String> MATCH_COLUMNS) {
            this.MATCH_COLUMNS = MATCH_COLUMNS;
        }

        public List<String> getUPDATE_COLUMNS() {
            return UPDATE_COLUMNS;
        }

        public void setUPDATE_COLUMNS(List<String> UPDATE_COLUMNS) {
            this.UPDATE_COLUMNS = UPDATE_COLUMNS;
        }

        public String getUPDATE_CONDITION() {
            return UPDATE_CONDITION;
        }

        public void setUPDATE_CONDITION(String UPDATE_CONDITION) {
            this.UPDATE_CONDITION = UPDATE_CONDITION;
        }

        public Map<String, String> getMAPPING() {
            return MAPPING;
        }

        public void setMAPPING(Map<String, String> MAPPING) {
            this.MAPPING = MAPPING;
        }
    }

    public static class Preload {
        private boolean TRUNCATE;
        private boolean REUSE_TABLES;
        private String STAGING_TABLE;
        private boolean FAST_MATCH;

        public static class Builder {
            private Preload preload;

            public Builder() {
                preload = new Preload();
            }

            public Builder truncate(boolean truncate) {
                preload.TRUNCATE = truncate;
                return this;
            }

            public Builder reuseTables(boolean reuseTables) {
                preload.REUSE_TABLES = reuseTables;
                return this;
            }

            public Builder stagingTable(String stagingTable) {
                preload.STAGING_TABLE = stagingTable;
                return this;
            }

            public Builder fastMatch(boolean fastMatch) {
                preload.FAST_MATCH = fastMatch;
                return this;
            }

            public Preload build() {
                return preload;
            }
        }
        public boolean isTRUNCATE() {
            return TRUNCATE;
        }

        public void setTRUNCATE(boolean TRUNCATE) {
            this.TRUNCATE = TRUNCATE;
        }

        public boolean isREUSE_TABLES() {
            return REUSE_TABLES;
        }

        public void setREUSE_TABLES(boolean REUSE_TABLES) {
            this.REUSE_TABLES = REUSE_TABLES;
        }

        public String getSTAGING_TABLE() {
            return STAGING_TABLE;
        }

        public void setSTAGING_TABLE(String STAGING_TABLE) {
            this.STAGING_TABLE = STAGING_TABLE;
        }

        public boolean isFAST_MATCH() {
            return FAST_MATCH;
        }

        public void setFAST_MATCH(boolean FAST_MATCH) {
            this.FAST_MATCH = FAST_MATCH;
        }
    }

    public static class SQL {
        private String BEFORE;
        private String AFTER;

        public static class Builder {
            private SQL sql;

            public Builder() {
                sql = new SQL();
            }

            public Builder before(String before) {
                sql.BEFORE = before;
                return this;
            }

            public Builder after(String after) {
                sql.AFTER = after;
                return this;
            }

            public SQL build() {
                return sql;
            }
        }
        public String getBEFORE() {
            return BEFORE;
        }

        public void setBEFORE(String BEFORE) {
            this.BEFORE = BEFORE;
        }

        public String getAFTER() {
            return AFTER;
        }

        public void setAFTER(String AFTER) {
            this.AFTER = AFTER;
        }
    }
}
