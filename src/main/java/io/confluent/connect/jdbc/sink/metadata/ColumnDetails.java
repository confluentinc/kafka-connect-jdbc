package io.confluent.connect.jdbc.sink.metadata;


public class ColumnDetails {
    private final String udtName;
    private String columnName;
    private String dataType;
    private String columnDefault;

    public ColumnDetails(String columnName, String dataType, String udtName, String columnDefault) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.columnDefault = columnDefault;
        this.udtName = udtName;
    }
    // getter/setter

    public String getColumnName() {
        return columnName;
    }

    public void setColumnDefault(String columnDefault) {
        this.columnDefault = columnDefault;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getColumnDefault() {
        return columnDefault;
    }

    public String getDataType() {
        return dataType;
    }

    public String getUdtName() {
        return udtName;
    }
    public DateType getDateType() {
        return DateType.fromString(dataType);
    }

}


