package io.confluent.connect.jdbc.sink.metadata;

public class ColumnDetails {
    private String columnName;
    private String columnType;
    private String columnDefault;

    public ColumnDetails(String columnName, String columnType, String columnDefault) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.columnDefault = columnDefault;
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

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public String getColumnDefault() {
        return columnDefault;
    }

    public String getColumnType() {
        return columnType;
    }

}
