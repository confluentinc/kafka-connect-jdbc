package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Created by rahulv on 6/21/17.
 */
public class PostgresTypes {
    public static final String TEXT_ARRAY_LOGICAL_NAME = "TEXTARRAY";
    //public static final String SCALE_FIELD = "scale";

    public static final String INTEGER_ARRAY_LOGICAL_NAME = "INTARRAY";

    public static final String JSONB_LOGICAL_NAME = "JSONB";

    public static final String POINT_LOGICAL_NAME = "POINT";
    /**
     * Returns a SchemaBuilder for a Decimal with the given scale factor. By returning a SchemaBuilder you can override
     * additional schema settings such as required/optional, default value, and documentation.
     *
     * @return a SchemaBuilder
     */

    public static SchemaBuilder TextArrayBuilder() {
        return SchemaBuilder.string().name("TEXTARRAY").version(Integer.valueOf(1));
    }

    public static SchemaBuilder JsonbBuilder(){
        return SchemaBuilder.string().name("JSONB").version(Integer.valueOf(1));
    }

    public static SchemaBuilder IntArrayBuilder(){
        return SchemaBuilder.string().name("INTARRAY").version(Integer.valueOf(1));
    }

    public static SchemaBuilder CidrBuilder(){
        return SchemaBuilder.string().name("CIDR").version(Integer.valueOf(1));
    }

    public static SchemaBuilder PointBuilder(){
        return SchemaBuilder.string().name("POINT").version(Integer.valueOf(1));
    }



}
