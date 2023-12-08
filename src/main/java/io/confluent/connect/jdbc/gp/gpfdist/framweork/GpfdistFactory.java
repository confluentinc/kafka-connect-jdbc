package io.confluent.connect.jdbc.gp.gpfdist.framweork;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.util.CommonUtils;

public class GpfdistFactory {

    // TODO - whether to create a new GpfdistMessageHandler or not?
//    public static GpfdistMessageHandler createGpfdistMessageHandler(JdbcSinkConfig config) {
//        String gpfHost = config.gpfdistHost;
//        if(gpfHost == null) {
//            gpfHost = CommonUtils.getLocalIpOrHost();
//        }
//        return new GpfdistMessageHandler(config.getGpfdistPort(), config.gpfFlushCount,  config.gpfFlushTime,  config.gpfBatchTimeout, config.gpfBatchCount, config.gpfBatchPeriod, config.delimiter, gpfHost);
//    }

}
