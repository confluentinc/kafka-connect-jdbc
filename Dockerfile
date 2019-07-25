#cp-kafka-connect-base does not ship with any connector
FROM confluentinc/cp-kafka-connect-base:5.2.0
MAINTAINER Bay Dynamics
WORKDIR /kafka-connect-jdbc

COPY config config
COPY target target
COPY config/connect-log4j.properties /etc/kafka/connect-log4j.properties

VOLUME /kafka-connect-jdbc/config
VOLUME /kafka-connect-jdbc/offsets

CMD CLASSPATH="$(find target/ -type f -name '*.jar'| grep '\-development' | tr '\n' ':')" connect-standalone config/worker.properties config/sink-riskfabric-db.properties