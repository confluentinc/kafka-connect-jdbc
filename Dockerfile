FROM confluentinc/cp-kafka-connect:5.1.0
MAINTAINER Data Pipeline <data.pipeline@tradeshift.com>

RUN rm /usr/share/java/kafka-connect-jdbc/kafka-connect-jdbc-5.1.0.jar
ADD  target/kafka-connect-jdbc-5.1.0.jar /usr/share/java/kafka-connect-jdbc/kafka-connect-jdbc-5.1.0.jar