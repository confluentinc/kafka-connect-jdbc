FROM confluentinc/cp-kafka-connect:5.1.0
MAINTAINER https://www.confluent.io/

RUN apt-get update
RUN apt-get install -y gettext-base
RUN rm /usr/share/java/kafka-connect-jdbc/kafka-connect-jdbc-5.1.0.jar

ADD target/kafka-connect-jdbc-5.1.0.jar /usr/share/java/kafka-connect-jdbc/kafka-connect-jdbc-5.1.0.jar