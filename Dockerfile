FROM confluentinc/cp-kafka-connect:5.5.1

RUN echo "===> Installing MySQL connector" \
  && curl https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar  --output /usr/share/java/kafka-connect-jdbc/mysql-connector-java-8.0.19.jar

RUN echo "===> Installing Oracle connector" \
  && wget https://repo1.maven.org/maven2/com/oracle/ojdbc/ojdbc8/19.3.0.0/ojdbc8-19.3.0.0.jar -O /usr/share/java/kafka-connect-jdbc/ojdbc8.jar

RUN echo "===> Installing SQL Server connector" \
  && wget https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/6.4.0.jre8/mssql-jdbc-6.4.0.jre8.jar -O /usr/share/java/kafka-connect-jdbc/mssql-jdbc-6.4.0.jre8.jar

RUN echo "===> Collecting Jsch" \
  && wget -O /usr/share/java/kafka-connect-jdbc/jsch-0.1.51.jar https://repo1.maven.org/maven2/com/jcraft/jsch/0.1.55/jsch-0.1.55.jar

RUN echo "===> Collecting SNS" \
  && wget -O /usr/share/java/kafka-connect-jdbc/aws-java-sdk-sns-1.11.725.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-sns/1.11.725/aws-java-sdk-sns-1.11.725.jar

RUN echo "===> Collecting S3" \
  && wget -O /usr/share/java/kafka-connect-jdbc/aws-java-sdk-s3-1.11.725.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.11.725/aws-java-sdk-s3-1.11.725.jar


RUN echo "===> Collecting AWS core" \
  && wget -O /usr/share/java/kafka/aws-java-sdk-core-1.11.725.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.11.725/aws-java-sdk-core-1.11.725.jar

RUN echo "===> Collecting AWS STS" \
  && wget -O /usr/share/java/kafka/aws-java-sdk-sts-1.11.725.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-sts/1.11.725/aws-java-sdk-sts-1.11.725.jar

RUN echo "===> Collecting Simple JSON" \
  && wget -O /usr/share/java/kafka-connect-jdbc/json-simple-1.1.1.jar https://repo1.maven.org/maven2/com/googlecode/json-simple/json-simple/1.1.1/json-simple-1.1.1.jar

RUN echo "===> Updating JDBC jar" \
  && rm -rf /usr/share/java/kafka-connect-jdbc/kafka-connect-jdbc-5.5.1.jar \
  && rm -rf /usr/share/java/kafka-connect-s3/kafka-connect-s3-5.5.1.jar

COPY ./target/kafka-connect-jdbc-5.5.1.jar /usr/share/java/kafka-connect-jdbc/

COPY ./kafka-connect-storage-cloud/kafka-connect-s3/target/kafka-connect-s3-5.5.1.jar /usr/share/java/kafka-connect-s3/
