FROM maven:3.6.0-jdk-8-slim

RUN apt-get update && apt-get install -y git

ARG GRADLE_VERSION=5.1.1
ARG GRADLE_FILE_NAME=gradle-${GRADLE_VERSION}
ARG GRADLE_PACKAGE=${GRADLE_FILE_NAME}-bin.zip
ARG GRADLE_HOME=/opt/gradle

WORKDIR /dependencies

RUN curl --fail -LO https://services.gradle.org/distributions/${GRADLE_PACKAGE} && \
    mkdir ${GRADLE_HOME} && unzip -d ${GRADLE_HOME} ${GRADLE_PACKAGE} && rm ${GRADLE_PACKAGE}

ENV PATH=$PATH:$GRADLE_HOME/${GRADLE_FILE_NAME}/bin

ARG CONLFUENT_KAFKA_VERSION=v5.1.0
ARG CONLFUENT_COMMON_VERSION=v5.1.0

RUN git clone https://github.com/confluentinc/kafka.git --branch ${CONLFUENT_KAFKA_VERSION} --single-branch && \
    cd kafka && gradle && ./gradlew build -x test && cd ..

RUN git clone https://github.com/confluentinc/common.git --branch ${CONLFUENT_COMMON_VERSION} --single-branch && \
    cd common && mvn install -DskipTests && cd ..

WORKDIR /build

COPY . .

ENV JFROG_USERNAME=this-need-to-be-passed-as-env-when-running-the-image
ENV JFROG_PASSWORD=this-need-to-be-passed-as-env-when-running-the-image
