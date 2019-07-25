#!/bin/bash

# Nico: I was only able to debug the connector from Intellj with a remote debugger on the local container
# using my private IP.
# In addition I had to set my IP in KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:9092,PLAINTEXT_HOST://192.168.33.33:29092 on the local kafka broker
# Change this
YOUR_HOST_PRIVATE_IP=192.168.33.33

IMAGE_NAME=kafka-connect-jdbc

docker stop $IMAGE_NAME

export CLASSPATH="$(find target -type f -name '*.jar'| grep '\-development' | tr '\n' ':')"

if hash docker 2>/dev/null; then

    docker build . -t baydynamics/kafka-connect-jdbc:1.0-5.4.0-SNAPSHOT

    # JAVA_TOOL_OPTIONS is required to be able to run Intellij remote debugger
    # suspend=y blocks the connector execution on startup until it receives intellij debugger request
    docker run \
            --env CLASSPATH=$CLASSPATH \
            --env "JAVA_TOOL_OPTIONS=\"-agentlib:jdwp=transport=dt_socket,address=0.0.0.0:5005,server=y,suspend=y\"" \
            --name $IMAGE_NAME \
            --add-host dockerhost:$YOUR_HOST_PRIVATE_IP \
            -p 5005:5005 \
            --rm \
            --volume offsets:/kafka-connect-jdbc/offsets \
            -t baydynamics/kafka-connect-jdbc:1.0-5.4.0-SNAPSHOT
else
    printf "Couldn't find a suitable way to run kafka connect for you"
fi;