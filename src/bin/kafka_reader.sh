#!/usr/bin/env bash

java -cp ../../target/letsdata-writeconnector-reader-1.0-SNAPSHOT-jar-with-dependencies.jar com.letsdata.reader.KafkaMain $@
