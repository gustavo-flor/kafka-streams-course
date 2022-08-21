#!/bin/bash

kafka-topics --bootstrap-server localhost:9092 --create --topic word-count-input --partitions 3

kafka-topics --bootstrap-server localhost:9092 --create --topic word-count-output --partitions 3
