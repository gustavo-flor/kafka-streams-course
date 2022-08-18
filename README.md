# Kafka Streams Course

## Word Count

Up Kafka environment
```shell
docker-compose up -d
```

Access your kafka broker
```shell
docker ps # Find the kafka container ID

docker exec -it <Kafka Container ID> bash
```

Create input topic
```shell
kafka-topics --bootstrap-server localhost:9092 --create --topic word-count-input --partitions 3
```

Create output topic
```shell
kafka-topics --bootstrap-server localhost:9092 --create --topic word-count-output --partitions 3
```

Produce data to be streamed
```shell
kafka-console-producer --bootstrap-server localhost:9092 --topic word-count-input
```

Consume the stream generate
```shell
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic word-count-output \
  --from-beginning \
  --formatter kafka.tools.DefaultMessageFormatter \
  --property print.key=true \
  --property print.value=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```