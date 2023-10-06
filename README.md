# Otel Kafka Konsumer

This library enables distributed tracing on the segmentio/kafka-go library and is used on kafka-konsumer.

Please refer to examples to learn how to use it. You can also look at [the open-telemetry go documentation](https://opentelemetry.io/docs/instrumentation/go/getting-started/)

# Demo

In the examples, you can run 
```sh
docker-compose up --build
```

## Producing

![Producing Example](.github/images/producer-example.png)

## Consuming

![Consuming Example](.github/images/consumer-example.png)

## Bring it all together

You can run producer and consumer, respectively, to see that they work together.

![Producing - Consuming Together](.github/images/consumer-producer-together.png)
