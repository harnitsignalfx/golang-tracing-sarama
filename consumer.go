package main

import (
	"fmt"
	"log"

	"github.com/signalfx/signalfx-go-tracing/ddtrace/tracer"
	"github.com/signalfx/signalfx-go-tracing/tracing"

	saramatrace "github.com/signalfx/signalfx-go-tracing/contrib/Shopify/sarama"	
	sarama "gopkg.in/Shopify/sarama.v1"
)

func Example_consumer() {
	tracing.Start(tracing.WithServiceName("Kafka-Consumer"), tracing.WithEndpointURL("https://ingest.<realm>.signalfx.com/v2/trace"),
		tracing.WithAccessToken("<my-token>"), tracing.WithGlobalTag("environment", "kafka-testing"))

	defer tracing.Stop()
	consumer, err := sarama.NewConsumer([]string{"<broker-ip>:9092"}, nil)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	consumer = saramatrace.WrapConsumer(consumer)

	partitionConsumer, err := consumer.ConsumePartition("TestTopic", 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	defer partitionConsumer.Close()

	consumed := 0
	for msg := range partitionConsumer.Messages() {
		// if you want to use the kafka message as a parent span:
		if spanctx, err := tracer.Extract(saramatrace.NewConsumerMessageCarrier(msg)); err == nil {
			// you can create a span using ChildOf(spanctx)
			_ = spanctx
		}

		log.Printf("Consumed message offset %d\n", msg.Offset)
		consumed++
	}
}

func main() {

	fmt.Println("Calling consumer")
	Example_consumer()
	fmt.Println("done..")
}
