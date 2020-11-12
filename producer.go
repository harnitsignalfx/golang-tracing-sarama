package main

import (
	"fmt"
	"time"
	"math/rand"

	saramatrace "github.com/signalfx/signalfx-go-tracing/contrib/Shopify/sarama"
	"github.com/signalfx/signalfx-go-tracing/tracing"
	sarama "gopkg.in/Shopify/sarama.v1"
	opentracing "github.com/opentracing/opentracing-go"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func RandStringBytesMask(n int) string {
	b := make([]byte, n)
	for i := 0; i < n; {
		if idx := int(rand.Int63() & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i++
		}
	}
	return string(b)
}

func Example_syncProducer() {
	fmt.Println("global tracer->",opentracing.GlobalTracer())
	tracing.Start(tracing.WithServiceName("Kafka-Producer"),tracing.WithEndpointURL("https://ingest.<realm>.signalfx.com/v2/trace"),
		tracing.WithAccessToken("<my-token>"),tracing.WithGlobalTag("environment","Kafka-Testing"))
	
	defer tracing.Stop()

	// initialize SERVER span kind by leveraging the global tracer
	sp := opentracing.StartSpan("incoming")
	sp.SetTag("span.kind","server")
	sp.SetTag("test-tag", "exampleTag")
	sp.Finish()

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"<broker-ip>:9092"}, cfg)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	producer = saramatrace.WrapSyncProducer(cfg, producer)

	msg := &sarama.ProducerMessage{
		Topic: "TestTopic",
		Value: sarama.StringEncoder(RandStringBytesMask(8)),
	}

	// Inject span context into carrier for propagation
	carrier := saramatrace.NewProducerMessageCarrier(msg)
	err_injecting := opentracing.GlobalTracer().Inject(sp.Context(), opentracing.TextMap, carrier)
	fmt.Println("err3->",err_injecting)
	

	_, _, err = producer.SendMessage(msg)
	if err != nil {
		panic(err)
	}

	
	fmt.Println("global tracer->",opentracing.GlobalTracer())

}

func main() {
	
	for {
		fmt.Println("Calling syncproducer")
		Example_syncProducer()
		fmt.Println("done..")
		time.Sleep(2*time.Second)
	}
	
}
