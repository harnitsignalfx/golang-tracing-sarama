package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/signalfx/signalfx-go-tracing/ddtrace/tracer"
	"github.com/signalfx/signalfx-go-tracing/tracing"

	opentracing "github.com/opentracing/opentracing-go"
	saramatrace "github.com/signalfx/signalfx-go-tracing/contrib/Shopify/sarama"
	httptrace "github.com/signalfx/signalfx-go-tracing/contrib/net/http"
	sarama "gopkg.in/Shopify/sarama.v1"
	
)

func Example_syncProducer() {

	tracing.Start(tracing.WithServiceName("Kafka-Producer"), tracing.WithEndpointURL("https://ingest.<realm>.signalfx.com/v2/trace"),
		tracing.WithAccessToken("<my-token>"), tracing.WithGlobalTag("environment", "kafka-testing"))
	defer tracing.Stop()

	waitGroup := &sync.WaitGroup{}

	waitGroup.Add(1)
	go handleRequests(waitGroup)

	waitGroup.Wait()

}

func handleRequests(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	// creates a new instance of a mux router

	mux := httptrace.NewServeMux(httptrace.WithServiceName("kafka-endpoint"))

	//myRouter := mux.NewRouter().StrictSlash(true)

	handleResponse := responseFunc()

	mux.HandleFunc("/auth", handleResponse)

	srv := &http.Server{
		Addr:    "0.0.0.0:9200",
		Handler: mux, // Pass our instance of gorilla/mux in.
	}

	err := srv.ListenAndServe()

	if err != nil {
		log.Println("Error with mux router: ", err)
		// Erroneous exit to force k8s deployment to restart the pod
		os.Exit(1)
	}

}

func responseFunc() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		keys, ok := r.URL.Query()["key"]

		if !ok || len(keys[0]) < 1 {
			log.Println("Url Param 'key' is missing")
			return
		}

		// Query()["key"] will return an array of items,
		// we only want the single item.
		key := keys[0]

		log.Println("Url Param 'key' is: " + string(key))

		// Get span from context (In this case, its the one generated by httptrace)
		sp, _ := tracer.SpanFromContext(r.Context())

		sp.SetTag("test-tag", "exampleTag")
		sp.SetTag("country", "US")
		sp.SetTag("bidId", "356")

		// You can choose to end the span here, since the response is finished.
		// Or you can defer the span finish to include the time taken to send to
		// kafka
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
			Value: sarama.StringEncoder(key)}

		carrier := saramatrace.NewProducerMessageCarrier(msg)
		err_injecting := opentracing.GlobalTracer().Inject(sp.Context(), opentracing.TextMap, carrier)
		if err_injecting != nil {
			fmt.Println("err injecting trace->", err_injecting)
		}
		_, _, err = producer.SendMessage(msg)
		if err != nil {
			panic(err)
		}
		fmt.Fprintf(w, "Received url parameter %v", key)
	}
}

func main() {

	fmt.Println("Calling syncproducer")
	Example_syncProducer()
	fmt.Println("done..")
	//time.Sleep(2*time.Second)

}
