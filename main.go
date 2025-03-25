package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// topic definition
const topicName = "yt-kafka-lecture"

var brokers = []string{"localhost:29092", "localhost:29093", "localhost:29094"}

var adminClient *kadm.Client

func getAdminClient() *kgo.Client {
	balancerr := kgo.RoundRobinBalancer()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.Balancers(balancerr), // for distrubute the balance
	)
	if err != nil {
		panic(err)
	}

	adminClient = kadm.NewClient(client)

	// ceration of topic
	_, err = adminClient.CreateTopic(context.Background(), 5, -1, nil, topicName)

	if err != nil {
		if !strings.Contains(err.Error(), "TOPIC_ALREADY_EXISTS") {
			log.Fatal("Topic creation error: ", err)
		}
	}

	return client

}

func main() {
	fmt.Println("starting.......")

	// configs
	// retention time
	// properties sets

	// simple producer client init
	simpleClient := getAdminClient()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		KafkaProducer(i, &wg, simpleClient)
	}
	wg.Wait()

}

func KafkaProducer(i int, wg *sync.WaitGroup, simpleClient *kgo.Client) {
	kafkaKey := strconv.Itoa(i)
	ctx := context.Background()
	// 1.) Producing a message
	// All record production goes through Produce, and the callback can be used
	// to allow for synchronous or asynchronous production.
	wg.Add(1)
	// prepare record to produce over kafka
	record := &kgo.Record{Topic: topicName, Key: []byte("kafka_" + kafkaKey), Value: []byte(fmt.Sprintf("Our %s message to kafka!", kafkaKey))}
	// produce
	simpleClient.Produce(ctx, record, func(_ *kgo.Record, err error) {
		defer wg.Done()
		if err != nil {
			fmt.Printf("record had a produce error: %v\n", err)
		}

	})
}

func kafkaProducerClientClose(client *kgo.Client) {
	client.Close()
}
