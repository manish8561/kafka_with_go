package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var brokers = []string{"localhost:29092", "localhost:29093", "localhost:29094"}

var adminClient *kadm.Client

func getAdminClient() {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		panic(err)
	}

	adminClient = kadm.NewClient(client)
}

func getKafkaSimpleClient() *kgo.Client {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		panic(err)
	}
	return client
}

func main() {
	fmt.Println("starting.......")

	//topic definition
	topicName := "yt-kafka-lecture"

	//init admin
	getAdminClient()

	// configs
	// retention time
	// properties sets

	// ceration of topic
	topicCreationResp, err := adminClient.CreateTopic(context.Background(), 5, -1, nil, topicName)

	if err != nil {
		log.Fatal("Topic creation error: ", err)
	}

	fmt.Println(topicCreationResp)

	// simple producer client init
	simpleClient := getKafkaSimpleClient()

	ctx := context.Background()
	// 1.) Producing a message
	// All record production goes through Produce, and the callback can be used
	// to allow for synchronous or asynchronous production.
	var wg sync.WaitGroup
	wg.Add(1)
	// prepare record to produce over kafka
	record := &kgo.Record{Topic: topicName, Value: []byte("Our second message to kafka!")}
	// produce
	simpleClient.Produce(ctx, record, func(_ *kgo.Record, err error) {
		defer wg.Done()
		if err != nil {
			fmt.Printf("record had a produce error: %v\n", err)
		}

	})
	wg.Wait()

}

func kafkaProducerClientClose(client *kgo.Client) {
	client.Close()
}
