package main

import (
	"context"
	"fmt"
	"log"

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
	// defer client.Close()

	adminClient = kadm.NewClient(client)
}

func main() {
	fmt.Println("starting.......")
	getAdminClient()

	topicName := "yt-kafka-lecture"

	//configs
	// retention time
	// properties sets
	topicCreationResp, err := adminClient.CreateTopic(context.Background(), 5, -1, nil, topicName)

	if err != nil {
		log.Fatal("Topic creation error: ", err)
	}

	fmt.Println(topicCreationResp)
}
