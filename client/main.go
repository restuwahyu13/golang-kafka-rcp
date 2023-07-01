package main

import (
	"context"
	"fmt"

	"github.com/jaswdr/faker"
	"github.com/lithammer/shortuuid"
	"github.com/restuwahyu13/go-kafka-rpc/pkg"
)

type KafkaMessage struct {
	Topic   string
	Message []byte
}

type Person struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Country  string `json:"country"`
	City     string `json:"city"`
	PostCode string `json:"postcode"`
}

func main() {
	var (
		topic     string             = "account"
		broker    pkg.InterfaceKafka = pkg.NewKafka(context.Background())
		brokerRes chan KafkaMessage  = make(chan KafkaMessage)
		fk        faker.Faker        = faker.New()
	)

	rcpRes := broker.PublishRpc(topic, &Person{
		ID:       shortuuid.New(),
		Name:     fk.App().Name(),
		Country:  fk.Address().Country(),
		City:     fk.Address().City(),
		PostCode: fk.Address().PostCode(),
	})

	go func() {
		for v := range rcpRes {
			brokerRes <- KafkaMessage{Topic: fmt.Sprintf("rpc.%s", string(v.Key)), Message: v.Value}
		}
	}()

	result := <-brokerRes

	defer broker.DeleteTopicRpc(result.Topic)
	fmt.Println("RPC Result: ", string(result.Message))
}
