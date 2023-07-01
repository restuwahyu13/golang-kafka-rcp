package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jaswdr/faker"
	"github.com/lithammer/shortuuid"
	"github.com/restuwahyu13/go-kafka-rpc/pkg"
)

type Person struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Country  string `json:"country"`
	City     string `json:"city"`
	PostCode string `json:"postcode"`
}

func main() {
	var (
		topic  string             = "account"
		broker pkg.InterfaceKafka = pkg.NewKafka(context.Background())
		fk     faker.Faker        = faker.New()
	)

	message, err := broker.PublishRpc(topic, &Person{
		ID:       shortuuid.New(),
		Name:     fk.App().Name(),
		Country:  fk.Address().Country(),
		City:     fk.Address().City(),
		PostCode: fk.Address().PostCode(),
	})

	if err != nil {
		log.Fatal(err.Error())
	}

	fmt.Println("RPC Result: ", string(message.Value))
}
