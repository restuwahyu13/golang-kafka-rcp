package main

import (
	"context"

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
		queue   string             = "account"
		groupId string             = "account-groupId"
		broker  pkg.InterfaceKafka = pkg.NewKafka(context.Background())

		// fk      faker.Faker                   = faker.New()
		// data    pkg.ConsumerOverwriteResponse = pkg.ConsumerOverwriteResponse{}
	)

	// data.Res = Person{
	// 	ID:       shortuuid.New(),
	// 	Name:     fk.App().Name(),
	// 	Country:  fk.Address().Country(),
	// 	City:     fk.Address().City(),
	// 	PostCode: fk.Address().PostCode(),
	// }

	broker.ConsumerRpc(queue, groupId, nil)
}
