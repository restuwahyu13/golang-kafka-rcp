package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jaswdr/faker"
	"github.com/lithammer/shortuuid"
	"github.com/restuwahyu13/go-kafka-rpc/pkg"
	"github.com/segmentio/kafka-go"
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
		topic   string             = "account"
		groupId string             = "account-groupId"
		broker  pkg.InterfaceKafka = pkg.NewKafka(context.Background())
		fk      faker.Faker        = faker.New()
		person  Person             = Person{}
	)

	go broker.ConsumerRpc(topic, groupId, func(message kafka.Message) ([]byte, error) {
		person.ID = shortuuid.New()
		person.Name = fk.App().Name()
		person.Country = fk.Address().Country()
		person.City = fk.Address().City()
		person.PostCode = fk.Address().PostCode()

		personByte, err := json.Marshal(&person)
		if err != nil {
			return nil, err
		}

		return personByte, nil
	})

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGALRM)

	for {
		select {
		case sigs := <-signalChan:
			log.Printf("Received Signal %s", sigs.String())
			os.Exit(15)
			break
		default:
			time.Sleep(time.Duration(time.Second * 3))
			log.Println("...........................")
			break
		}
	}
}
