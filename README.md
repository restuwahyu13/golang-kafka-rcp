# KAFKA RPC (Request & Reply Pattern)

if you need tutorial about kafka check my repo [here](https://github.com/restuwahyu13/node-kafka), in reallife kafka not support messaging rpc pattern, because kafka only support messaging pattern like pub sub, so i manipulated kafka to apply the concept of the rpc pattern like you use rabbitmq but not perfect.

## Server RPC

```go
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
 )


 broker.ConsumerRpc(queue, groupId, nil)
}
```

## Client RPC

```go
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
```
