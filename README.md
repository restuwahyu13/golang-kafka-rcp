# KAFKA RPC (Request & Reply Pattern)

if you need tutorial about kafka check my repo [here](https://github.com/restuwahyu13/node-kafka), in reallife kafka not support messaging rpc pattern, because kafka only support messaging pattern like pub sub, so i manipulated kafka to apply the concept of the rpc pattern like you use rabbitmq but not perfect.

## Server RPC

```go
package main

import (
 "context"
 "log"
 "os"
 "os/signal"
 "syscall"
 "time"

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
  topic   string             = "account"
  groupId string             = "account-groupId"
  broker  pkg.InterfaceKafka = pkg.NewKafka(context.Background())
  fk      faker.Faker        = faker.New()
  data    Person             = Person{}
 )

 data.ID = shortuuid.New()
 data.Name = fk.App().Name()
 data.Country = fk.Address().Country()
 data.City = fk.Address().City()
 data.PostCode = fk.Address().PostCode()

 replyTo := pkg.ConsumerOverwriteResponse{}
 replyTo.Res = data

 go broker.ConsumerRpc(topic, groupId, &replyTo)

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
```

## Client RPC

```go
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
```
