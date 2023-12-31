package pkg

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/lithammer/shortuuid"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

type InterfaceKafka interface {
	listeningConsumer(metadata *publishMetadata, isMatchChan chan bool, messageChan chan kafka.Message)
	listeningConsumerRpc(isMatchChan chan bool, messageChan chan kafka.Message, message kafka.Message, metadata *publishMetadata)
	PublishRpc(topic string, body interface{}) (*kafka.Message, error)
	ConsumerRpc(topic, groupId string, handler func(message kafka.Message) ([]byte, error))
	DeleteTopicRpc(topic string)
}

type publishMetadata struct {
	CorrelationId string `json:"correlationId"`
	ReplyTo       string `json:"replyTo"`
}

type ConsumerOverwriteResponse struct {
	Res interface{} `json:"res"`
}

type structKafka struct {
	ctx           context.Context
	topic         string
	corellationId string
	replyTo       string
}

var (
	publishRequest  publishMetadata    = publishMetadata{}
	publishRequests []publishMetadata  = []publishMetadata{}
	mutex           sync.Mutex         = sync.Mutex{}
	brokers         []string           = []string{"localhost:9092", "localhost:9093"}
	network         string             = "tcp"
	retryCon        int                = 10
	messageChan     chan kafka.Message = make(chan kafka.Message, 1)
	isMatchChan     chan bool          = make(chan bool, 1)
)

func NewKafka(ctx context.Context) InterfaceKafka {
	return &structKafka{ctx: ctx}
}

func (h *structKafka) listeningConsumer(metadata *publishMetadata, isMatchChan chan bool, messageChan chan kafka.Message) {
	log.Println("CLIENT CONSUMER RPC CORRELATION ID: ", metadata.CorrelationId)
	log.Println("CLIENT CONSUMER RPC REPLY TO: ", metadata.ReplyTo)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:               brokers,
		GroupID:               metadata.CorrelationId,
		Topic:                 metadata.ReplyTo,
		MaxAttempts:           retryCon,
		WatchPartitionChanges: true,
	})

	message, err := reader.FetchMessage(h.ctx)
	if err != nil {
		log.Fatal(err.Error())
		return
	}

	if err := reader.CommitMessages(h.ctx, message); err != nil {
		log.Fatal(err.Error())
		return
	}

	for _, v := range message.Headers {
		if v.Key == "correlationId" {
			metadata.CorrelationId = string(v.Value)
			continue
		}

		if v.Key == "replyTo" {
			metadata.ReplyTo = string(v.Value)
			continue
		}

		break
	}

	for _, d := range publishRequests {
		if d.CorrelationId != metadata.CorrelationId {
			isMatchChan <- false
			h.listeningConsumerRpc(isMatchChan, messageChan, message, metadata)
			continue
		}
	}

	isMatchChan <- true
	h.listeningConsumerRpc(isMatchChan, messageChan, message, metadata)
}

func (h *structKafka) listeningConsumerRpc(isMatchChan chan bool, messageChan chan kafka.Message, message kafka.Message, metadata *publishMetadata) {
	log.Println("CLIENT CONSUMER RPC BODY: ", string(message.Value))

	for _, v := range publishRequests {
		select {
		case ok := <-isMatchChan:
			if ok && v.CorrelationId == metadata.CorrelationId {
				messageChan <- message
			} else {
				messageChan <- kafka.Message{}
			}
		default:
			messageChan <- kafka.Message{}
		}
	}
}

func (h *structKafka) PublishRpc(topic string, body interface{}) (*kafka.Message, error) {
	broker := kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Compression:            kafka.Snappy,
		AllowAutoTopicCreation: true,
	}

	if len(publishRequests) > 0 {
		publishRequests = nil
	}

	publishRequest.CorrelationId = shortuuid.New()
	publishRequest.ReplyTo = fmt.Sprintf("rpc.%s", publishRequest.CorrelationId)

	defer mutex.Unlock()
	mutex.Lock()
	publishRequests = append(publishRequests, publishRequest)

	go h.listeningConsumer(&publishRequest, isMatchChan, messageChan)

	bodyByte, err := json.Marshal(&body)
	if err != nil {
		return nil, err
	}

	headers := []protocol.Header{
		{Key: "correlationId", Value: []byte(publishRequest.CorrelationId)},
		{Key: "replyTo", Value: []byte(publishRequest.ReplyTo)},
	}

	msg := kafka.Message{
		Key:     []byte(publishRequest.CorrelationId),
		Value:   bodyByte,
		Headers: headers,
	}

	if err := broker.WriteMessages(h.ctx, msg); err != nil {
		return nil, err
	}

	res := <-messageChan

	defer h.DeleteTopicRpc(fmt.Sprintf("rpc.%s", res.Key))
	return &res, nil
}

func (h *structKafka) ConsumerRpc(topic, groupId string, handler func(message kafka.Message) ([]byte, error)) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:               brokers,
		Topic:                 topic,
		GroupID:               groupId,
		MaxAttempts:           retryCon,
		WatchPartitionChanges: true,
	})

	for {
		message, err := reader.FetchMessage(h.ctx)
		if err != nil {
			log.Fatal(err.Error())
		}

		if err := reader.CommitMessages(h.ctx, message); err != nil {
			log.Fatal(err.Error())
		}

		for _, v := range message.Headers {
			if v.Key == "correlationId" {
				h.corellationId = string(v.Value)
				continue
			}

			if v.Key == "replyTo" {
				h.replyTo = string(v.Value)
				continue
			}

			break
		}

		log.Println("SERVER CONSUMER RPC CORRELATION ID: ", h.corellationId)
		log.Println("SERVER CONSUMER RPC REPLY TO: ", h.replyTo)
		log.Println("SERVER CONSUMER RPC BODY: ", string(message.Value))

		if string(message.Key) == h.corellationId {
			res, err := handler(message)
			if err != nil {
				log.Fatal(err.Error())
			}

			for _, v := range brokers {
				broker, err := kafka.DialLeader(h.ctx, network, v, h.replyTo, message.Partition)
				if err != nil {
					log.Fatal(err.Error())
				}

				headers := []protocol.Header{
					{Key: "correlationId", Value: []byte(h.corellationId)},
					{Key: "replyTo", Value: []byte(h.replyTo)},
				}

				msg := kafka.Message{
					Key:     []byte(h.corellationId),
					Value:   res,
					Headers: headers,
				}

				if _, err := broker.WriteCompressedMessages(kafka.Snappy.Codec(), msg); err != nil {
					log.Fatal(err.Error())
				}
			}
		}
	}
}

func (h *structKafka) DeleteTopicRpc(topic string) {
	for _, v := range brokers {
		broker, err := kafka.Dial(network, v)
		if err != nil {
			log.Fatal(err)
			return
		}

		broker.DeleteTopics(topic)
	}
}
