
package main

import (
	"os"
	"log"
	"time"
	"errors"
	"strings"
	"gopkg.in/Shopify/sarama.v1"
	"github.com/fayizk1/kafka-1/consumergroup"
)

type KafkaConsumer struct {
	processorPipe chan Packet
	uri string //FIXME: We should use intrface here
	zk_nodes []string
	topics []string
	group string
	consumer *consumergroup.ConsumerGroup
	closeCh chan struct{}
	closeOtCh chan struct{}
	timeout time.Duration
}

func ParseKafkaURI(uri string) ([]string, []string, string, error) {
	splt_uri := strings.Split(uri, ":::")
	if len(splt_uri) != 3 {
		return nil, nil, "", errors.New("Invalid URI")
	}
	zk_nodes := strings.Split(splt_uri[0], ",")
	topics := strings.Split(splt_uri[1], ",")
	group := splt_uri[2]
	return zk_nodes, topics, group, nil
}

func NewKafkaConsumer(processorPipe chan Packet, uri string) *KafkaConsumer {
	zk_nodes, topics, group, err := ParseKafkaURI(uri)
	if err != nil {
		panic(err)
	}
	closeCh := make(chan struct{})
	closeOtCh := make(chan struct{})
	return &KafkaConsumer{processorPipe : processorPipe, uri : uri, zk_nodes : zk_nodes, topics : topics, group : group, closeCh : closeCh, closeOtCh :closeOtCh}
}


func (kc *KafkaConsumer) Run() {
	go func() {
		sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
		hostname, _ := os.Hostname()
		config := consumergroup.NewConfig()
		config.Offsets.Initial = sarama.OffsetNewest
		config.Offsets.ProcessingTimeout = 10 * time.Second
		kc.timeout = 10 * time.Second
		consumer, consumerErr := consumergroup.JoinConsumerGroup(kc.group, kc.topics, kc.zk_nodes, config)
		if consumerErr != nil {
			log.Fatalln(consumerErr)
		}
		kc.consumer = consumer
		go func() {
			for err := range consumer.Errors() {
				log.Println(err)
			}
		}()
		go kc.Watcher()
		var eventCount uint64 = 0
		offsets := make(map[string]map[int32]int64)
		consumerMessageCh := consumer.Messages()
	messageLoop:
		for {
			select {
			case <- kc.closeCh: //This is necessary to handle no message condition
				log.Println("Closing Kafka Consumer")
				break messageLoop
			case message := <-consumerMessageCh:
				if offsets[message.Topic] == nil {
					offsets[message.Topic] = make(map[int32]int64)
				}
				eventCount += 1
				if offsets[message.Topic][message.Partition] != 0 && offsets[message.Topic][message.Partition] != message.Offset-1 {
					log.Printf("Unexpected offset on %s:%d. Expected %d, found %d, diff %d.\n", message.Topic, message.Partition, 
						offsets[message.Topic][message.Partition]+1, message.Offset, message.Offset-offsets[message.Topic][message.Partition]+1)
				}
				offsets[message.Topic][message.Partition] = message.Offset
				select {
				case <- kc.closeCh: // This is necessary to handle exit singnal, since defer execute LIFO order
					log.Println("Closing Kafka Consumer")
					break messageLoop
				case kc.processorPipe <-Packet{pkt : message.Value, addr : hostname, n : 0}:
					consumer.CommitUpto(message)	
				}
			}
		}
		log.Printf("Processed %d events.", eventCount)
	}()	
}

func (kc *KafkaConsumer) Watcher() {
	time.Sleep(kc.timeout)
	zk_failure_count := 0
	for {
		registered, err := kc.consumer.InstanceRegistered()
		if err != nil {
			log.Println(err)
			zk_failure_count += 1
		} else if !registered {
			log.Println("Instance not found in zk, restarting")
			kc.Restart()
			return
		} else {
			log.Println("Instance found in zk, healthy.")
			zk_failure_count = 0
		}
		if zk_failure_count > 15 {
			log.Println("Maximum Zk failure count reached , restarting")
			kc.Restart()
			return
		}
		select {
		case <-kc.closeOtCh:
			log.Println("Kafka CG watcher, got signal, exiting")
			return
		default:
		}
		time.Sleep(time.Second * 2)
	}
}

func (kc *KafkaConsumer) Restart() { //Need to merge with Close
	kc.closeCh <- struct{}{}
	time.Sleep(kc.timeout) //Allow commit routine to commit message
	log.Println("Waiting before closing channel")
	time.Sleep(2 * time.Second) //Another Waiting for clean shutdown
	if err := kc.consumer.Close(); err != nil {
		sarama.Logger.Println("Error closing the consumer", err)
	}
	kc.Run()
	log.Println("Restarted Kafka Consumer")
}

func (kc *KafkaConsumer) Close(pluginName string) {
	kc.closeOtCh <- struct{}{}
	kc.closeCh <- struct{}{}
	time.Sleep(kc.timeout) //Allow commit routine to commit message
	log.Println("Waiting before closing channel")
	time.Sleep(2 * time.Second) //Another Waiting for clean shutdown
	if err := kc.consumer.Close(); err != nil {
		sarama.Logger.Println("Error closing the consumer", err)
	}
	log.Println("Closed Plugin ", pluginName)
}

func init() {
	RegisterInputPlugin("kafka_input", func(processorPipe chan Packet, uri string) interface{} {
		return NewKafkaConsumer(processorPipe, uri)
	})
}

