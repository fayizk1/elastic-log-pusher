package main

import (
	"net"
	"sync"
	"time"
	"log"
	"strings"
	"git-wip-us.apache.org/repos/asf/thrift.git/lib/go/thrift"
	"github.com/fayizk1/gen-go/elasticsearch" // generated code
)

const BUFFER_SIZE = 2048

type thriftClient struct {
	sync.Mutex
	client *elasticsearch.RestClient
	host, thriftPort string
	outputPipe  chan []byte
	flushInterval int
	closeCh chan struct{}
}

func ThriftConnect(host string, thriftPort string) (*elasticsearch.RestClient, error) {
	binaryProtocol := thrift.NewTBinaryProtocolFactoryDefault()
	socket, err := thrift.NewTSocket(net.JoinHostPort(host, thriftPort))
	if err != nil {
		return nil, err
	}
	bufferedTransport := thrift.NewTBufferedTransport(socket, BUFFER_SIZE)
	client := elasticsearch.NewRestClientFactory(bufferedTransport, binaryProtocol)
	if err := bufferedTransport.Open(); err != nil {
		return nil, err
	}
	return client, nil
}

func NewThriftClient(uri string, outputPipe  chan []byte, flushInterval int)  *thriftClient {
	split_uri := strings.Split(uri, ":")
	if len(split_uri) != 2 {
		panic("Thrift Client : Unknow URI")
	}
	host, thriftPort := split_uri[0], split_uri[1]
	client, err := ThriftConnect(host, thriftPort)
	if err != nil {
		panic("Thrift Client : Unable to connect")
	}
	closeCh := make(chan struct{})
	return &thriftClient{client:client, host:host, thriftPort:thriftPort, outputPipe: outputPipe, flushInterval : flushInterval, closeCh : closeCh}
}

func (tc *thriftClient) Reconnect() {
	tc.Lock()
	defer tc.Unlock()
	if tc.client.Transport != nil {
		tc.client.Transport.Close()
	}
connect:
	log.Println("Reconnecting thrift client- ", tc.host, tc.thriftPort)
	client, err := ThriftConnect(tc.host, tc.thriftPort)
	if err != nil {
		log.Println("Reconnecting failed, waiting 2 Sec before next try")
		time.Sleep(2 * time.Second)
		goto connect
	}
	tc.client = client
}

func (tc *thriftClient) SendData(request elasticsearch.RestRequest) error {
	tc.Lock()
	defer tc.Unlock()
	_,err := tc.client.Execute(&request)
	if err != nil {
		go tc.Reconnect()
		return err
	}
	return nil
}

func (tc *thriftClient) Run() error {
	go func () { 
		var request = elasticsearch.RestRequest{
			Method: elasticsearch.Method_POST,
			Uri:    "/_bulk",
		}
		log.Println(tc.flushInterval)
		var ticker = time.NewTicker(time.Duration(tc.flushInterval) * time.Millisecond)
		if ticker == nil {
			panic("Ticker not initialized")
		}
		log.Println("added ticker", ticker)
		var messages []byte
	mainloop:
		for {
			select {
			case <-ticker.C:
				if len(messages) == 0 {
					continue mainloop
				}
				request.Body = messages
			send:
				err := tc.SendData(request)
				if err != nil {
					log.Println("Failed to send message,", err)
					time.Sleep(100 * time.Millisecond)
					log.Println("sending data again")
					goto send
				}
				messages = nil
			case m := <- tc.outputPipe:
				messages = append(messages, m...)
			case <-tc.closeCh:
				if len(messages) == 0 {
					return
				}
				request.Body = messages
				err := tc.SendData(request)
				if err != nil {
					log.Println("Failed to send message, Not retrying", err)
				}
				return
			}
		}
	}()
	return nil
}

func (tc *thriftClient) Close(pluginName string) {
	tc.closeCh <-struct{}{}
	log.Println("Closed plugin", pluginName)
}

func init() {
        RegisterOutputPlugin("elasticthrift_output", func(uri string, outputPipe  chan []byte, flushInterval int) interface{} {
		return NewThriftClient(uri, outputPipe, flushInterval)
        })
}
