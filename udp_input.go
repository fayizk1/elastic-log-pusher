// Syslog server library. It is based on RFC 3164 so it doesn't parse properly
// packets with new header format (described in RFC 5424).

package main

import (
	"log"
	"net"
	"strings"
)

type UDPServer struct {
	conn    net.PacketConn
	processorPipe chan Packet
	uri string
	closeCh chan struct{}
}

//  NewServer creates idle server
func NewUDPServer(processorPipe chan Packet, uri string) *UDPServer {
	closeCh := make(chan struct{})
	return &UDPServer{processorPipe : processorPipe, uri : uri, closeCh : closeCh}
}

// Listen starts gorutine that receives syslog messages on specified address.
// addr can be a path (for unix domain sockets) or host:port (for UDP).
func (s *UDPServer) Run() {
	var c net.PacketConn
	if strings.IndexRune(s.uri, ':') != -1 {
		a, err := net.ResolveUDPAddr("udp", s.uri)
		if err != nil {
			panic(err)
		}
		c, err = net.ListenUDP("udp", a)
		if err != nil {
			panic(err)
		}
	} else {
		a, err := net.ResolveUnixAddr("unixgram", s.uri)
		if err != nil {
			panic(err)
		}
		c, err = net.ListenUnixgram("unixgram", a)
		if err != nil {
			panic(err)
		}
	}
	s.conn = c
	go s.receiver()
}

func (s *UDPServer) receiver() {
	buf := make([]byte, 4096)
	for {
		n, addr, err := s.conn.ReadFrom(buf)
		if err != nil {
				log.Fatalln("Read error:", err)
		}
		pkt := buf[:n]
		s.processorPipe <-Packet{pkt : pkt, addr : addr.String(), n : n}
	}
}

func (s *UDPServer) Close(pluginName string) {
	// Nothing
	log.Println("Closed plugin", pluginName)
}

func init() {
	RegisterInputPlugin("udp_input", func(processorPipe chan Packet, uri string) interface{} {
		return NewUDPServer(processorPipe, uri)
	})
}
