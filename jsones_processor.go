package main

import (
	"log"
	"time"
	"bytes"
	"strconv"
	"encoding/json"
)

type JsonESProcessor struct {
	recieverPipe chan Packet
	outputPipe chan []byte
	closeCh chan struct{}
}

const iso_time_layout = time.RFC3339



func NewJsonESProcessor(recieverPipe chan Packet, outputPipe chan[]byte, raw_rules []string)  *JsonESProcessor {
	closeCh := make(chan struct{})
	return &JsonESProcessor{recieverPipe : recieverPipe, outputPipe : outputPipe, closeCh : closeCh}
}

func (jep *JsonESProcessor) Run(){
	go jep.processJsonEsPkt()
}

func (jep *JsonESProcessor) processJsonEsPkt() {
	var pkt []byte
	var addr string
	var packet Packet
	for {
		select {
		case packet = <- jep.recieverPipe: //Wait untill all input close
		case <- jep.closeCh:
			return
		}
		pkt = packet.pkt
		addr = packet.addr
		current_time := time.Now().UTC()
		pkt_buf := new(bytes.Buffer)
		if err := json.Compact(pkt_buf, pkt); err != nil {
			log.Println(err)
			continue
		}
		var index_buf, fulldoc_buf bytes.Buffer
		const index_layout = "logstash-2006.01.02"
		current_index := time.Now().Format(index_layout)
		id := randSeq(15)
		index_buf.WriteString(`{"index":{"_index":`)
		index_buf.WriteString(strconv.Quote(current_index))
		index_buf.WriteString(`,"_type":`)
		index_buf.WriteString(strconv.Quote("message"))
		index_buf.WriteString(`,"_id":`)
		index_buf.WriteString(strconv.Quote(id))
		index_buf.WriteString(`}}`)
		requestJ := index_buf.Bytes()
		fulldoc_buf.WriteString(`{"@source":`)
		fulldoc_buf.WriteString(strconv.Quote(addr))
		fulldoc_buf.WriteString(`,"@type":`)
		fulldoc_buf.WriteString(strconv.Quote("message"))
		fulldoc_buf.WriteString(`,"@tags":`)
		fulldoc_buf.WriteString(strconv.Quote(`[]`))
		fulldoc_buf.WriteString(`,"@timestamp":`)
		fulldoc_buf.WriteString(strconv.Quote(current_time.Format(iso_time_layout)))
		fulldoc_buf.WriteString(`,"@fields":`)
		fulldoc_buf.Write(pkt_buf.Bytes())
		fulldoc_buf.WriteString(`}`)
		fullDocJ := fulldoc_buf.Bytes()
		val := append(requestJ, []byte("\n")...) 
		val = append(val, fullDocJ...)
		val = append(val, []byte("\n")...)
		jep.outputPipe <- val
	}
}

func (jep *JsonESProcessor) Close(pluginName string) {
	jep.closeCh <- struct{}{}
	log.Println("Closed Plugin ", pluginName)
}

func init() {
        RegisterProcessorPlugin("jsones_processor", func(recieverPipe chan Packet, outputPipe chan[]byte, raw_rules []string) interface{} {
                return NewJsonESProcessor(recieverPipe, outputPipe, raw_rules)
        })
}
