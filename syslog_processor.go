package main

import (
	"log"
	"time"
	"bytes"
	"errors"
	"unicode"
	"strings"
	"strconv"
	"regexp"
	"math/rand"
	"encoding/json"
)

type Rule struct {
	Keys []string
	Keywords string
}

type SyslogProcessor struct {
	recieverPipe chan Packet
	outputPipe chan []byte
	rules map[string]Rule
	closeCh chan struct{}
}

const layout = time.RFC3339

type SyslogMessage struct {
	Time   time.Time
	Source string
	Facility
	Severity
	Timestamp time.Time // optional
	Hostname  string    // optional
	Tag       string // message tag as defined in RFC 3164
	Content   string // message content as defined in RFC 3164
	Tag1      string // alternate message tag (white rune as separator)
	Content1  string // alternate message content (white rune as separator)
}

func isNotAlnum(r rune) bool {
	return !(unicode.IsLetter(r) || unicode.IsNumber(r))
}

func isNulCrLf(r rune) bool {
	return r == 0 || r == '\r' || r == '\n'
}

func randSeq(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
        b := make([]rune, n)
        for i := range b {
                b[i] = letters[rand.Intn(len(letters))]
        }
        return strconv.FormatInt(time.Now().UnixNano(), 10) + "_" + string(b)
}

func SyslogRuleValidator(raw_rules []string) map[string]Rule {
	rulekeywords := make(map[string]Rule)
	for i := range raw_rules {
		temprulestr := strings.Split(raw_rules[i], "~~~")
		if len(temprulestr) == 3{
			if temprulestr[0] == "" || temprulestr[1] == "" || temprulestr[2] == ""{
				continue
			}
			tempkeys := strings.Split(temprulestr[1], ",")
			temprule := Rule{Keys:tempkeys, Keywords : temprulestr[2]}
			rulekeywords[temprulestr[0]] = temprule
		}
	}
	log.Println("Validated Syslog Rule: ", rulekeywords)
	return rulekeywords
}

func NewSyslogProcessor(recieverPipe chan Packet, outputPipe chan[]byte, raw_rules []string)  *SyslogProcessor {
	rulewords := SyslogRuleValidator(raw_rules)
	closeCh := make(chan struct{})
	return &SyslogProcessor{recieverPipe : recieverPipe, outputPipe : outputPipe, rules : rulewords, closeCh : closeCh}
}

func (sp *SyslogProcessor) Run(){
	go sp.processSyslogPkt()
}

func (sp *SyslogProcessor) processSyslogPkt() {
	var pkt []byte
	var addr string
	var n int
	var packet Packet
	for {
		select {
		case packet = <- sp.recieverPipe:
		case <-sp.closeCh:
			return
		}
		pkt = packet.pkt
		addr = packet.addr
		n = packet.n
		m := new(SyslogMessage)
		m.Source = addr
		m.Time = time.Now().UTC()
		
		// Parse priority (if exists)
		prio := 13 // default priority
		hasPrio := false
		if pkt[0] == '<' {
			n = 1 + bytes.IndexByte(pkt[1:], '>')
			if n > 1 && n < 5 {
				p, err := strconv.Atoi(string(pkt[1:n]))
				if err == nil && p >= 0 {
					hasPrio = true
					prio = p
					pkt = pkt[n+1:]
				}
			}
		}
		m.Severity = Severity(prio & 0x07)
		m.Facility = Facility(prio >> 3)
		// Parse header (if exists)
		if hasPrio && len(pkt) >= 16 && pkt[15] == ' ' {
			// Get timestamp
			layout := "Jan _2 15:04:05"
			ts, err := time.Parse(layout, string(pkt[:15]))
			if err == nil && !ts.IsZero() {
				// Get hostname
				n = 16 + bytes.IndexByte(pkt[16:], ' ')
				if n != 15 {
					m.Timestamp = ts
					m.Hostname = string(pkt[16:n])
					pkt = pkt[n+1:]
				}
			}
			// TODO: check for version an new format of header as
			// described in RFC 5424.
		}
		
		// Parse msg part
		msg := string(bytes.TrimRightFunc(pkt, isNulCrLf))
		n = strings.IndexFunc(msg, isNotAlnum)
		if n != -1 {
			m.Tag = msg[:n]
			m.Content = msg[n:]
		} else {
			m.Content = msg
		}
		msg = strings.TrimFunc(msg, unicode.IsSpace)
		n = strings.IndexFunc(msg, unicode.IsSpace)
		if n != -1 {
			m.Tag1 = msg[:n]
			m.Content1 = strings.TrimLeftFunc(msg[n+1:], unicode.IsSpace)
		} else {
			m.Content1 = msg
		}
		esoutput, err := sp.EncodeESFormat(m)
		if err != nil {
			log.Println(err)
			continue
		}
		sp.outputPipe <- esoutput
	}
}

func (sp *SyslogProcessor) ProcessFN(baseJ []byte, tag, content string) ([]byte, error) {
        rulekeyword, ok := sp.rules[tag]
        if !ok {
                return nil, errors.New("Tag not found.")
        }
        Regexp := regexp.MustCompile(rulekeyword.Keywords)
        result := Regexp.FindStringSubmatch(content)
        if (len(result) - 1) != len(rulekeyword.Keys) {
                return nil, errors.New("No match.")
        }
        root := make(map[string]interface{})
        d := json.NewDecoder(strings.NewReader(string(baseJ)))
        d.UseNumber()
        err := d.Decode(&root);
        if err != nil {
                return nil, err
        }
        for i:= range rulekeyword.Keys {
                root[rulekeyword.Keys[i]] = result[i+1]
        }
        parseJ, err := json.Marshal(&root)
        return parseJ, err
}

func (sp *SyslogProcessor) EncodeESFormat(m *SyslogMessage) ([]byte, error) {
	var index_buf, document_buf, fulldoc_buf bytes.Buffer
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
	document_buf.WriteString(`{"host":`)
	document_buf.WriteString(strconv.Quote(m.Hostname))
	document_buf.WriteString(`,"message":`)
	document_buf.WriteString(strconv.Quote(m.Content))
	document_buf.WriteString(`,"timestamp":`)
	document_buf.WriteString(strconv.Quote(m.Time.Format(layout)))
	document_buf.WriteString(`,"level":`)
	document_buf.WriteString(strconv.Quote(strconv.Itoa(int(m.Severity))))
	document_buf.WriteString(`,"tag":`)
	document_buf.WriteString(strconv.Quote(m.Tag))
	document_buf.WriteString(`,"source":`)
	document_buf.WriteString(strconv.Quote(m.Source))
	document_buf.WriteString(`,"log_type":`)
	document_buf.WriteString(strconv.Quote("syslog"))
	document_buf.WriteString(`,"_id":`)
	document_buf.WriteString(strconv.Quote(id))
	document_buf.WriteString(`}`)
	fieldJ := document_buf.Bytes()
	parseJ, err := sp.ProcessFN(fieldJ, m.Tag, m.Content)
	if err != nil {
		parseJ = fieldJ
	} 
	fulldoc_buf.WriteString(`{"@source":`)
	fulldoc_buf.WriteString(strconv.Quote(m.Source))
	fulldoc_buf.WriteString(`,"@type":`)
	fulldoc_buf.WriteString(strconv.Quote(m.Tag))
	fulldoc_buf.WriteString(`,"@tags":`)
	fulldoc_buf.WriteString(strconv.Quote(`[]`))
	fulldoc_buf.WriteString(`,"@timestamp":`)
	fulldoc_buf.WriteString(strconv.Quote(m.Time.Format(layout)))
	fulldoc_buf.WriteString(`,"@fields":`)
	fulldoc_buf.Write(parseJ)
	fulldoc_buf.WriteString(`}`)
	fullDocJ := fulldoc_buf.Bytes()
	val := append(requestJ, []byte("\n")...) 
	val = append(val, fullDocJ...)
	val = append(val, []byte("\n")...)
	return val, nil
}

func (sp *SyslogProcessor) Close(pluginName string) {
	sp.closeCh <- struct{}{}
	log.Println("Closed plugin ", pluginName)
}

func init() {
        RegisterProcessorPlugin("syslog_processor", func(recieverPipe chan Packet, outputPipe chan[]byte, raw_rules []string) interface{} {
                return NewSyslogProcessor(recieverPipe, outputPipe, raw_rules)
        })
}
