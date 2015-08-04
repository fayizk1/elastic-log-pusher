package main

import (
	"os"
	"fmt"
	"log"
	"syscall"
	"os/signal"
	"github.com/BurntSushi/toml"
)

type tomlConfig struct {
	Input map[string]inputPlugin
	Processor map[string]processorPlugin
	Output map[string]outputPlugin
	Route map[string]RouteConfig
}

type Packet struct {
	pkt []byte
	addr string
	n int
}

type inputPlugin struct {
	Module string
	Uri string
}

type processorPlugin struct {
	Module string
	Worker int
	Rules []string
}

type outputPlugin struct {
	Module string
	Flushinterval int
	Uris []string
}

type RouteConfig struct {
	Input string
	Processor string
	Output string
}

func main() {
	var config tomlConfig
	var inputPipes = make(map[string]chan Packet)
	var outputPipes = make(map[string]chan []byte)
	if _, err := toml.DecodeFile("appconfig.toml", &config); err != nil {
		fmt.Println(err)
		return
	}
	//Input
	for input := range config.Input {
		isRequire := false
		for route := range config.Route {
			if config.Route[route].Input == input {
				isRequire = true
			}
		}
		if !isRequire {
			continue
		}
		if _, ok := AvailableInputPlugins[config.Input[input].Module]; !ok {
			panic(fmt.Sprintf("Unknown Input Module %s", config.Input[input].Module))
		}
		inputPipes[input] = make(chan Packet)
		inputPluginObj := AvailableInputPlugins[config.Input[input].Module](inputPipes[input], config.Input[input].Uri)
		log.Println("Starting Input Module : ", config.Input[input].Module)
		InvokeMethod(inputPluginObj, "Run")
		defer InvokeMethod(inputPluginObj, "Close", config.Input[input].Module)
	}
	//Output
	for output := range config.Output {
		isRequire := false
                for route := range config.Route {
                        if config.Route[route].Output == output {
                                isRequire = true
                        }
                }
		if !isRequire {
			continue
		}
		if _, ok := AvailableOutputPlugins[config.Output[output].Module]; !ok {
                        panic(fmt.Sprintf("Unknown Output Module %s", config.Output[output].Module))
                }
		outputPipes[output] = make(chan []byte)
		fmt.Println(config.Output[output].Flushinterval)
		for i:= range config.Output[output].Uris {
			outputPluginObj := AvailableOutputPlugins[config.Output[output].Module](config.Output[output].Uris[i], outputPipes[output], config.Output[output].Flushinterval)
			log.Println("Starting Output Module : ", config.Output[output].Module)
			InvokeMethod(outputPluginObj, "Run")
			defer InvokeMethod(outputPluginObj, "Close", config.Output[output].Module)
		}
	}

	//Route/Processor
	for route := range config.Route {
		_, ok := inputPipes[config.Route[route].Input]
		if !ok {
			panic(fmt.Sprintf("Input module is not available: %s", config.Route[route].Input))
		}
		_, ok = outputPipes[config.Route[route].Output]
		if !ok {
			panic(fmt.Sprintf("Output module is not available: %s", config.Route[route].Output))
                }
		processor_plugin_name := config.Route[route].Processor
		_, ok = AvailableProcessorPlugins[config.Processor[processor_plugin_name].Module]
		if !ok {
			panic(fmt.Sprintf("Processor module is not available, %s", config.Processor[processor_plugin_name].Module))
		}
		for i := 0 ; i < config.Processor[processor_plugin_name].Worker; i++ {
			processorPluginObj := AvailableProcessorPlugins[config.Processor[processor_plugin_name].Module](inputPipes[config.Route[route].Input], outputPipes[config.Route[route].Output], config.Processor[processor_plugin_name].Rules)
			log.Println("Starting Processor Module : ", config.Processor[processor_plugin_name].Module)
			InvokeMethod(processorPluginObj, "Run")
			defer InvokeMethod(processorPluginObj, "Close", config.Processor[processor_plugin_name].Module)
		}
	}
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		os.Interrupt)
	cleanupDone := make(chan struct{})
	go func() {
		for _ = range sc {
			log.Println("Got Signal, Shutting down the server...")
			cleanupDone <- struct{}{}
		}
	}()
	<-cleanupDone
}
