package main

import (
	"reflect"
)

var (
        AvailableInputPlugins = make(map[string]func(chan Packet, string) interface{})
        AvailableProcessorPlugins = make(map[string]func(chan Packet, chan[]byte, []string) interface{})
        AvailableOutputPlugins = make(map[string]func(string, chan []byte, int) interface{})
)

func RegisterInputPlugin(name string, factory func(chan Packet, string) interface{}) {
        AvailableInputPlugins[name] = factory
}

func RegisterProcessorPlugin(name string, factory func(chan Packet, chan []byte, []string) interface{}) {
	AvailableProcessorPlugins[name] = factory
}

func RegisterOutputPlugin(name string, factory func(string, chan []byte, int) interface{}) {
        AvailableOutputPlugins[name] = factory
}

func InvokeMethod(obj interface{}, func_name string, args... interface{}) {
	inputs := make([]reflect.Value, len(args))
	for i, _ := range args {
		inputs[i] = reflect.ValueOf(args[i])
	}
	reflect.ValueOf(obj).MethodByName(func_name).Call(inputs)
}
