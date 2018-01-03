package pubsub

import (
	"testing"

	"github.com/shackbus/go-shackbus/codec"
	"github.com/shackbus/go-shackbus/codec/json"
	"github.com/shackbus/go-shackbus/codec/protobuf"
	greeter "github.com/shackbus/go-shackbus/codec/protobuf/test"
)

type myObj struct {
	sub Subscriber
}

var codectests = []struct {
	codec codec.Codec
}{
	{json.NewCodec()},
	{protobuf.NewCodec()},
}

var testString = "hello world"
var testNum float32 = 392.123
var topic = "mystation.shackbus.testing"

func TestNatsPubSubWithCodecs(t *testing.T) {

	var subHandler = func(pub Publication) error {
		msg := pub.Message()
		if msg == nil {
			t.Fatal("publication does not contain a message")
		}

		text := ""
		var num float32 = 0

		switch msg.(type) {
		case greeter.GreeterRequest:
			g := msg.(greeter.GreeterRequest)
			text = g.GetText()
			num = g.GetNum()
		default:
			t.Fatal("unknown type of message")
		}

		if text != testString {
			t.Fatal("invalid string in message")
		}

		if num != testNum {
			t.Fatal("invalid float in message")
		}

		return nil
	}

	for _, tt := range codectests {
		nps := newNatsPubSub(Codec(tt.codec))

		if err := nps.Connect(); err != nil {
			t.Fatal(err)
		}

		_, err := nps.Subscribe(topic, subHandler)
		if err != nil {
			t.Fatal(err)
		}

		greetings := greeter.GreeterRequest{
			Text: testString,
			Num:  testNum,
		}

		if err := nps.Publish(topic, &greetings); err != nil {
			t.Fatal(err)
		}

		nps.Disconnect()
	}

}
