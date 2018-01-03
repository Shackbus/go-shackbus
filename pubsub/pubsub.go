package pubsub

// Package pubsub is an interface used for asynchronous messaging

// PubSub is an interface used for asynchronous messaging.
type PubSub interface {
	Options() Options
	Address() string
	Connect() error
	Disconnect() error
	Init(...Option) error
	Publish(string, interface{}, ...PublishOption) error
	Subscribe(string, Handler, ...SubscribeOption) (Subscriber, error)
	String() string
}

// Handler is used to process messages via a subscription of a topic.
// The handler is passed a publication interface which contains the
// message and optional Ack method to acknowledge receipt of the message.
type Handler func(Publication) error

// type Message struct {
// 	Header map[string]string
// 	Body   []byte
// }

type Message interface{}

// Publication is given to a subscription handler for processing
type Publication interface {
	Topic() string
	Message() interface{}
	Ack() error
}

// Subscriber is a convenience return type for the Subscribe method
type Subscriber interface {
	Options() SubscribeOptions
	Topic() string
	Unsubscribe() error
}

var (
	DefaultPubSub PubSub = newNatsPubSub()
)

func NewPubSub(opts ...Option) PubSub {
	return newNatsPubSub(opts...)
}

func Init(opts ...Option) error {
	return DefaultPubSub.Init(opts...)
}

func Connect() error {
	return DefaultPubSub.Connect()
}

func Disconnect() error {
	return DefaultPubSub.Disconnect()
}

func Publish(topic string, msg interface{}, opts ...PublishOption) error {
	return DefaultPubSub.Publish(topic, msg, opts...)
}

func Subscribe(topic string, handler Handler, opts ...SubscribeOption) (Subscriber, error) {
	return DefaultPubSub.Subscribe(topic, handler, opts...)
}

func String() string {
	return DefaultPubSub.String()
}
