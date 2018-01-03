// Package nats provides a NATS broker
package pubsub

import (
	"strings"
	"time"

	"github.com/nats-io/nats"
	"github.com/shackbus/go-shackbus/codec/json"
)

type npubsub struct {
	addrs []string
	conn  *nats.Conn
	opts  Options
}

type subscriber struct {
	s    *nats.Subscription
	opts SubscribeOptions
}

type publication struct {
	t string
	m interface{}
}

func (n *publication) Topic() string {
	return n.t
}

func (n *publication) Message() interface{} {
	return n.m
}

func (n *publication) Ack() error {
	return nil
}

func (n *subscriber) Options() SubscribeOptions {
	return n.opts
}

func (n *subscriber) Topic() string {
	return n.s.Subject
}

func (n *subscriber) Unsubscribe() error {
	return n.s.Unsubscribe()
}

func (n *npubsub) Address() string {
	if n.conn != nil && n.conn.IsConnected() {
		return n.conn.ConnectedUrl()
	}
	if len(n.addrs) > 0 {
		return n.addrs[0]
	}

	return ""
}

func setAddrs(addrs []string) []string {
	var cAddrs []string
	for _, addr := range addrs {
		if len(addr) == 0 {
			continue
		}
		if !strings.HasPrefix(addr, "nats://") {
			addr = "nats://" + addr
		}
		cAddrs = append(cAddrs, addr)
	}
	if len(cAddrs) == 0 {
		cAddrs = []string{nats.DefaultURL}
	}
	return cAddrs
}

func (n *npubsub) Connect() error {
	if n.conn != nil {
		return nil
	}

	opts := nats.DefaultOptions
	opts.Servers = n.addrs
	opts.Secure = n.opts.Secure
	opts.TLSConfig = n.opts.TLSConfig
	opts.AllowReconnect = false
	opts.MaxPingsOut = 2
	opts.PingInterval = time.Second * 5

	// secure might not be set
	if n.opts.TLSConfig != nil {
		opts.Secure = true
	}

	c, err := opts.Connect()
	if err != nil {
		return err
	}
	n.conn = c
	return nil
}

func (n *npubsub) Disconnect() error {
	n.conn.Close()
	return nil
}

func (n *npubsub) Init(opts ...Option) error {
	for _, o := range opts {
		o(&n.opts)
	}
	n.addrs = setAddrs(n.opts.Addrs)
	return nil
}

func (n *npubsub) Options() Options {
	return n.opts
}

func (n *npubsub) Publish(topic string, msg interface{}, opts ...PublishOption) error {
	b, err := n.opts.Codec.Marshal(msg)
	if err != nil {
		return err
	}
	return n.conn.Publish(topic, b)
}

func (n *npubsub) Subscribe(topic string, handler Handler, opts ...SubscribeOption) (Subscriber, error) {
	opt := SubscribeOptions{
		AutoAck: true,
	}

	for _, o := range opts {
		o(&opt)
	}

	fn := func(msg *nats.Msg) {
		var m Message
		if err := n.opts.Codec.Unmarshal(msg.Data, &m); err != nil {
			return
		}
		handler(&publication{m: &m, t: msg.Subject})
	}

	var sub *nats.Subscription
	var err error

	sub, err = n.conn.Subscribe(topic, fn)

	if err != nil {
		return nil, err
	}
	return &subscriber{s: sub, opts: opt}, nil
}

func (n *npubsub) String() string {
	return "nats"
}

func newNatsPubSub(opts ...Option) PubSub {
	options := Options{
		// Default codec
		Addrs: []string{"nats://0.0.0.0:4222"},
		Codec: json.NewCodec(),
	}

	for _, o := range opts {
		o(&options)
	}

	return &npubsub{
		addrs: setAddrs(options.Addrs),
		opts:  options,
	}
}
