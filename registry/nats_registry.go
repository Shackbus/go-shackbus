package registry

import (
	"crypto/tls"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats"
	// "github.com/shackbus/go-shackbus/codec/protobuf"
	sbJson "github.com/shackbus/go-shackbus/codec/json"
	sbService "github.com/shackbus/go-shackbus/sb_service"
)

type natsRegistry struct {
	addrs []string
	opts  Options

	sync.RWMutex
	conn          *nats.Conn
	services      map[string]Service
	queryListener chan bool
	watcher       *nats.Subscription
	updateFunc    UpdateFunc
}

var (
	QueryTopic = "shackbus.registry.query"
	WatchTopic = "shackbus.registry.watch"

	DefaultTimeout = time.Second * 2
	// DefaultCodec   = protobuf.NewCodec()
	DefaultCodec = sbJson.NewCodec()
)

func newConn(addrs []string, secure bool, config *tls.Config) (*nats.Conn, error) {
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

	opts := nats.DefaultOptions
	opts.Servers = cAddrs
	opts.Secure = secure
	opts.TLSConfig = config

	// secure might not be set
	if config != nil {
		opts.Secure = true
	}

	c, err := opts.Connect()
	if err != nil {
		return nil, err
	}
	return c, err
}

func (n *natsRegistry) getConn() (*nats.Conn, error) {
	n.Lock()
	defer n.Unlock()
	if n.conn == nil {
		c, err := newConn(n.addrs, n.opts.Secure, n.opts.TLSConfig)
		if err != nil {
			return nil, err
		}
		n.conn = c
	}
	return n.conn, nil
}

func (n *natsRegistry) register(s Service) error {

	n.Lock()
	defer n.Unlock()

	// cache service
	n.services[s.Name] = s

	// handler which will be used for listening for Service Queries
	// on the QueryTopic
	var queryHandler = func(m *nats.Msg) {

		sQuery := sbService.Query{}

		if err := n.opts.Codec.Unmarshal(m.Data, &sQuery); err != nil {
			return
		}

		service := sbService.Service{}

		switch sQuery.Qtype {
		// is this a get query and we own the service?
		case sbService.QueryType_LIST:
			n.RLock()
			service.Name = n.services[s.Name].Name
			service.Version = n.services[s.Name].Version
			service.Address = n.services[s.Name].Address
			service.Port = n.services[s.Name].Port
			service.Metadata = n.services[s.Name].Metadata
			n.RUnlock()
		default:
			// does not match
			return
		}

		// respond to query
		b, err := n.opts.Codec.Marshal(service)
		if err != nil {
			log.Println(err)
		}
		n.conn.Publish(m.Reply, b)
	}

	// create query listener
	if n.queryListener == nil {
		queryListener := make(chan bool)

		// create a subscriber that responds to queries
		sub, err := n.conn.Subscribe(QueryTopic, queryHandler)
		if err != nil {
			return err
		}

		// Unsubscribe if we're told to do so
		go func() {
			<-queryListener
			sub.Unsubscribe()
		}()

		n.queryListener = queryListener
	}

	return nil
}

func (n *natsRegistry) deregister(s Service) error {
	n.Lock()
	defer n.Unlock()

	// cache leftover service
	_, ok := n.services[s.Name]
	if !ok {
		return nil
	}

	// delete cached service
	delete(n.services, s.Name)

	// delete query listener
	if n.queryListener != nil {
		close(n.queryListener)
		n.queryListener = nil
	}

	return nil
}

func (n *natsRegistry) query() ([]Service, error) {

	conn, err := n.getConn()
	if err != nil {
		return nil, err
	}

	inbox := nats.NewInbox()

	response := make(chan Service, 10)

	// prepare subscription to receive response from the active
	// service(s)
	var responseHandler = func(m *nats.Msg) {
		service := Service{}
		sReply := sbService.Service{}
		if err := n.opts.Codec.Unmarshal(m.Data, &sReply); err != nil {
			log.Println(err)
			return
		}
		service.Name = sReply.Name
		service.Version = sReply.Version
		service.Address = sReply.Address
		service.Port = sReply.Port
		service.Metadata = sReply.Metadata

		select {
		case response <- service:
		case <-time.After(n.opts.Timeout):
		}
	}

	sub, err := conn.Subscribe(inbox, responseHandler)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()

	// prepare request
	query := sbService.Query{
		Qtype: sbService.QueryType_LIST,
	}
	b, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	// publish request
	if err := conn.PublishMsg(&nats.Msg{
		Subject: QueryTopic,
		Reply:   inbox,
		Data:    b,
	}); err != nil {
		return nil, err
	}

	services := make([]Service, 0)

	// wait for responses from the active services and process them
loop:
	for {
		select {
		case service := <-response:
			services = append(services, service)
		case <-time.After(n.opts.Timeout):
			break loop
		}
	}

	return services, nil
}

func (n *natsRegistry) Register(s Service, opts ...RegisterOption) error {

	conn, err := n.getConn()
	if err != nil {
		return err
	}

	if err := n.register(s); err != nil {
		return err
	}

	// Create
	sPub := sbService.Publication{
		Action: sbService.Action_CREATE,
		Service: &sbService.Service{
			Name:     s.Name,
			Version:  s.Version,
			Address:  s.Address,
			Port:     s.Port,
			Metadata: s.Metadata,
		},
	}

	b, err := n.opts.Codec.Marshal(&sPub)
	if err != nil {
		return err
	}

	return conn.Publish(WatchTopic, b)
}

func (n *natsRegistry) Deregister(s Service) error {

	if err := n.deregister(s); err != nil {
		return err
	}

	conn, err := n.getConn()
	if err != nil {
		return err
	}

	// Delete
	sPub := sbService.Publication{
		Action: sbService.Action_DELETE,
		Service: &sbService.Service{
			Name:     s.Name,
			Version:  s.Version,
			Address:  s.Address,
			Port:     s.Port,
			Metadata: s.Metadata,
		},
	}

	b, err := n.opts.Codec.Marshal(&sPub)
	if err != nil {
		return err
	}

	return conn.Publish(WatchTopic, b)
}

func (n *natsRegistry) ListServices() ([]Service, error) {

	n.RLock()
	defer n.RUnlock()

	services := make([]Service, 0, len(n.services))

	for _, s := range n.services {
		services = append(services, s)
	}

	return services, nil
}

func (n *natsRegistry) watch() error {
	conn, err := n.getConn()
	if err != nil {
		return err
	}

	// TBD
	// better use async subscriber and then via
	// channels like in query
	sub, err := conn.SubscribeSync(WatchTopic)
	if err != nil {
		return err
	}

	defer sub.Unsubscribe()

	for {
		m, err := sub.NextMsg(time.Second * 10)
		if err != nil && err == nats.ErrTimeout {
			continue
		} else if err != nil {
			return err
		}

		var sPub *sbService.Publication

		if err := n.opts.Codec.Unmarshal(m.Data, sPub); err != nil {
			return err
		}

		switch sPub.Action {
		case sbService.Action_CREATE:
			n.addService(sPub.Service)
		case sbService.Action_DELETE:
			n.removeService(sPub.Service)
		default:
			log.Printf("unknown service action %v for service %v\n",
				sPub.Action.String(),
				sPub.Service.Name)
		}
	}
}

func (n *natsRegistry) addService(s *sbService.Service) {

	if s == nil {
		return
	}

	// check if service exists in the registry's cache
	n.RLock()
	_, exists := n.services[s.Name]
	n.RUnlock()
	if exists {
		return
	}

	newService := Service{
		Name:     s.Name,
		Version:  s.Version,
		Address:  s.Address,
		Port:     s.Port,
		Metadata: s.Metadata,
	}

	// save new service in registry cache
	n.Lock()
	n.services[newService.Name] = newService
	n.Unlock()

	// execute update callback to inform the application
	if n.updateFunc != nil {
		n.updateFunc(Create, newService)
	}
}

func (n *natsRegistry) removeService(s *sbService.Service) {

	if s == nil {
		return
	}

	// check if service exists in the registry's cache
	n.RLock()
	service, exists := n.services[s.Name]
	n.RUnlock()
	if !exists {
		return
	}

	// execute update callback
	if n.updateFunc != nil {
		n.updateFunc(Delete, service)
	}

	// delete service from registry cache
	n.Lock()
	delete(n.services, service.Name)
	n.Unlock()
}

func (n *natsRegistry) SetUpdateFunc(f UpdateFunc) {
	n.Lock()
	defer n.Unlock()
	n.updateFunc = f
}

func (n *natsRegistry) String() string {
	return "nats"
}

func newNatsRegistry(opts ...Option) Registry {
	options := Options{
		Timeout: DefaultTimeout,
		Codec:   DefaultCodec,
	}

	for _, o := range opts {
		o(&options)
	}

	n := natsRegistry{
		addrs:         options.Addrs,
		opts:          options,
		services:      make(map[string]Service),
		queryListener: make(chan bool),
	}

	return &n
}
