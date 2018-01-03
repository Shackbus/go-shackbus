// Package registry is an interface for service discovery
package registry

import (
	"errors"
)

// The registry provides an interface for service discovery
// and an abstraction over varying implementations
// {consul, etcd, zookeeper, ...}
type Registry interface {
	Register(Service, ...RegisterOption) error
	Deregister(Service) error
	ListServices() ([]Service, error)
	SetUpdateFunc(UpdateFunc)
	String() string
}

type Action int

const (
	Create Action = iota
	Delete Action = iota
)

type UpdateFunc func(Action, Service)

type Option func(*Options)

type RegisterOption func(*RegisterOptions)

var (
	DefaultRegistry = newNatsRegistry()

	ErrNotFound = errors.New("not found")
)

func NewRegistry(opts ...Option) Registry {
	return newNatsRegistry(opts...)
}

// Register a service node. Additionally supply options such as TTL.
func Register(s Service, opts ...RegisterOption) error {
	return DefaultRegistry.Register(s, opts...)
}

// Deregister a service node
func Deregister(s Service) error {
	return DefaultRegistry.Deregister(s)
}

// List the services. Only returns service names
func ListServices() ([]Service, error) {
	return DefaultRegistry.ListServices()
}

// SetUpdateFunc sets a callback function which will be executed
// whenever a service is created or deleted
func SetUpdateFunc(f UpdateFunc) {
	DefaultRegistry.SetUpdateFunc(f)
}

func String() string {
	return DefaultRegistry.String()
}
