package registry

import (
	"reflect"
	"testing"
	"time"

	"github.com/gogo/protobuf/codec"
	"github.com/shackbus/go-shackbus/codec/json"
	"github.com/shackbus/go-shackbus/codec/protobuf"
)

var codectests = []struct {
	codec codec.Codec
}{
	{json.NewCodec()},
	{protobuf.NewCodec()},
}

func TestRegistryWithCodecs(t *testing.T) {

	for _, tt := range codectests {

		reg := NewRegistry(Codec(tt.codec))
		s := Service{
			Name:     "shackbus.dummy",
			Version:  "dev",
			Address:  "none",
			Port:     "55",
			Metadata: map[string]string{"hello": "world"},
		}
		if err := reg.Register(s); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond * 500)

		services, err := reg.ListServices()
		if err != nil {
			t.Fatal(err)
		}

		if len(services) > 1 {
			t.Fatalf("expected only 1 service, but got %v\n", len(services))
		}

		if !reflect.DeepEqual(s, services[0]) {
			t.Fatal("listed service should be equal to running service")
		}

		if err := reg.Deregister(s); err != nil {
			t.Fatal(err)
		}
	}
}
