package protobuf

import (
	proto "github.com/golang/protobuf/proto"
	"github.com/shackbus/go-shackbus/codec"
)

type pbCodec struct{}

func (pb pbCodec) Marshal(v interface{}) ([]byte, error) {
	return proto.Marshal(v.(proto.Message))
}

func (pb pbCodec) Unmarshal(d []byte, v interface{}) error {
	return proto.Unmarshal(d, v.(proto.Message))
}

func (pb pbCodec) String() string {
	return "protobuf"
}

func NewCodec() codec.Codec {
	return pbCodec{}
}
