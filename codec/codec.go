package codec

// Takes in a connection/buffer and returns a new Codec
type NewCodec func() Codec

// Codec is used for encoding where the broker doesn't natively support
// headers in the message type. In this case the entire message is
// encoded as the payload
type Codec interface {
	Marshal(interface{}) ([]byte, error)
	Unmarshal([]byte, interface{}) error
	String() string
}
