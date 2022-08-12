// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package generic

import (
	"encoding/json"
	"github.com/atomix/go-client/pkg/atomix/generic/scalar"
	gogoproto "github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/proto"
	"gopkg.in/yaml.v3"
)

type Encoder[T any] interface {
	Encode(T) ([]byte, error)
}

type Decoder[T any] interface {
	Decode([]byte) (T, error)
}

type Codec[T any] interface {
	Encoder[T]
	Decoder[T]
}

func Scalar[T scalar.Scalar]() Codec[T] {
	return &scalarCodec[T]{
		encoder: scalar.NewEncodeFunc[T](),
		decoder: scalar.NewDecodeFunc[T](),
	}
}

type scalarCodec[T scalar.Scalar] struct {
	decoder func(string) (T, error)
	encoder func(T) string
}

func (c *scalarCodec[T]) Encode(value T) ([]byte, error) {
	return []byte(c.encoder(value)), nil
}

func (c *scalarCodec[T]) Decode(out []byte) (T, error) {
	return c.decoder(string(out))
}

func JSON[T any]() Codec[T] {
	return jsonCodec[T]{}
}

type jsonCodec[T any] struct{}

func (c jsonCodec[T]) Encode(value T) ([]byte, error) {
	return json.Marshal(&value)
}

func (c jsonCodec[T]) Decode(bytes []byte) (T, error) {
	var t T
	if err := json.Unmarshal(bytes, &t); err != nil {
		return t, err
	}
	return t, nil
}

func YAML[T any]() Codec[T] {
	return yamlCodec[T]{}
}

type yamlCodec[T any] struct{}

func (c yamlCodec[T]) Encode(value T) ([]byte, error) {
	return yaml.Marshal(&value)
}

func (c yamlCodec[T]) Decode(bytes []byte) (T, error) {
	var t T
	if err := yaml.Unmarshal(bytes, &t); err != nil {
		return t, err
	}
	return t, nil
}

func Proto[T proto.Message](prototype T) Codec[T] {
	return &protoCodec[T]{
		prototype: prototype,
	}
}

type protoCodec[T proto.Message] struct {
	prototype T
}

func (c protoCodec[T]) Encode(value T) ([]byte, error) {
	return proto.Marshal(value)
}

func (c protoCodec[T]) Decode(bytes []byte) (T, error) {
	t := proto.Clone(c.prototype)
	if err := proto.Unmarshal(bytes, t); err != nil {
		return t.(T), err
	}
	return t.(T), nil
}

func GoGoProto[T gogoproto.Message](prototype T) Codec[T] {
	return &gogoProtoCodec[T]{
		prototype: prototype,
	}
}

type gogoProtoCodec[T gogoproto.Message] struct {
	prototype T
}

func (c *gogoProtoCodec[T]) Encode(value T) ([]byte, error) {
	return gogoproto.Marshal(value)
}

func (c *gogoProtoCodec[T]) Decode(bytes []byte) (T, error) {
	t := gogoproto.Clone(c.prototype)
	if err := gogoproto.Unmarshal(bytes, t); err != nil {
		return t.(T), err
	}
	return t.(T), nil
}
