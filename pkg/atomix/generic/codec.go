// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package generic

import (
	"encoding/json"
	"github.com/golang/protobuf/proto"
	"gopkg.in/yaml.v3"
	"strconv"
)

type Marshaler[T any] interface {
	Marshal(*T) ([]byte, error)
}

type Unmarshaler[T any] interface {
	Unmarshal([]byte, *T) error
}

type Codec[T any] interface {
	Marshaler[T]
	Unmarshaler[T]
}

type bytesCodec struct{}

func (c bytesCodec) Marshal(value *[]byte) ([]byte, error) {
	return *value, nil
}

func (c bytesCodec) Unmarshal(out []byte, in *[]byte) error {
	copy(out, *in)
	return nil
}

type stringCodec struct{}

func (c stringCodec) Marshal(value *string) ([]byte, error) {
	return []byte(*value), nil
}

func (c stringCodec) Unmarshal(bytes []byte, value *string) error {
	*value = string(bytes)
	return nil
}

type intCodec struct{}

func (c intCodec) Marshal(value *int) ([]byte, error) {
	return []byte(strconv.FormatInt(int64(*value), 10)), nil
}

func (c intCodec) Unmarshal(bytes []byte, value *int) error {
	i, err := strconv.Atoi(string(bytes))
	if err != nil {
		return err
	}
	*value = i
	return nil
}

type jsonCodec[T any] struct{}

func (c jsonCodec[T]) Marshal(value *T) ([]byte, error) {
	return json.Marshal(value)
}

func (c jsonCodec[T]) Unmarshal(bytes []byte, value *T) error {
	return json.Unmarshal(bytes, value)
}

type yamlCodec[T any] struct{}

func (c yamlCodec[T]) Marshal(value *T) ([]byte, error) {
	return yaml.Marshal(value)
}

func (c yamlCodec[T]) Unmarshal(bytes []byte, value *T) error {
	return yaml.Unmarshal(bytes, value)
}

type protoCodec[T proto.Message] struct{}

func (c protoCodec[T]) Marshal(value *T) ([]byte, error) {
	return proto.Marshal(*value)
}

func (c protoCodec[T]) Unmarshal(bytes []byte, value *T) error {
	return proto.Unmarshal(bytes, *value)
}
