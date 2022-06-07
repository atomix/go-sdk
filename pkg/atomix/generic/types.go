// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package generic

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"reflect"
)

func New[T any](name string, codec Codec[T]) Type[T] {
	return &goType[T]{
		Codec: codec,
		name:  name,
	}
}

type Type[T any] interface {
	fmt.Stringer
	Codec[T]
	Name() string
}

type goType[T any] struct {
	fmt.Stringer
	Codec[T]
	name string
}

func (t *goType[T]) Name() string {
	return t.name
}

func (t *goType[T]) String() string {
	return t.Name()
}

func Bytes() Type[[]byte] {
	return New[[]byte]("string", bytesCodec{})
}

func String() Type[string] {
	return New[string]("string", stringCodec{})
}

func Int() Type[int] {
	return New[int]("int", intCodec{})
}

func Proto[T proto.Message]() Type[T] {
	return New[T](getTypeName[T](), protoCodec[T]{})
}

func JSON[T any]() Type[T] {
	return New[T](getTypeName[T](), jsonCodec[T]{})
}

func YAML[T any]() Type[T] {
	return New[T](getTypeName[T](), yamlCodec[T]{})
}

func getTypeName[T any]() string {
	var t T
	return reflect.TypeOf(t).Name()
}
