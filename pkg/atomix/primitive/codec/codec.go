// Copyright 2020-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package codec

import "strconv"

type Codec[T any] interface {
	Encode(T) ([]byte, error)
	Decode([]byte) (T, error)
}

func Bytes() Codec[[]byte] {
	return bytesCodec{}
}

type bytesCodec struct{}

func (c bytesCodec) Encode(bytes []byte) ([]byte, error) {
	return bytes, nil
}

func (c bytesCodec) Decode(bytes []byte) ([]byte, error) {
	return bytes, nil
}

type Bytesifier[T ~[]byte] func([]byte) T

func BytesLike[T ~[]byte](s Bytesifier[T]) Codec[T] {
	return bytesLikeCodec[T]{
		bytesifier: s,
	}
}

type bytesLikeCodec[T ~[]byte] struct {
	bytesifier Bytesifier[T]
}

func (c bytesLikeCodec[T]) Encode(value T) ([]byte, error) {
	return []byte(value), nil
}

func (c bytesLikeCodec[T]) Decode(bytes []byte) (T, error) {
	return c.bytesifier(bytes), nil
}

func String() Codec[string] {
	return stringCodec{}
}

type stringCodec struct{}

func (c stringCodec) Encode(value string) ([]byte, error) {
	return []byte(value), nil
}

func (c stringCodec) Decode(bytes []byte) (string, error) {
	return string(bytes), nil
}

type Stringifier[T ~string] func(string) T

func StringLike[T ~string](s Stringifier[T]) Codec[T] {
	return stringLikeCodec[T]{
		stringifier: s,
	}
}

type stringLikeCodec[T ~string] struct {
	stringifier Stringifier[T]
}

func (c stringLikeCodec[T]) Encode(value T) ([]byte, error) {
	return []byte(value), nil
}

func (c stringLikeCodec[T]) Decode(bytes []byte) (T, error) {
	return c.stringifier(string(bytes)), nil
}

func Int() Codec[int] {
	return intCodec{}
}

type intCodec struct{}

func (c intCodec) Encode(value int) ([]byte, error) {
	return []byte(strconv.FormatInt(int64(value), 10)), nil
}

func (c intCodec) Decode(bytes []byte) (int, error) {
	i, err := strconv.Atoi(string(bytes))
	if err != nil {
		return 0, err
	}
	return int(i), nil
}

type Intifier[T ~int] func(int) T

func IntLike[T ~int](i Intifier[T]) Codec[T] {
	return intLikeCodec[T]{
		intifier: i,
	}
}

type intLikeCodec[T ~int] struct {
	intifier Intifier[T]
}

func (c intLikeCodec[T]) Encode(value T) ([]byte, error) {
	return []byte(strconv.FormatInt(int64(value), 10)), nil
}

func (c intLikeCodec[T]) Decode(bytes []byte) (T, error) {
	i, err := strconv.Atoi(string(bytes))
	if err != nil {
		return 0, err
	}
	return c.intifier(i), nil
}
