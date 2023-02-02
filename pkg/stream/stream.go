// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package stream

import "io"

type Stream[T any] interface {
	Next() (T, error)
}

type Result[T any] struct {
	Value T
	Error error
}

func NewChannelStream[T any](ch <-chan Result[T]) Stream[T] {
	return &channelStream[T]{
		ch: ch,
	}
}

type channelStream[T any] struct {
	ch <-chan Result[T]
}

func (s *channelStream[T]) Next() (T, error) {
	var t T
	result, ok := <-s.ch
	if !ok {
		return t, io.EOF
	}
	return result.Value, result.Error
}

func NewTranscodingStream[I, O any](stream Stream[I], transcoder func(I) (O, error)) Stream[O] {
	return &transcodingStream[I, O]{
		stream:     stream,
		transcoder: transcoder,
	}
}

type transcodingStream[I, O any] struct {
	stream     Stream[I]
	transcoder func(I) (O, error)
}

func (s *transcodingStream[I, O]) Next() (O, error) {
	var out O
	in, err := s.stream.Next()
	if err != nil {
		return out, err
	}
	return s.transcoder(in)
}
