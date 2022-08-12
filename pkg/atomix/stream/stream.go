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
