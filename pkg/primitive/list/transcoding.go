// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package list

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/go-sdk/pkg/stream"
	"github.com/atomix/go-sdk/pkg/types"
)

func newTranscodingList[E any](parent List[[]byte], codec types.Codec[E]) List[E] {
	return &transcodingList[E]{
		List:  parent,
		codec: codec,
	}
}

// transcodingList is the single partition implementation of List
type transcodingList[E any] struct {
	List[[]byte]
	codec types.Codec[E]
}

func (l *transcodingList[E]) Append(ctx context.Context, value E) error {
	bytes, err := l.codec.Encode(value)
	if err != nil {
		return errors.NewInvalid("value encoding failed", err)
	}
	return l.List.Append(ctx, bytes)
}

func (l *transcodingList[E]) Insert(ctx context.Context, index int, value E) error {
	bytes, err := l.codec.Encode(value)
	if err != nil {
		return errors.NewInvalid("value encoding failed", err)
	}
	return l.List.Insert(ctx, index, bytes)
}

func (l *transcodingList[E]) Set(ctx context.Context, index int, value E) error {
	bytes, err := l.codec.Encode(value)
	if err != nil {
		return errors.NewInvalid("value encoding failed", err)
	}
	return l.List.Set(ctx, index, bytes)
}

func (l *transcodingList[E]) Get(ctx context.Context, index int) (E, error) {
	var e E
	bytes, err := l.List.Get(ctx, index)
	if err != nil {
		return e, err
	}
	return l.codec.Decode(bytes)
}

func (l *transcodingList[E]) Remove(ctx context.Context, index int) (E, error) {
	var e E
	bytes, err := l.List.Remove(ctx, index)
	if err != nil {
		return e, err
	}
	return l.codec.Decode(bytes)
}

func (l *transcodingList[E]) Items(ctx context.Context) (ItemStream[E], error) {
	items, err := l.List.Items(ctx)
	if err != nil {
		return nil, err
	}
	return stream.NewTranscodingStream[[]byte, E](items, l.codec.Decode), nil
}

func (l *transcodingList[E]) Watch(ctx context.Context) (ItemStream[E], error) {
	items, err := l.List.Watch(ctx)
	if err != nil {
		return nil, err
	}
	return stream.NewTranscodingStream[[]byte, E](items, l.codec.Decode), nil
}

func (l *transcodingList[E]) Events(ctx context.Context, opts ...EventsOption) (EventStream[E], error) {
	events, err := l.List.Events(ctx)
	if err != nil {
		return nil, err
	}
	return stream.NewTranscodingStream[Event[[]byte], Event[E]](events, func(event Event[[]byte]) (Event[E], error) {
		switch e := event.(type) {
		case *Inserted[[]byte]:
			value, err := l.codec.Decode(e.Value)
			if err != nil {
				return nil, err
			}
			return &Inserted[E]{
				Value: value,
			}, nil
		case *Updated[[]byte]:
			oldValue, err := l.codec.Decode(e.PrevValue)
			if err != nil {
				return nil, err
			}
			newValue, err := l.codec.Decode(e.Value)
			if err != nil {
				return nil, err
			}
			return &Updated[E]{
				PrevValue: oldValue,
				Value:     newValue,
			}, nil
		case *Removed[[]byte]:
			value, err := l.codec.Decode(e.Value)
			if err != nil {
				return nil, err
			}
			return &Removed[E]{
				Value: value,
			}, nil
		default:
			panic("unknown Set event type")
		}
	}), nil
}
