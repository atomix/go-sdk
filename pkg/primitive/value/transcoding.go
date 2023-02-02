// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
	"github.com/atomix/go-sdk/pkg/types"
)

func newTranscodingValue[E any](parent Value[[]byte], codec types.Codec[E]) Value[E] {
	return &transcodingValue[E]{
		Value: parent,
		codec: codec,
	}
}

type transcodingValue[V any] struct {
	Value[[]byte]
	codec types.Codec[V]
}

func (m *transcodingValue[V]) Set(ctx context.Context, value V, opts ...SetOption) (primitive.Versioned[V], error) {
	bytes, err := m.codec.Encode(value)
	if err != nil {
		return primitive.Versioned[V]{}, errors.NewInvalid("value encoding failed", err)
	}
	versioned, err := m.Value.Set(ctx, bytes, opts...)
	if err != nil {
		return primitive.Versioned[V]{}, err
	}
	return primitive.Versioned[V]{
		Version: versioned.Version,
		Value:   value,
	}, nil
}

func (m *transcodingValue[V]) Update(ctx context.Context, value V, opts ...UpdateOption) (primitive.Versioned[V], error) {
	bytes, err := m.codec.Encode(value)
	if err != nil {
		return primitive.Versioned[V]{}, errors.NewInvalid("value encoding failed", err)
	}
	versioned, err := m.Value.Update(ctx, bytes, opts...)
	if err != nil {
		return primitive.Versioned[V]{}, err
	}
	return primitive.Versioned[V]{
		Version: versioned.Version,
		Value:   value,
	}, nil
}

func (m *transcodingValue[V]) Get(ctx context.Context, opts ...GetOption) (primitive.Versioned[V], error) {
	versioned, err := m.Value.Get(ctx, opts...)
	if err != nil {
		return primitive.Versioned[V]{}, err
	}
	value, err := m.codec.Decode(versioned.Value)
	if err != nil {
		return primitive.Versioned[V]{}, errors.NewInvalid("value decoding failed", err)
	}
	return primitive.Versioned[V]{
		Version: versioned.Version,
		Value:   value,
	}, nil
}

func (m *transcodingValue[V]) Watch(ctx context.Context) (ValueStream[V], error) {
	elements, err := m.Value.Watch(ctx)
	if err != nil {
		return nil, err
	}
	return stream.NewTranscodingStream[primitive.Versioned[[]byte], primitive.Versioned[V]](elements, func(versioned primitive.Versioned[[]byte]) (primitive.Versioned[V], error) {
		value, err := m.codec.Decode(versioned.Value)
		if err != nil {
			return primitive.Versioned[V]{}, errors.NewInvalid("value decoding failed", err)
		}
		return primitive.Versioned[V]{
			Version: versioned.Version,
			Value:   value,
		}, nil
	}), nil
}

func (v *transcodingValue[V]) Events(ctx context.Context, opts ...EventsOption) (EventStream[V], error) {
	events, err := v.Value.Events(ctx)
	if err != nil {
		return nil, err
	}
	return stream.NewTranscodingStream[Event[[]byte], Event[V]](events, func(event Event[[]byte]) (Event[V], error) {
		switch e := event.(type) {
		case *Created[[]byte]:
			value, err := v.codec.Decode(e.Value.Value)
			if err != nil {
				return nil, errors.NewInvalid("value decoding failed", err)
			}
			return &Created[V]{
				Value: primitive.Versioned[V]{
					Value:   value,
					Version: e.Value.Version,
				},
			}, nil
		case *Updated[[]byte]:
			oldValue, err := v.codec.Decode(e.OldValue.Value)
			if err != nil {
				return nil, errors.NewInvalid("value decoding failed", err)
			}
			newValue, err := v.codec.Decode(e.NewValue.Value)
			if err != nil {
				return nil, errors.NewInvalid("value decoding failed", err)
			}
			return &Updated[V]{
				OldValue: primitive.Versioned[V]{
					Value:   oldValue,
					Version: e.OldValue.Version,
				},
				NewValue: primitive.Versioned[V]{
					Value:   newValue,
					Version: e.NewValue.Version,
				},
			}, nil
		case *Deleted[[]byte]:
			value, err := v.codec.Decode(e.Value.Value)
			if err != nil {
				return nil, errors.NewInvalid("value decoding failed", err)
			}
			return &Deleted[V]{
				Value: primitive.Versioned[V]{
					Value:   value,
					Version: e.Value.Version,
				},
			}, nil
		default:
			panic("unknown Set event type")
		}
	}), nil
}
