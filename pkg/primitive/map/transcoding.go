// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package _map //nolint:golint

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
	"github.com/atomix/go-sdk/pkg/types"
	"github.com/atomix/go-sdk/pkg/types/scalar"
)

func newTranscodingMap[K scalar.Scalar, V any](parent Map[string, []byte], keyCodec types.Codec[K], valueCodec types.Codec[V]) Map[K, V] {
	return &transcodingMap[K, V]{
		Map:        parent,
		keyEncoder: scalar.NewEncodeFunc[K](),
		keyDecoder: scalar.NewDecodeFunc[K](),
		valueCodec: valueCodec,
	}
}

type transcodingMap[K scalar.Scalar, V any] struct {
	Map[string, []byte]
	keyEncoder func(K) string
	keyDecoder func(string) (K, error)
	valueCodec types.Codec[V]
}

func (m *transcodingMap[K, V]) Put(ctx context.Context, key K, value V, opts ...PutOption) (*Entry[K, V], error) {
	valueBytes, err := m.valueCodec.Encode(value)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}
	entry, err := m.Map.Put(ctx, m.keyEncoder(key), valueBytes, opts...)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, nil
	}
	value, err = m.valueCodec.Decode(entry.Value)
	if err != nil {
		return nil, errors.NewInvalid("value decoding failed", err)
	}
	return &Entry[K, V]{
		Versioned: primitive.Versioned[V]{
			Value:   value,
			Version: entry.Version,
		},
		Key: key,
	}, nil
}

func (m *transcodingMap[K, V]) Insert(ctx context.Context, key K, value V, opts ...InsertOption) (*Entry[K, V], error) {
	valueBytes, err := m.valueCodec.Encode(value)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}
	entry, err := m.Map.Insert(ctx, m.keyEncoder(key), valueBytes, opts...)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, nil
	}
	value, err = m.valueCodec.Decode(entry.Value)
	if err != nil {
		return nil, errors.NewInvalid("value decoding failed", err)
	}
	return &Entry[K, V]{
		Versioned: primitive.Versioned[V]{
			Value:   value,
			Version: entry.Version,
		},
		Key: key,
	}, nil
}

func (m *transcodingMap[K, V]) Update(ctx context.Context, key K, value V, opts ...UpdateOption) (*Entry[K, V], error) {
	valueBytes, err := m.valueCodec.Encode(value)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}
	entry, err := m.Map.Update(ctx, m.keyEncoder(key), valueBytes, opts...)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, nil
	}
	value, err = m.valueCodec.Decode(entry.Value)
	if err != nil {
		return nil, errors.NewInvalid("value decoding failed", err)
	}
	return &Entry[K, V]{
		Versioned: primitive.Versioned[V]{
			Value:   value,
			Version: entry.Version,
		},
		Key: key,
	}, nil
}

func (m *transcodingMap[K, V]) Get(ctx context.Context, key K, opts ...GetOption) (*Entry[K, V], error) {
	entry, err := m.Map.Get(ctx, m.keyEncoder(key), opts...)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, nil
	}
	value, err := m.valueCodec.Decode(entry.Value)
	if err != nil {
		return nil, errors.NewInvalid("value decoding failed", err)
	}
	return &Entry[K, V]{
		Versioned: primitive.Versioned[V]{
			Value:   value,
			Version: entry.Version,
		},
		Key: key,
	}, nil
}

func (m *transcodingMap[K, V]) Remove(ctx context.Context, key K, opts ...RemoveOption) (*Entry[K, V], error) {
	entry, err := m.Map.Remove(ctx, m.keyEncoder(key), opts...)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, nil
	}
	value, err := m.valueCodec.Decode(entry.Value)
	if err != nil {
		return nil, errors.NewInvalid("value decoding failed", err)
	}
	return &Entry[K, V]{
		Versioned: primitive.Versioned[V]{
			Value:   value,
			Version: entry.Version,
		},
		Key: key,
	}, nil
}

func (m *transcodingMap[K, V]) List(ctx context.Context) (EntryStream[K, V], error) {
	entries, err := m.Map.List(ctx)
	if err != nil {
		return nil, err
	}
	return stream.NewTranscodingStream[*Entry[string, []byte], *Entry[K, V]](entries, m.decode), nil
}

func (m *transcodingMap[K, V]) Watch(ctx context.Context) (EntryStream[K, V], error) {
	entries, err := m.Map.Watch(ctx)
	if err != nil {
		return nil, err
	}
	return stream.NewTranscodingStream[*Entry[string, []byte], *Entry[K, V]](entries, m.decode), nil
}

func (m *transcodingMap[K, V]) Events(ctx context.Context, opts ...EventsOption) (EventStream[K, V], error) {
	events, err := m.Map.Events(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return stream.NewTranscodingStream[Event[string, []byte], Event[K, V]](events, func(event Event[string, []byte]) (Event[K, V], error) {
		switch e := event.(type) {
		case *Inserted[string, []byte]:
			entry, err := m.decode(e.Entry)
			if err != nil {
				return nil, err
			}
			return &Inserted[K, V]{
				Entry: entry,
			}, nil
		case *Updated[string, []byte]:
			oldEntry, err := m.decode(e.PrevEntry)
			if err != nil {
				return nil, err
			}
			newEntry, err := m.decode(e.Entry)
			if err != nil {
				return nil, err
			}
			return &Updated[K, V]{
				PrevEntry: oldEntry,
				Entry:     newEntry,
			}, nil
		case *Removed[string, []byte]:
			entry, err := m.decode(e.Entry)
			if err != nil {
				return nil, err
			}
			return &Removed[K, V]{
				Entry:   entry,
				Expired: e.Expired,
			}, nil
		default:
			panic("unknown Set event type")
		}
	}), nil
}

func (m *transcodingMap[K, V]) Transaction(ctx context.Context) Transaction[K, V] {
	return &transcodingTransaction[K, V]{
		transcodingMap: m,
		Transaction:    m.Map.Transaction(ctx),
	}
}

func (m *transcodingMap[K, V]) decode(entry *Entry[string, []byte]) (*Entry[K, V], error) {
	key, err := m.keyDecoder(entry.Key)
	if err != nil {
		return nil, errors.NewInvalid("key decoding failed", err)
	}
	value, err := m.valueCodec.Decode(entry.Value)
	if err != nil {
		return nil, errors.NewInvalid("value decoding failed", err)
	}
	return &Entry[K, V]{
		Versioned: primitive.Versioned[V]{
			Value:   value,
			Version: entry.Version,
		},
		Key: key,
	}, nil
}

type transcodingTransaction[K scalar.Scalar, V any] struct {
	*transcodingMap[K, V]
	Transaction[string, []byte]
	operations []func() error
}

func (t *transcodingTransaction[K, V]) Put(key K, value V, opts ...PutOption) Transaction[K, V] {
	t.operations = append(t.operations, func() error {
		valueBytes, err := t.valueCodec.Encode(value)
		if err != nil {
			return errors.NewInvalid("value encoding failed", err)
		}
		t.Transaction.Put(t.keyEncoder(key), valueBytes, opts...)
		return nil
	})
	return t
}

func (t *transcodingTransaction[K, V]) Insert(key K, value V, opts ...InsertOption) Transaction[K, V] {
	t.operations = append(t.operations, func() error {
		valueBytes, err := t.valueCodec.Encode(value)
		if err != nil {
			return errors.NewInvalid("value encoding failed", err)
		}
		t.Transaction.Insert(t.keyEncoder(key), valueBytes, opts...)
		return nil
	})
	return t
}

func (t *transcodingTransaction[K, V]) Update(key K, value V, opts ...UpdateOption) Transaction[K, V] {
	t.operations = append(t.operations, func() error {
		valueBytes, err := t.valueCodec.Encode(value)
		if err != nil {
			return errors.NewInvalid("value encoding failed", err)
		}
		t.Transaction.Update(t.keyEncoder(key), valueBytes, opts...)
		return nil
	})
	return t
}

func (t *transcodingTransaction[K, V]) Remove(key K, opts ...RemoveOption) Transaction[K, V] {
	t.operations = append(t.operations, func() error {
		t.Transaction.Remove(t.keyEncoder(key), opts...)
		return nil
	})
	return t
}

func (t *transcodingTransaction[K, V]) Commit() ([]*Entry[K, V], error) {
	for _, operation := range t.operations {
		if err := operation(); err != nil {
			return nil, err
		}
	}
	results, err := t.Transaction.Commit()
	if err != nil {
		return nil, err
	}
	entries := make([]*Entry[K, V], len(results))
	for i, result := range results {
		entry, err := t.decode(result)
		if err != nil {
			return nil, err
		}
		entries[i] = entry
	}
	return entries, nil
}
