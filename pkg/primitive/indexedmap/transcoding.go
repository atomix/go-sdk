// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package indexedmap

import (
	"context"
	"encoding/base64"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
	"github.com/atomix/go-sdk/pkg/types"
	"github.com/atomix/go-sdk/pkg/types/scalar"
)

func newTranscodingIndexedMap[K scalar.Scalar, V any](parent IndexedMap[string, []byte], keyCodec types.Codec[K], valueCodec types.Codec[V]) IndexedMap[K, V] {
	return &transcodingIndexedMap[K, V]{
		IndexedMap: parent,
		keyCodec:   keyCodec,
		valueCodec: valueCodec,
	}
}

// transcodingIndexedMap is the default single-partition implementation of Map
type transcodingIndexedMap[K scalar.Scalar, V any] struct {
	IndexedMap[string, []byte]
	keyCodec   types.Codec[K]
	valueCodec types.Codec[V]
}

func (m *transcodingIndexedMap[K, V]) Append(ctx context.Context, key K, value V, opts ...AppendOption) (*Entry[K, V], error) {
	keyBytes, err := m.keyCodec.Encode(key)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}
	valueBytes, err := m.valueCodec.Encode(value)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}
	entry, err := m.IndexedMap.Append(ctx, base64.StdEncoding.EncodeToString(keyBytes), valueBytes, opts...)
	if err != nil {
		return nil, err
	}
	return m.decode(entry)
}

func (m *transcodingIndexedMap[K, V]) Update(ctx context.Context, key K, value V, opts ...UpdateOption) (*Entry[K, V], error) {
	keyBytes, err := m.keyCodec.Encode(key)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}
	valueBytes, err := m.valueCodec.Encode(value)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}
	entry, err := m.IndexedMap.Update(ctx, base64.StdEncoding.EncodeToString(keyBytes), valueBytes, opts...)
	if err != nil {
		return nil, err
	}
	return m.decode(entry)
}

func (m *transcodingIndexedMap[K, V]) Get(ctx context.Context, key K, opts ...GetOption) (*Entry[K, V], error) {
	keyBytes, err := m.keyCodec.Encode(key)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}
	entry, err := m.IndexedMap.Get(ctx, base64.StdEncoding.EncodeToString(keyBytes), opts...)
	if err != nil {
		return nil, err
	}
	return m.decode(entry)
}

func (m *transcodingIndexedMap[K, V]) GetIndex(ctx context.Context, index Index, opts ...GetOption) (*Entry[K, V], error) {
	entry, err := m.IndexedMap.GetIndex(ctx, index, opts...)
	if err != nil {
		return nil, err
	}
	return m.decode(entry)
}

func (m *transcodingIndexedMap[K, V]) FirstEntry(ctx context.Context) (*Entry[K, V], error) {
	entry, err := m.IndexedMap.FirstEntry(ctx)
	if err != nil {
		return nil, err
	}
	return m.decode(entry)
}

func (m *transcodingIndexedMap[K, V]) LastEntry(ctx context.Context) (*Entry[K, V], error) {
	entry, err := m.IndexedMap.LastEntry(ctx)
	if err != nil {
		return nil, err
	}
	return m.decode(entry)
}

func (m *transcodingIndexedMap[K, V]) PrevEntry(ctx context.Context, index Index) (*Entry[K, V], error) {
	entry, err := m.IndexedMap.PrevEntry(ctx, index)
	if err != nil {
		return nil, err
	}
	return m.decode(entry)
}

func (m *transcodingIndexedMap[K, V]) NextEntry(ctx context.Context, index Index) (*Entry[K, V], error) {
	entry, err := m.IndexedMap.NextEntry(ctx, index)
	if err != nil {
		return nil, err
	}
	return m.decode(entry)
}

func (m *transcodingIndexedMap[K, V]) Remove(ctx context.Context, key K, opts ...RemoveOption) (*Entry[K, V], error) {
	keyBytes, err := m.keyCodec.Encode(key)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}
	entry, err := m.IndexedMap.Remove(ctx, base64.StdEncoding.EncodeToString(keyBytes), opts...)
	if err != nil {
		return nil, err
	}
	return m.decode(entry)
}

func (m *transcodingIndexedMap[K, V]) RemoveIndex(ctx context.Context, index Index, opts ...RemoveOption) (*Entry[K, V], error) {
	entry, err := m.IndexedMap.RemoveIndex(ctx, index, opts...)
	if err != nil {
		return nil, err
	}
	return m.decode(entry)
}

func (m *transcodingIndexedMap[K, V]) List(ctx context.Context) (EntryStream[K, V], error) {
	entries, err := m.IndexedMap.List(ctx)
	if err != nil {
		return nil, err
	}
	return stream.NewTranscodingStream[*Entry[string, []byte], *Entry[K, V]](entries, m.decode), nil
}

func (m *transcodingIndexedMap[K, V]) Watch(ctx context.Context) (EntryStream[K, V], error) {
	entries, err := m.IndexedMap.Watch(ctx)
	if err != nil {
		return nil, err
	}
	return stream.NewTranscodingStream[*Entry[string, []byte], *Entry[K, V]](entries, m.decode), nil
}

func (m *transcodingIndexedMap[K, V]) Events(ctx context.Context, opts ...EventsOption) (EventStream[K, V], error) {
	events, err := m.IndexedMap.Events(ctx)
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
			oldEntry, err := m.decode(e.OldEntry)
			if err != nil {
				return nil, err
			}
			newEntry, err := m.decode(e.NewEntry)
			if err != nil {
				return nil, err
			}
			return &Updated[K, V]{
				OldEntry: oldEntry,
				NewEntry: newEntry,
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

func (m *transcodingIndexedMap[K, V]) decode(entry *Entry[string, []byte]) (*Entry[K, V], error) {
	keyBytes, err := base64.StdEncoding.DecodeString(entry.Key)
	if err != nil {
		return nil, errors.NewInvalid("key decoding failed", err)
	}
	key, err := m.keyCodec.Decode(keyBytes)
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
		Key:   key,
		Index: entry.Index,
	}, nil
}
