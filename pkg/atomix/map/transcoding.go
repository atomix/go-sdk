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

package _map

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive/codec"
)

func newTranscodingMap[K1, V1, K2, V2 any](m Map[K2, V2], keyCodec codec.Codec[K1, K2], valueCodec codec.Codec[V1, V2]) Map[K1, V1] {
	return &transcodingMap[K1, V1, K2, V2]{
		delegate:   m,
		keyCodec:   keyCodec,
		valueCodec: valueCodec,
	}
}

type transcodingMap[K1, V1, K2, V2 any] struct {
	delegate   Map[K2, V2]
	keyCodec   codec.Codec[K1, K2]
	valueCodec codec.Codec[V1, V2]
}

func (m *transcodingMap[K1, V1, K2, V2]) Type() primitive.Type[Map[K1, V1]] {
	return m.delegate.Type()
}

func (m *transcodingMap[K1, V1, K2, V2]) Name() string {
	return m.delegate.Name()
}

func (m *transcodingMap[K1, V1, K2, V2]) Put(ctx context.Context, key K1, value V1, opts ...PutOption) (*Entry[K1, V1], error) {
	encKey, err := m.keyCodec.Encode(key)
	if err != nil {
		return nil, err
	}
	encValue, err := m.valueCodec.Encode(value)
	if err != nil {
		return nil, err
	}
	entry, err := m.delegate.Put(ctx, encKey, encValue, opts...)
	if err != nil {
		return nil, err
	}
	return m.decode(entry)
}

func (m *transcodingMap[K1, V1, K2, V2]) Get(ctx context.Context, key K1, opts ...GetOption) (*Entry[K1, V1], error) {
	encKey, err := m.keyCodec.Encode(key)
	if err != nil {
		return nil, err
	}
	entry, err := m.delegate.Get(ctx, encKey, opts...)
	if err != nil {
		return nil, err
	}
	return m.decode(entry)
}

func (m *transcodingMap[K1, V1, K2, V2]) Remove(ctx context.Context, key K1, opts ...RemoveOption) (*Entry[K1, V1], error) {
	encKey, err := m.keyCodec.Encode(key)
	if err != nil {
		return nil, err
	}
	entry, err := m.delegate.Remove(ctx, encKey, opts...)
	if err != nil {
		return nil, err
	}
	return m.decode(entry)
}

func (m *transcodingMap[K1, V1, K2, V2]) Len(ctx context.Context) (int, error) {
	return m.delegate.Len(ctx)
}

func (m *transcodingMap[K1, V1, K2, V2]) Clear(ctx context.Context) error {
	return m.delegate.Clear(ctx)
}

func (m *transcodingMap[K1, V1, K2, V2]) Entries(ctx context.Context, ch chan<- Entry[K1, V1]) error {
	encCh := make(chan Entry[K2, V2])
	go func() {
		defer close(ch)
		for encEntry := range encCh {
			entry, err := m.decode(&encEntry)
			if err != nil {
				log.Error(err)
			} else {
				ch <- *entry
			}
		}
	}()
	return m.delegate.Entries(ctx, encCh)
}

func (m *transcodingMap[K1, V1, K2, V2]) Watch(ctx context.Context, ch chan<- Event[K1, V1], opts ...WatchOption) error {
	encCh := make(chan Event[K2, V2])
	go func() {
		defer close(ch)
		for encEvent := range encCh {
			entry, err := m.decode(&encEvent.Entry)
			if err != nil {
				log.Error(err)
			} else {
				ch <- Event[K1, V1]{
					Type:  encEvent.Type,
					Entry: *entry,
				}
			}
		}
	}()
	return m.delegate.Watch(ctx, encCh, opts...)
}

func (m *transcodingMap[K1, V1, K2, V2]) Close(ctx context.Context) error {
	return m.delegate.Close(ctx)
}

func (m *transcodingMap[K1, V1, K2, V2]) decode(entry *Entry[K2, V2]) (*Entry[K1, V1], error) {
	newKey, err := m.keyCodec.Decode(entry.Key)
	if err != nil {
		return nil, err
	}
	newValue, err := m.valueCodec.Decode(entry.Value)
	if err != nil {
		return nil, err
	}
	return &Entry[K1, V1]{
		ObjectMeta: entry.ObjectMeta,
		Key:        newKey,
		Value:      newValue,
	}, nil
}

func (m *transcodingMap[K1, V1, K2, V2]) encode(entry *Entry[K1, V1]) (*Entry[K2, V2], error) {
	newKey, err := m.keyCodec.Encode(entry.Key)
	if err != nil {
		return nil, err
	}
	newValue, err := m.valueCodec.Encode(entry.Value)
	if err != nil {
		return nil, err
	}
	return &Entry[K2, V2]{
		ObjectMeta: entry.ObjectMeta,
		Key:        newKey,
		Value:      newValue,
	}, nil
}
