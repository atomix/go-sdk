// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package set

import (
	"context"
	"encoding/base64"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/go-sdk/pkg/stream"
	"github.com/atomix/go-sdk/pkg/types"
)

func newTranscodingSet[E any](parent Set[string], codec types.Codec[E]) Set[E] {
	return &transcodingSet[E]{
		Set:   parent,
		codec: codec,
	}
}

type transcodingSet[E any] struct {
	Set[string]
	codec types.Codec[E]
}

func (s *transcodingSet[E]) Add(ctx context.Context, value E) (bool, error) {
	bytes, err := s.codec.Encode(value)
	if err != nil {
		return false, errors.NewInvalid("value encoding failed", err)
	}
	return s.Set.Add(ctx, base64.StdEncoding.EncodeToString(bytes))
}

func (s *transcodingSet[E]) Remove(ctx context.Context, value E) (bool, error) {
	bytes, err := s.codec.Encode(value)
	if err != nil {
		return false, errors.NewInvalid("value encoding failed", err)
	}
	return s.Set.Remove(ctx, base64.StdEncoding.EncodeToString(bytes))
}

func (s *transcodingSet[E]) Contains(ctx context.Context, value E) (bool, error) {
	bytes, err := s.codec.Encode(value)
	if err != nil {
		return false, errors.NewInvalid("value encoding failed", err)
	}
	return s.Set.Contains(ctx, base64.StdEncoding.EncodeToString(bytes))
}

func (s *transcodingSet[E]) Elements(ctx context.Context) (ElementStream[E], error) {
	elements, err := s.Set.Elements(ctx)
	if err != nil {
		return nil, err
	}
	return stream.NewTranscodingStream[string, E](elements, func(value string) (E, error) {
		var element E
		bytes, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return element, err
		}
		return s.codec.Decode(bytes)
	}), nil
}

func (s *transcodingSet[E]) Watch(ctx context.Context) (ElementStream[E], error) {
	elements, err := s.Set.Watch(ctx)
	if err != nil {
		return nil, err
	}
	return stream.NewTranscodingStream[string, E](elements, func(value string) (E, error) {
		var element E
		bytes, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return element, err
		}
		return s.codec.Decode(bytes)
	}), nil
}

func (s *transcodingSet[E]) Events(ctx context.Context) (EventStream[E], error) {
	events, err := s.Set.Events(ctx)
	if err != nil {
		return nil, err
	}
	return stream.NewTranscodingStream[Event[string], Event[E]](events, func(rawEvent Event[string]) (Event[E], error) {
		switch e := rawEvent.(type) {
		case *Added[string]:
			bytes, err := base64.StdEncoding.DecodeString(e.Element)
			if err != nil {
				return nil, err
			}
			element, err := s.codec.Decode(bytes)
			if err != nil {
				return nil, err
			}
			return &Added[E]{
				Element: element,
			}, nil
		case *Removed[string]:
			bytes, err := base64.StdEncoding.DecodeString(e.Element)
			if err != nil {
				return nil, err
			}
			element, err := s.codec.Decode(bytes)
			if err != nil {
				return nil, err
			}
			return &Removed[E]{
				Element: element,
			}, nil
		default:
			panic("unknown Set event type")
		}
	}), nil
}
