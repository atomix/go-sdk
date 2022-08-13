// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package generic

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestScalarCodec(t *testing.T) {
	codec := Scalar[string]()
	bytes, err := codec.Encode("foo")
	assert.NoError(t, err)
	value, err := codec.Decode(bytes)
	assert.NoError(t, err)
	assert.Equal(t, "foo", value)
}

type JSONStruct struct {
	Foo string `json:"foo"`
	Bar int    `json:"bar"`
}

func TestJSONCodec(t *testing.T) {
	codec := JSON[JSONStruct]()
	bytes, err := codec.Encode(JSONStruct{
		Foo: "bar",
		Bar: 1,
	})
	assert.NoError(t, err)
	value, err := codec.Decode(bytes)
	assert.NoError(t, err)
	assert.Equal(t, "bar", value.Foo)
	assert.Equal(t, 1, value.Bar)
}

func TestJSONPointerCodec(t *testing.T) {
	codec := JSON[*JSONStruct]()
	bytes, err := codec.Encode(&JSONStruct{
		Foo: "bar",
		Bar: 1,
	})
	assert.NoError(t, err)
	value, err := codec.Decode(bytes)
	assert.NoError(t, err)
	assert.Equal(t, "bar", value.Foo)
	assert.Equal(t, 1, value.Bar)
}

type YAMLStruct struct {
	Foo string `yaml:"foo"`
	Bar int    `yaml:"bar"`
}

func TestYAMLCodec(t *testing.T) {
	codec := YAML[YAMLStruct]()
	bytes, err := codec.Encode(YAMLStruct{
		Foo: "bar",
		Bar: 1,
	})
	assert.NoError(t, err)
	value, err := codec.Decode(bytes)
	assert.NoError(t, err)
	assert.Equal(t, "bar", value.Foo)
	assert.Equal(t, 1, value.Bar)
}

func TestYAMLPointerCodec(t *testing.T) {
	codec := YAML[*YAMLStruct]()
	bytes, err := codec.Encode(&YAMLStruct{
		Foo: "bar",
		Bar: 1,
	})
	assert.NoError(t, err)
	value, err := codec.Decode(bytes)
	assert.NoError(t, err)
	assert.Equal(t, "bar", value.Foo)
	assert.Equal(t, 1, value.Bar)
}

func TestGoGoProtoCodec(t *testing.T) {
	codec := GoGoProto[*runtimev1.PrimitiveId](&runtimev1.PrimitiveId{})
	bytes, err := codec.Encode(&runtimev1.PrimitiveId{
		Name: "foo",
	})
	assert.NoError(t, err)
	value, err := codec.Decode(bytes)
	assert.NoError(t, err)
	assert.Equal(t, "foo", value.Name)
}
