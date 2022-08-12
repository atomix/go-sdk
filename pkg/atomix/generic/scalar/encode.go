// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"reflect"
	"strconv"
)

func NewEncodeFunc[T Scalar]() func(T) string {
	var key T
	switch any(key).(type) {
	case string:
		return func(key T) string {
			return any(key).(string)
		}
	case int:
		return func(key T) string {
			return strconv.Itoa(any(key).(int))
		}
	case int8:
		return func(key T) string {
			return strconv.Itoa(int(any(key).(int8)))
		}
	case int16:
		return func(key T) string {
			return strconv.Itoa(int(any(key).(int16)))
		}
	case int32:
		return func(key T) string {
			return strconv.Itoa(int(any(key).(int32)))
		}
	case int64:
		return func(key T) string {
			return strconv.Itoa(int(any(key).(int64)))
		}
	case uint:
		return func(key T) string {
			return strconv.Itoa(int(any(key).(uint)))
		}
	case uint8:
		return func(key T) string {
			return strconv.Itoa(int(any(key).(uint8)))
		}
	case uint16:
		return func(key T) string {
			return strconv.Itoa(int(any(key).(uint16)))
		}
	case uint32:
		return func(key T) string {
			return strconv.Itoa(int(any(key).(uint32)))
		}
	case uint64:
		return func(key T) string {
			return strconv.Itoa(int(any(key).(uint64)))
		}
	default:
		keyType := reflect.TypeOf(key)
		switch keyType.Kind() {
		case reflect.String:
			return func(key T) string {
				return string(key)
			}
		case reflect.Int:
			intType := reflect.TypeOf(1)
			return func(key T) string {
				return strconv.Itoa(reflect.ValueOf(key).
					Convert(intType).
					Interface().(int))
			}
		case reflect.Int8:
			intType := reflect.TypeOf(int8(1))
			return func(key T) string {
				return strconv.Itoa(int(reflect.ValueOf(key).
					Convert(intType).
					Interface().(int8)))
			}
		case reflect.Int16:
			intType := reflect.TypeOf(int16(1))
			return func(key T) string {
				return strconv.Itoa(int(reflect.ValueOf(key).
					Convert(intType).
					Interface().(int16)))
			}
		case reflect.Int32:
			intType := reflect.TypeOf(int32(1))
			return func(key T) string {
				return strconv.Itoa(int(reflect.ValueOf(key).
					Convert(intType).
					Interface().(int32)))
			}
		case reflect.Int64:
			intType := reflect.TypeOf(int64(1))
			return func(key T) string {
				return strconv.Itoa(int(reflect.ValueOf(key).
					Convert(intType).
					Interface().(int64)))
			}
		case reflect.Uint:
			intType := reflect.TypeOf(uint(1))
			return func(key T) string {
				return strconv.Itoa(int(reflect.ValueOf(key).
					Convert(intType).
					Interface().(uint)))
			}
		case reflect.Uint8:
			intType := reflect.TypeOf(uint8(1))
			return func(key T) string {
				return strconv.Itoa(int(reflect.ValueOf(key).
					Convert(intType).
					Interface().(uint8)))
			}
		case reflect.Uint16:
			intType := reflect.TypeOf(uint16(1))
			return func(key T) string {
				return strconv.Itoa(int(reflect.ValueOf(key).
					Convert(intType).
					Interface().(uint16)))
			}
		case reflect.Uint32:
			intType := reflect.TypeOf(uint32(1))
			return func(key T) string {
				return strconv.Itoa(int(reflect.ValueOf(key).
					Convert(intType).
					Interface().(uint32)))
			}
		case reflect.Uint64:
			intType := reflect.TypeOf(uint64(1))
			return func(key T) string {
				return strconv.Itoa(int(reflect.ValueOf(key).
					Convert(intType).
					Interface().(uint64)))
			}
		}
		panic("unsupported key type")
	}
}
