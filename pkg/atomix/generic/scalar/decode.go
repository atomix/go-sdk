// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"reflect"
	"strconv"
)

func NewDecodeFunc[T Scalar]() func(string) (T, error) {
	var key T
	switch any(key).(type) {
	case string:
		return func(s string) (T, error) {
			return any(s).(T), nil
		}
	case int:
		return func(s string) (T, error) {
			i, _ := strconv.Atoi(s)
			return any(i).(T), nil
		}
	case int8:
		return func(s string) (T, error) {
			i, _ := strconv.Atoi(s)
			return any(int8(i)).(T), nil
		}
	case int16:
		return func(s string) (T, error) {
			i, _ := strconv.Atoi(s)
			return any(int16(i)).(T), nil
		}
	case int32:
		return func(s string) (T, error) {
			i, _ := strconv.Atoi(s)
			return any(int32(i)).(T), nil
		}
	case int64:
		return func(s string) (T, error) {
			i, _ := strconv.Atoi(s)
			return any(int64(i)).(T), nil
		}
	case uint:
		return func(s string) (T, error) {
			i, _ := strconv.Atoi(s)
			return any(uint(i)).(T), nil
		}
	case uint8:
		return func(s string) (T, error) {
			i, _ := strconv.Atoi(s)
			return any(uint8(i)).(T), nil
		}
	case uint16:
		return func(s string) (T, error) {
			i, _ := strconv.Atoi(s)
			return any(uint16(i)).(T), nil
		}
	case uint32:
		return func(s string) (T, error) {
			i, _ := strconv.Atoi(s)
			return any(uint32(i)).(T), nil
		}
	case uint64:
		return func(s string) (T, error) {
			i, _ := strconv.Atoi(s)
			return any(uint64(i)).(T), nil
		}
	default:
		keyType := reflect.TypeOf(key)
		switch keyType.Kind() {
		case reflect.String:
			return func(s string) (T, error) {
				return reflect.ValueOf(s).
					Convert(keyType).
					Interface().(T), nil
			}
		case reflect.Int:
			return func(s string) (T, error) {
				i, _ := strconv.Atoi(s)
				return reflect.ValueOf(i).
					Convert(keyType).
					Interface().(T), nil
			}
		case reflect.Int8:
			return func(s string) (T, error) {
				i, _ := strconv.Atoi(s)
				return reflect.ValueOf(int8(i)).
					Convert(keyType).
					Interface().(T), nil
			}
		case reflect.Int16:
			return func(s string) (T, error) {
				i, _ := strconv.Atoi(s)
				return reflect.ValueOf(int16(i)).
					Convert(keyType).
					Interface().(T), nil
			}
		case reflect.Int32:
			return func(s string) (T, error) {
				i, _ := strconv.Atoi(s)
				return reflect.ValueOf(int32(i)).
					Convert(keyType).
					Interface().(T), nil
			}
		case reflect.Int64:
			return func(s string) (T, error) {
				i, _ := strconv.Atoi(s)
				return reflect.ValueOf(int64(i)).
					Convert(keyType).
					Interface().(T), nil
			}
		case reflect.Uint:
			return func(s string) (T, error) {
				i, _ := strconv.Atoi(s)
				return reflect.ValueOf(uint(i)).
					Convert(keyType).
					Interface().(T), nil
			}
		case reflect.Uint8:
			return func(s string) (T, error) {
				i, _ := strconv.Atoi(s)
				return reflect.ValueOf(uint8(i)).
					Convert(keyType).
					Interface().(T), nil
			}
		case reflect.Uint16:
			return func(s string) (T, error) {
				i, _ := strconv.Atoi(s)
				return reflect.ValueOf(uint16(i)).
					Convert(keyType).
					Interface().(T), nil
			}
		case reflect.Uint32:
			return func(s string) (T, error) {
				i, _ := strconv.Atoi(s)
				return reflect.ValueOf(uint32(i)).
					Convert(keyType).
					Interface().(T), nil
			}
		case reflect.Uint64:
			return func(s string) (T, error) {
				i, _ := strconv.Atoi(s)
				return reflect.ValueOf(uint64(i)).
					Convert(keyType).
					Interface().(T), nil
			}
		}
		panic("unsupported key type")
	}
}
