package oasgospice

import "encoding/json"

type NullableAnyOf struct {
	value *interface{}
	isSet bool
}

func (v NullableAnyOf) Get() *interface{} {
	return v.value
}

func (v *NullableAnyOf) Set(val *interface{}) {
	v.value = val
	v.isSet = true
}

func (v NullableAnyOf) IsSet() bool {
	return v.isSet
}

func (v *NullableAnyOf) Unset() {
	v.value = nil
	v.isSet = false
}

func NewNullableAnyOf(val *interface{}) *NullableAnyOf {
	return &NullableAnyOf{value: val, isSet: true}
}

func (v NullableAnyOf) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.value)
}

func (v *NullableAnyOf) UnmarshalJSON(src []byte) error {
	v.isSet = true
	return json.Unmarshal(src, &v.value)
}

type TYPE = string

type AnyOf = interface{}
