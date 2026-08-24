// Package strictjson decodes bounded protocol documents without accepting
// ambiguous object keys or lossy UTF-8 replacement.
package strictjson

import (
	"bytes"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"
)

var (
	// ErrInvalidUTF8 identifies raw protocol bytes that JSON would otherwise replace with U+FFFD.
	ErrInvalidUTF8 = errors.New("JSON input is not valid UTF-8")
	// ErrDuplicateKey identifies an object whose decoded member names are not unique.
	ErrDuplicateKey = errors.New("JSON object contains a duplicate key")
	// ErrMultipleValues identifies a document containing a second JSON value after the first.
	ErrMultipleValues = errors.New("JSON input contains more than one value")
	// ErrNonCanonicalKey identifies a struct member that differs in case from its declared wire name.
	ErrNonCanonicalKey = errors.New("JSON object key does not use the canonical field name")
)

var (
	jsonUnmarshalerType = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()
	textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
	rawMessageType      = reflect.TypeOf(json.RawMessage{})
)

// Decode requires a caller-bounded byte slice, rejects lossy UTF-8, unknown or
// case-aliased struct fields, duplicate object keys at any depth, and any second JSON value.
// Like encoding/json, destination may be partially populated when an error is returned.
func Decode(payload []byte, destination any) error {
	if !utf8.Valid(payload) {
		return ErrInvalidUTF8
	}
	if err := validateDocumentKeys(payload, reflect.TypeOf(destination)); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	return nil
}

// validateDocumentKeys walks one JSON document before destination mutation,
// enforcing exact struct wire names, unique object keys, and one top-level value.
func validateDocumentKeys(payload []byte, destinationType reflect.Type) error {
	framingDecoder := json.NewDecoder(bytes.NewReader(payload))
	var document json.RawMessage
	if err := framingDecoder.Decode(&document); err != nil {
		return err
	}
	var trailing json.RawMessage
	if err := framingDecoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return ErrMultipleValues
		}
		return err
	}
	keyDecoder := json.NewDecoder(bytes.NewReader(document))
	keyDecoder.UseNumber()
	return scanValue(keyDecoder, destinationType)
}

// scanValue consumes one complete JSON value from decoder and recursively
// verifies unique keys and exact declared names for struct-backed objects.
func scanValue(decoder *json.Decoder, destinationType reflect.Type) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		fields := exactStructFields(destinationType)
		mapValueType := objectMapValueType(destinationType)
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("JSON object member name is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("%w %q", ErrDuplicateKey, key)
			}
			seen[key] = struct{}{}
			fieldType := mapValueType
			structFieldType, exact := fields[key]
			if exact {
				fieldType = structFieldType
			} else {
				for canonical := range fields {
					if strings.EqualFold(key, canonical) {
						return fmt.Errorf("%w %q; expected %q", ErrNonCanonicalKey, key, canonical)
					}
				}
			}
			if err := scanValue(decoder, fieldType); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim('}') {
			return errors.New("JSON object has an invalid closing delimiter")
		}
		return nil
	case '[':
		elementType := collectionElementType(destinationType)
		for decoder.More() {
			if err := scanValue(decoder, elementType); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim(']') {
			return errors.New("JSON array has an invalid closing delimiter")
		}
		return nil
	default:
		return fmt.Errorf("JSON value begins with unexpected delimiter %q", delimiter)
	}
}

// exactStructFields returns the exact JSON member names promoted by a struct;
// maps and custom unmarshaler types intentionally have no fixed field schema.
func exactStructFields(destinationType reflect.Type) map[string]reflect.Type {
	typeToInspect := indirectType(destinationType)
	if typeToInspect == nil || typeToInspect == rawMessageType || implementsUnmarshaler(typeToInspect) || typeToInspect.Kind() != reflect.Struct {
		return nil
	}
	fields := make(map[string]reflect.Type)
	collectStructFields(typeToInspect, fields, make(map[reflect.Type]bool))
	return fields
}

// collectStructFields records exported tagged fields and promoted anonymous
// struct fields while bounding recursive embedded-type traversal.
func collectStructFields(structType reflect.Type, fields map[string]reflect.Type, visiting map[reflect.Type]bool) {
	if visiting[structType] {
		return
	}
	visiting[structType] = true
	defer delete(visiting, structType)
	for index := 0; index < structType.NumField(); index++ {
		field := structType.Field(index)
		tagName, tagged, ignored := jsonFieldName(field)
		if ignored {
			continue
		}
		fieldType := indirectType(field.Type)
		if field.Anonymous && !tagged && fieldType != nil && fieldType.Kind() == reflect.Struct {
			collectStructFields(fieldType, fields, visiting)
			continue
		}
		if field.PkgPath != "" {
			continue
		}
		name := tagName
		if !tagged {
			name = field.Name
		}
		if _, exists := fields[name]; !exists {
			fields[name] = field.Type
		}
	}
}

// jsonFieldName parses the name-bearing portion of an encoding/json struct tag.
func jsonFieldName(field reflect.StructField) (name string, tagged, ignored bool) {
	tag, present := field.Tag.Lookup("json")
	if !present {
		return "", false, false
	}
	name, _, _ = strings.Cut(tag, ",")
	if name == "-" {
		return "", true, true
	}
	if name == "" {
		return "", false, false
	}
	return name, true, false
}

// objectMapValueType returns the schema shared by every value in a map-backed
// JSON object while leaving the map's case-sensitive string keys unrestricted.
func objectMapValueType(destinationType reflect.Type) reflect.Type {
	typeToInspect := indirectType(destinationType)
	if typeToInspect == nil || typeToInspect == rawMessageType || implementsUnmarshaler(typeToInspect) || typeToInspect.Kind() != reflect.Map {
		return nil
	}
	return typeToInspect.Elem()
}

// collectionElementType selects recursive schema only for arrays, slices, and
// maps; interface values and scalar mismatches remain the decoder's concern.
func collectionElementType(destinationType reflect.Type) reflect.Type {
	typeToInspect := indirectType(destinationType)
	if typeToInspect == nil || typeToInspect == rawMessageType || implementsUnmarshaler(typeToInspect) {
		return nil
	}
	switch typeToInspect.Kind() {
	case reflect.Array, reflect.Slice, reflect.Map:
		return typeToInspect.Elem()
	default:
		return nil
	}
}

// indirectType removes pointer layers used only to make an optional destination.
func indirectType(valueType reflect.Type) reflect.Type {
	for valueType != nil && valueType.Kind() == reflect.Pointer {
		valueType = valueType.Elem()
	}
	return valueType
}

// implementsUnmarshaler reports types whose private JSON schema cannot be
// derived safely through reflection and must remain owned by their method.
func implementsUnmarshaler(valueType reflect.Type) bool {
	if valueType == nil {
		return false
	}
	if valueType.Implements(jsonUnmarshalerType) || valueType.Implements(textUnmarshalerType) {
		return true
	}
	return valueType.Kind() != reflect.Pointer && (reflect.PointerTo(valueType).Implements(jsonUnmarshalerType) || reflect.PointerTo(valueType).Implements(textUnmarshalerType))
}
