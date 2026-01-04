package storage

import (
	"testing"
)

func TestSerializeDeserializeSchema(t *testing.T) {
	schema := &Schema{
		Columns: []Column{
			{
				Name:      "id",
				Type:      TypeInt,
				Length:    8,
				IsIndexed: true,
			},
			{
				Name:      "name",
				Type:      TypeVarchar,
				Length:    50,
				IsIndexed: false,
			},
			{
				Name:      "age",
				Type:      TypeInt,
				Length:    8,
				IsIndexed: true,
			},
		},
	}

	serialized := SerializeSchema(schema)

	deserialized := DeserializeSchema(serialized)

	if len(deserialized.Columns) != len(schema.Columns) {
		t.Errorf("Column count mismatch: got %d, want %d",
			len(deserialized.Columns), len(schema.Columns))
		return
	}

	for i, originalCol := range schema.Columns {
		deserializedCol := deserialized.Columns[i]

		if originalCol.Name != deserializedCol.Name {
			t.Errorf("Column %d: Name mismatch: got %s, want %s",
				i, deserializedCol.Name, originalCol.Name)
		}

		if originalCol.Type != deserializedCol.Type {
			t.Errorf("Column %d: Type mismatch: got %v, want %v",
				i, deserializedCol.Type, originalCol.Type)
		}

		if originalCol.Length != deserializedCol.Length {
			t.Errorf("Column %d: Length mismatch: got %d, want %d",
				i, deserializedCol.Length, originalCol.Length)
		}

		if originalCol.IsIndexed != deserializedCol.IsIndexed {
			t.Errorf("Column %d (%s): IsIndexed mismatch: got %v, want %v",
				i, originalCol.Name, deserializedCol.IsIndexed, originalCol.IsIndexed)
		}
	}
}
