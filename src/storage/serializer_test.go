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
func TestSerializeDeserializeRecord(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "int_col", Type: TypeInt},
			{Name: "varchar_col", Type: TypeVarchar},
			{Name: "date_col", Type: TypeDate},
			{Name: "timestamp_col", Type: TypeTimestamp},
			{Name: "float_col", Type: TypeFloat},
			{Name: "json_col", Type: TypeJSON},
		},
	}

	record := Record{
		Items: []Item{
			{Literal: int64(123)},
			{Literal: "hello"},
			{Literal: "2024-01-26"},
			{Literal: "2024-01-26T12:00:00Z"},
			{Literal: 3.14},
			{Literal: "{\"name\": \"test\"}"},
		},
		IsDeleted: false,
	}

	t.Run("basic serialization and deserialization", func(t *testing.T) {
		serialized := SerializeRecord(schema, record)
		projection := map[int]ColumnProjection{
			0: {MustExtract: true, IsProjected: true},
			1: {MustExtract: true, IsProjected: true},
			2: {MustExtract: true, IsProjected: true},
			3: {MustExtract: true, IsProjected: true},
			4: {MustExtract: true, IsProjected: true},
			5: {MustExtract: true, IsProjected: true},
		}

		deserialized := DeserializeRecord(schema, serialized, projection)
		if deserialized == nil {
			t.Fatal("deserialized record is nil")
		}

		if len(deserialized.Items) != 6 {
			t.Errorf("expected 6 items, got %d", len(deserialized.Items))
		}

		if deserialized.Items[0].Literal.(int64) != 123 {
			t.Errorf("int mismatch, got %v", deserialized.Items[0].Literal)
		}
		if deserialized.Items[1].Literal.(string) != "hello" {
			t.Errorf("varchar mismatch, got %v", deserialized.Items[1].Literal)
		}
		if deserialized.Items[2].Literal.(string) != "2024-01-26" {
			t.Errorf("date mismatch, got %v", deserialized.Items[2].Literal)
		}
		if deserialized.Items[3].Literal.(string) != "2024-01-26T12:00:00Z" {
			t.Errorf("timestamp mismatch, got %v", deserialized.Items[3].Literal)
		}
		if deserialized.Items[4].Literal.(float64) != 3.14 {
			t.Errorf("float mismatch, got %v", deserialized.Items[4].Literal)
		}
		if _, ok := deserialized.Items[5].Literal.(map[string]any); !ok {
			t.Errorf("json mismatch, expected map[string]any, got %T", deserialized.Items[5].Literal)
		}
	})

	t.Run("projection (only int and float)", func(t *testing.T) {
		serialized := SerializeRecord(schema, record)
		projection := map[int]ColumnProjection{
			0: {MustExtract: true, IsProjected: true},
			4: {MustExtract: true, IsProjected: true},
		}

		deserialized := DeserializeRecord(schema, serialized, projection)
		if deserialized == nil {
			t.Fatal("deserialized record is nil")
		}

		if len(deserialized.Items) != 2 {
			t.Errorf("expected 2 items, got %d", len(deserialized.Items))
		}
		if deserialized.Items[0].Literal.(int64) != 123 {
			t.Errorf("int mismatch, got %v", deserialized.Items[0].Literal)
		}
		if deserialized.Items[1].Literal.(float64) != 3.14 {
			t.Errorf("float mismatch, got %v", deserialized.Items[1].Literal)
		}
	})

	t.Run("filtering (int eq)", func(t *testing.T) {
		serialized := SerializeRecord(schema, record)
		projection := map[int]ColumnProjection{
			0: {MustExtract: true, IsFiltered: true, FilterValue: int64(123), FilterOperator: "=", IsProjected: true},
		}
		deserialized := DeserializeRecord(schema, serialized, projection)
		if deserialized == nil {
			t.Error("expected record to match filter")
		}

		projection = map[int]ColumnProjection{
			0: {MustExtract: true, IsFiltered: true, FilterValue: int64(124), FilterOperator: "=", IsProjected: true},
		}
		deserialized = DeserializeRecord(schema, serialized, projection)
		if deserialized != nil {
			t.Error("expected record NOT to match filter")
		}
	})

	t.Run("deleted record", func(t *testing.T) {
		recordDeleted := record
		recordDeleted.IsDeleted = true
		serialized := SerializeRecord(schema, recordDeleted)
		projection := map[int]ColumnProjection{
			0: {MustExtract: true, IsProjected: true},
		}
		deserialized := DeserializeRecord(schema, serialized, projection)
		if deserialized != nil {
			t.Error("expected deleted record to return nil")
		}
	})
}
