package utils

import (
	"errors"
	"fmt"
	"strings"

	"rdbms/api/models"
	"rdbms/src/storage"
)

var (
	allowedOperatorsSet  = map[string]struct{}{string(storage.OpEq): {}, string(storage.OpNe): {}}
	allowedOperatorsList = []string{string(storage.OpEq), string(storage.OpNe)}
)

func ToStorageSchema(req models.CreateTableRequest) (storage.Schema, error) {
	if req.Name == "" {
		return storage.Schema{}, errors.New("table name is required")
	}
	if len(req.Columns) == 0 {
		return storage.Schema{}, errors.New("at least one column is required")
	}

	columns := make([]storage.Column, 0, len(req.Columns))

	for _, c := range req.Columns {
		colName := c.Name
		if colName == "" {
			return storage.Schema{}, errors.New("column name is required")
		}

		var column_type storage.ColumnType
		var length int
		switch c.Type {
		case 0:
			column_type = storage.TypeInt
			length = 0
		case 1:
			column_type = storage.TypeVarchar
			if c.Length == nil {
				length = 255
			} else {
				length = *c.Length
				if length < 0 {
					return storage.Schema{}, errors.New("invalid length for varchar column")
				}
			}
		case 2:
			column_type = storage.TypeDate
			length = 0
		case 3:
			column_type = storage.TypeTimestamp
			length = 0
		case 4:
			column_type = storage.TypeFloat
			length = 0
		case 5:
			column_type = storage.TypeJSON
			length = 0
		default:
			return storage.Schema{}, errors.New("unsupported type")
		}

		columns = append(columns, storage.Column{
			Name:      colName,
			Type:      column_type,
			Length:    length,
			IsIndexed: c.IsIndexed,
		})
	}

	return storage.Schema{Columns: columns}, nil
}

func SetFilterColumnIndexes(schema storage.Schema, filters []storage.Filter) ([]storage.Filter, error) {
	schemaMap := make(map[string]int)
	for idx, column := range schema.Columns {
		schemaMap[column.Name] = idx
	}

	unknownCols := make([]string, 0)
	badOps := make([]string, 0)
	typeErrors := make([]string, 0)

	for i, f := range filters {
		idx, ok := schemaMap[f.Column]
		if !ok {
			unknownCols = append(unknownCols, f.Column)
			continue
		}
		filters[i].ColumnIndex = idx

		colType := schema.Columns[idx].Type

		if _, ok := allowedOperatorsSet[f.Operator]; !ok {
			badOps = append(badOps, fmt.Sprintf("%s(%s)", f.Column, f.Operator))
			continue
		}
		filters[i].Operator = f.Operator

		var filterValue interface{}
		switch colType {
		case storage.TypeInt:
			n, ok := f.Value.(float64)
			if !ok || n != float64(int64(n)) {
				typeErrors = append(typeErrors, fmt.Sprintf("%s: expected integer", f.Column))
				continue
			}
			filterValue = int64(n)
		case storage.TypeFloat:
			n, ok := f.Value.(float64)
			if !ok {
				typeErrors = append(typeErrors, fmt.Sprintf("%s: expected number", f.Column))
				continue
			}
			filterValue = n
		case storage.TypeVarchar, storage.TypeDate, storage.TypeTimestamp:
			s, ok := f.Value.(string)
			if !ok {
				typeErrors = append(typeErrors, fmt.Sprintf("%s: expected string", f.Column))
				continue
			}
			filterValue = s
		}
		filters[i].Value = filterValue
	}

	if len(unknownCols) > 0 || len(badOps) > 0 || len(typeErrors) > 0 {
		parts := make([]string, 0, 3)
		if len(unknownCols) > 0 {
			parts = append(parts, fmt.Sprintf("unknown columns: %v", unknownCols))
		}
		if len(badOps) > 0 {
			parts = append(parts, fmt.Sprintf("unsupported operators: %v; allowed: %s", badOps, strings.Join(allowedOperatorsList, ", ")))
		}
		if len(typeErrors) > 0 {
			parts = append(parts, fmt.Sprintf("type errors: %v", typeErrors))
		}
		return filters, errors.New(strings.Join(parts, "; "))
	}

	return filters, nil
}
