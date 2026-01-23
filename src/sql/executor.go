package sql

import (
	"fmt"
	"rdbms/src"
	"rdbms/src/storage"
	"strconv"
	"strings"

	"github.com/xwb1989/sqlparser"
)

type Executor struct {
	Storage src.StorageI
}

func NewExecutor(stg src.StorageI) *Executor {
	return &Executor{Storage: stg}
}

type Result struct {
	Type         string
	Message      string
	Data         []map[string]any
	AffectedRows int
}

func (e *Executor) Execute(query string) (*Result, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("SQL parse error: %w", err)
	}

	switch s := stmt.(type) {
	case *sqlparser.Select:
		return e.executeSelect(s)
	case *sqlparser.Insert:
		return e.executeInsert(s)
	case *sqlparser.Update:
		return e.executeUpdate(s)
	case *sqlparser.Delete:
		return e.executeDelete(s)
	case *sqlparser.DDL:
		return e.executeDDL(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

func (e *Executor) executeSelect(stmt *sqlparser.Select) (*Result, error) {
	tableName := sqlparser.String(stmt.From[0])
	tableName = strings.Trim(tableName, "`")

	var columns []string
	for _, expr := range stmt.SelectExprs {
		switch col := expr.(type) {
		case *sqlparser.StarExpr:
			schema, err := e.Storage.Table().GetTableSchema(tableName + ".schema")
			if err != nil {
				return nil, err
			}
			for _, c := range schema.Columns {
				columns = append(columns, c.Name)
			}
		case *sqlparser.AliasedExpr:
			colName := sqlparser.String(col.Expr)
			colName = strings.Trim(colName, "`")
			columns = append(columns, colName)
		}
	}

	filters, err := e.parseWhere(stmt.Where)
	if err != nil {
		return nil, err
	}

	selectedColumns := storage.SelectedColumns{Columns: columns}
	data, err := e.Storage.Table().GetAllData(tableName, filters, selectedColumns)
	if err != nil {
		return nil, err
	}

	return &Result{
		Type: "SELECT",
		Data: data,
	}, nil
}

func (e *Executor) executeInsert(stmt *sqlparser.Insert) (*Result, error) {
	tableName := sqlparser.String(stmt.Table.Name)
	tableName = strings.Trim(tableName, "`")

	schema, err := e.Storage.Table().GetTableSchema(tableName + ".schema")
	if err != nil {
		return nil, err
	}

	var columnNames []string
	if len(stmt.Columns) > 0 {
		for _, col := range stmt.Columns {
			columnNames = append(columnNames, col.String())
		}
	} else {
		for _, col := range schema.Columns {
			columnNames = append(columnNames, col.Name)
		}
	}

	rows, ok := stmt.Rows.(sqlparser.Values)
	if !ok {
		return nil, fmt.Errorf("unsupported insert format")
	}

	insertedCount := 0
	for _, row := range rows {
		items := make([]storage.Item, len(schema.Columns))

		colValueMap := make(map[string]sqlparser.Expr)
		for i, colName := range columnNames {
			if i < len(row) {
				colValueMap[colName] = row[i]
			}
		}

		for i, col := range schema.Columns {
			expr, exists := colValueMap[col.Name]
			if !exists {
				return nil, fmt.Errorf("missing value for column: %s", col.Name)
			}

			val, err := e.parseValue(expr, col.Type)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", col.Name, err)
			}
			items[i] = storage.Item{Literal: val}
		}

		record := storage.Record{Items: items}
		if err := e.Storage.Table().Insert(tableName, record); err != nil {
			return nil, err
		}
		insertedCount++
	}

	return &Result{
		Type:         "INSERT",
		Message:      fmt.Sprintf("%d row(s) inserted", insertedCount),
		AffectedRows: insertedCount,
	}, nil
}

func (e *Executor) executeUpdate(stmt *sqlparser.Update) (*Result, error) {
	if len(stmt.TableExprs) == 0 {
		return nil, fmt.Errorf("no table specified")
	}

	tableName := ""
	switch t := stmt.TableExprs[0].(type) {
	case *sqlparser.AliasedTableExpr:
		tableName = sqlparser.String(t.Expr)
	default:
		return nil, fmt.Errorf("unsupported table expression")
	}
	tableName = strings.Trim(tableName, "`")

	schema, err := e.Storage.Table().GetTableSchema(tableName + ".schema")
	if err != nil {
		return nil, err
	}

	updatedFields := make(map[string]any)
	for _, expr := range stmt.Exprs {
		colName := expr.Name.Name.String()

		var col *storage.Column
		for i := range schema.Columns {
			if schema.Columns[i].Name == colName {
				col = &schema.Columns[i]
				break
			}
		}
		if col == nil {
			return nil, fmt.Errorf("unknown column: %s", colName)
		}

		val, err := e.parseValue(expr.Expr, col.Type)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", colName, err)
		}
		updatedFields[colName] = val
	}

	filters, err := e.parseWhere(stmt.Where)
	if err != nil {
		return nil, err
	}

	updatedCount, err := e.Storage.Table().Update(tableName, updatedFields, filters)
	if err != nil {
		return nil, err
	}

	return &Result{
		Type:         "UPDATE",
		Message:      fmt.Sprintf("%d row(s) updated", updatedCount),
		AffectedRows: updatedCount,
	}, nil
}

func (e *Executor) executeDelete(stmt *sqlparser.Delete) (*Result, error) {
	if len(stmt.TableExprs) == 0 {
		return nil, fmt.Errorf("no table specified")
	}

	tableName := ""
	switch t := stmt.TableExprs[0].(type) {
	case *sqlparser.AliasedTableExpr:
		tableName = sqlparser.String(t.Expr)
	default:
		return nil, fmt.Errorf("unsupported table expression")
	}
	tableName = strings.Trim(tableName, "`")

	filters, err := e.parseWhere(stmt.Where)
	if err != nil {
		return nil, err
	}

	deletedCount, err := e.Storage.Table().Delete(tableName, filters)
	if err != nil {
		return nil, err
	}

	return &Result{
		Type:         "DELETE",
		Message:      fmt.Sprintf("%d row(s) deleted", deletedCount),
		AffectedRows: deletedCount,
	}, nil
}

func (e *Executor) executeDDL(stmt *sqlparser.DDL) (*Result, error) {
	if stmt.Action != sqlparser.CreateStr {
		return nil, fmt.Errorf("unsupported DDL action: %s", stmt.Action)
	}

	if stmt.TableSpec == nil {
		return nil, fmt.Errorf("invalid CREATE TABLE syntax")
	}

	tableName := stmt.NewName.Name.String()

	var columns []storage.Column
	for _, col := range stmt.TableSpec.Columns {
		colName := col.Name.String()
		colType, colLength := e.parseColumnType(col.Type)

		isIndexed := false
		for _, idx := range stmt.TableSpec.Indexes {
			for _, idxCol := range idx.Columns {
				if idxCol.Column.String() == colName {
					isIndexed = true
					break
				}
			}
		}

		columns = append(columns, storage.Column{
			Name:      colName,
			Type:      colType,
			Length:    colLength,
			IsIndexed: isIndexed,
		})
	}

	schema := &storage.Schema{Columns: columns}
	if err := e.Storage.Table().CreateTable(tableName, schema); err != nil {
		return nil, err
	}

	return &Result{
		Type:    "CREATE",
		Message: fmt.Sprintf("Table '%s' created", tableName),
	}, nil
}

func (e *Executor) parseWhere(where *sqlparser.Where) ([]storage.Filter, error) {
	if where == nil {
		return []storage.Filter{}, nil
	}

	return e.parseExpr(where.Expr)
}

func (ex *Executor) parseExpr(expr sqlparser.Expr) ([]storage.Filter, error) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		colName := ""
		switch left := e.Left.(type) {
		case *sqlparser.ColName:
			colName = left.Name.String()
		default:
			return nil, fmt.Errorf("unsupported left expression in comparison")
		}

		var value any
		switch right := e.Right.(type) {
		case *sqlparser.SQLVal:
			switch right.Type {
			case sqlparser.IntVal:
				v, _ := strconv.ParseInt(string(right.Val), 10, 64)
				value = v
			case sqlparser.StrVal:
				value = string(right.Val)
			case sqlparser.FloatVal:
				v, _ := strconv.ParseFloat(string(right.Val), 64)
				value = v
			}
		}

		return []storage.Filter{{
			Column:   colName,
			Operator: e.Operator,
			Value:    value,
		}}, nil

	case *sqlparser.AndExpr:
		left, err := ex.parseExpr(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := ex.parseExpr(e.Right)
		if err != nil {
			return nil, err
		}
		return append(left, right...), nil

	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func (e *Executor) parseValue(expr sqlparser.Expr, colType storage.ColumnType) (any, error) {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		switch colType {
		case storage.TypeInt:
			val, err := strconv.ParseInt(string(v.Val), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid integer value")
			}
			return val, nil
		case storage.TypeVarchar:
			return string(v.Val), nil
		case storage.TypeFloat:
			val, err := strconv.ParseFloat(string(v.Val), 64)
			if err != nil {
				return nil, fmt.Errorf("invalid float value")
			}
			return val, nil
		case storage.TypeDate, storage.TypeTimestamp:
			return string(v.Val), nil
		case storage.TypeJSON:
			return string(v.Val), nil
		}
	}
	return nil, fmt.Errorf("unsupported value type")
}

func (e *Executor) parseColumnType(colType sqlparser.ColumnType) (storage.ColumnType, int) {
	typeName := strings.ToUpper(colType.Type)

	switch typeName {
	case "INT", "INTEGER", "BIGINT":
		return storage.TypeInt, 0
	case "VARCHAR", "CHAR", "TEXT":
		length := 255
		if colType.Length != nil {
			l, _ := strconv.Atoi(string(colType.Length.Val))
			length = l
		}
		return storage.TypeVarchar, length
	case "FLOAT", "DOUBLE", "DECIMAL":
		return storage.TypeFloat, 0
	case "DATE":
		return storage.TypeDate, 0
	case "TIMESTAMP", "DATETIME":
		return storage.TypeTimestamp, 0
	case "JSON":
		return storage.TypeJSON, 0
	default:
		return storage.TypeVarchar, 255
	}
}
