package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type ColumnType int

const (
	TypeInt ColumnType = iota
	TypeVarchar
	TypeDate
	TypeTimestamp
	TypeFloat
	TypeJSON
)

type FilterOperator string

const (
	OpEq FilterOperator = "="
	OpNe FilterOperator = "!="
	// OpLt FilterOperator = "<"
	// OpLe FilterOperator = "<="
	// OpGt FilterOperator = ">"
	// OpGe FilterOperator = ">="
	// OpIn   FilterOperator = "IN"
	// OpLike FilterOperator = "LIKE"
)

type Column struct {
	Name   string
	Type   ColumnType
	Length int
}

type Schema struct {
	Columns []Column
}

type Record struct {
	Items []Item
}

// filter
type Filter struct {
	Column      string
	Operator    string
	Value       interface{}
	ColumnIndex int
}

type SelectedColumns struct {
	Columns []string
}

type ColumnProjection struct {
	Name           string
	Index          int
	IsFiltered     bool
	IsProjected    bool
	MustExtract    bool
	FilterValue    any
	FilterOperator string
}

type Item struct {
	Literal interface{}
}

type TableManager struct {
	FileManager *FileManager
}

type TableI interface {
	CreateTable(name string, schema *Schema) error
	Insert(tableName string, record Record) error
	GetAllData(tableName string, filters []Filter, selectedColumns SelectedColumns) ([]map[string]any, error)
	GetTableSchema(schemaName string) (Schema, error)

	CreateIndex(tableName string, columnName string) error
	GetIndexHeader(fileName string) (IndexHeader, error)
	GetNodeById(fileName string, node_id int64) (Node, error)
	GetNode(fileName string, node_pointer int64) (Node, error)
	InsetValueToIndex(indexName string, value int64, location RecordLocation) error
	SearchValue(indexName string, value int64) (int64, bool, error)

	CreateRecordListFile(tableName string, columnName string) error
	GetRecordListFileHeader(fileName string) (RecordListFileHeader, error)
	AddValueToRecordListFile(fileName string, recordListHead int64, location RecordLocation) (int64, error)
	GetRecordsByValue(fileName string, recordListHead int64) ([]RecordLocation, error)
}

const PageSize = 8192

type PageHeader struct {
	RecordCount      uint16
	FreeSpacePointer uint16
}

type ItemPointer struct {
	Offset uint16
	Length uint16
}

type Page struct {
	Header PageHeader
	Data   [PageSize]byte
	Items  []ItemPointer
}

func NewTableManager(dataDir string) (*TableManager, error) {
	fileManager, err := NewFileManager(dataDir)
	if err != nil {
		return nil, err
	}
	return &TableManager{FileManager: fileManager}, nil
}

func (tm *TableManager) CreateTable(name string, schema *Schema) error {
	schema_exist := tm.FileManager.FileExists(name + ".schema")

	if schema_exist {
		return errors.New("schema already exists")
	}

	table_exist := tm.FileManager.FileExists(name + ".table")

	if table_exist {
		return errors.New("table already exists")
	}

	fsm_exist := tm.FileManager.FileExists(name + ".fsm")

	if fsm_exist {
		return errors.New("fsm file already exists")
	}

	schema_file, err := tm.FileManager.CreateFile(name + ".schema")

	if err != nil {
		return err
	}

	_, err = tm.FileManager.CreateFile(name + ".table")

	if err != nil {
		return err
	}

	_, err = tm.FileManager.CreateFile(name + ".fsm")

	if err != nil {
		return err
	}

	serialized_schema := SerializeSchema(schema)
	_, err = schema_file.Write(serialized_schema)

	if err != nil {
		return err
	}

	return nil
}

func (tm *TableManager) GetTableSchema(schemaName string) (schema Schema, err error) {
	schema = Schema{}

	if !tm.FileManager.FileExists(schemaName) {
		return schema, errors.New("table does not exist")
	}

	data, err := tm.FileManager.ReadAll(schemaName)
	if err != nil {
		return schema, err
	}
	schema = DeserializeSchema(data)

	return schema, nil
}

func (tm *TableManager) Insert(tableName string, record Record) error {

	schema, err := tm.GetTableSchema(tableName + ".schema")

	if err != nil {
		fmt.Println("schema")
		fmt.Println(err.Error())
		return err
	}

	serialized_record := SerializeRecord(schema, record)

	page, page_order, err := tm.FindOrCreatePage(tableName, serialized_record)

	if err != nil {
		fmt.Println("page finding section:")
		fmt.Println(err.Error())
		return err
	}

	err = tm.FileManager.Write(tableName+".table", (int64(page_order)-1)*8192, page)

	if err != nil {
		fmt.Println("page section")
		fmt.Println(err.Error())
		return err
	}

	return nil
}

func (tm *TableManager) GetAllData(tableName string, filters []Filter, selectedColumns SelectedColumns) ([]map[string]any, error) {
	schema, err := tm.GetTableSchema(tableName + ".schema")
	if err != nil {
		return nil, err
	}

	data := make([]map[string]any, 0)

	fsm_size, err := tm.FileManager.GetFileSize(tableName + ".fsm")
	if err != nil {
		return nil, err
	}
	fsm_binary_data, err := tm.FileManager.Read(tableName+".fsm", 0, fsm_size)
	if err != nil {
		return nil, err
	}
	fsm_data := DeserializeFSM(fsm_binary_data)
	pages_count := len(fsm_data)

	empty_free := PageSize - 8

	for i := 1; i <= pages_count; i++ {
		fsm_free := int(fsm_data[i-1])
		if fsm_free >= empty_free {
			continue
		}

		offsetBytes := int64((i - 1) * PageSize)
		page, err := tm.FileManager.Read(tableName+".table", offsetBytes, int64(PageSize))
		if err != nil {
			return nil, err
		}

		record_count := uint16(binary.LittleEndian.Uint16(page[0:2]))
		offset := PageSize
		for record_count > 0 {
			record_offset := uint16(binary.LittleEndian.Uint16(page[offset-2 : offset]))
			offset -= 2
			record_length := uint16(binary.LittleEndian.Uint16(page[offset-2 : offset]))
			columnProjection := BuildColumnProjection(schema, filters, selectedColumns)
			rec := DeserializeRecord(schema, page[record_offset:record_offset+record_length], columnProjection)
			if rec != nil {
				row := make(map[string]any)
				itemIndex := 0
				for colIndex := 0; colIndex < len(schema.Columns); colIndex++ {
					if columnProjection[colIndex].IsProjected {
						row[columnProjection[colIndex].Name] = rec.Items[itemIndex].Literal
						itemIndex++
					}
				}
				data = append(data, row)
			}
			offset -= 2
			record_count--
		}
	}

	return data, nil

}

func (tm *TableManager) FindOrCreatePage(tableName string, record []byte) (page []byte, page_order int, err error) {
	record_size := uint16(len(record))
	fsm_size, err := tm.FileManager.GetFileSize(tableName + ".fsm")

	if err != nil {
		return nil, 0, err
	}

	fsm_binary_data, err := tm.FileManager.Read(tableName+".fsm", 0, fsm_size)

	if err != nil {
		return nil, 0, err
	}

	pages_count := int(len(fsm_binary_data) / 2)
	table_size, err := tm.FileManager.GetFileSize(tableName + ".table")
	if err != nil {
		return nil, 0, err
	}
	table_pages := int(table_size / PageSize)
	if table_pages != pages_count {
		return nil, 0, errors.New("fsm data is not compatible with table")
	}

	fsm_data := DeserializeFSM(fsm_binary_data)

	for i := 1; i <= pages_count; i++ {
		fsm_free := int(fsm_data[i-1])
		if fsm_free < int(record_size)+4 {
			continue
		}

		offset := int64((i - 1) * PageSize)
		page, err = tm.FileManager.Read(tableName+".table", offset, int64(PageSize))
		if err != nil {
			return nil, 0, err
		}

		record_count := uint16(binary.LittleEndian.Uint16(page[:2]))
		free_space_pointer := uint16(binary.LittleEndian.Uint16(page[2:4]))
		actual_free := PageSize - ((int(record_count)+1)*4 + int(free_space_pointer))

		if actual_free != fsm_free {
			return nil, 0, errors.New("fsm and page free space mismatch")
		}

		slot_beginning_address := (PageSize - int(record_count)*4) - 4
		record_count++
		binary.LittleEndian.PutUint16(page[:2], uint16(record_count))
		copy(page[free_space_pointer:], record)
		binary.LittleEndian.PutUint16(page[slot_beginning_address+2:slot_beginning_address+4], uint16(free_space_pointer))
		binary.LittleEndian.PutUint16(page[slot_beginning_address:slot_beginning_address+2], record_size)

		free_space_pointer += record_size
		binary.LittleEndian.PutUint16(page[2:4], uint16(free_space_pointer))

		new_free := fsm_free - (int(record_size) + 4)
		if new_free < 0 {
			new_free = 0
		}
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, uint16(new_free))
		if err := tm.FileManager.Write(tableName+".fsm", int64((i-1)*2), buf); err != nil {
			return nil, 0, err
		}

		page_order = i
		return page, page_order, nil
	}

	page = make([]byte, PageSize)
	binary.LittleEndian.PutUint16(page[:2], uint16(1))
	binary.LittleEndian.PutUint16(page[2:4], uint16(4+len(record)))
	copy(page[4:], record)
	binary.LittleEndian.PutUint16(page[PageSize-4:PageSize-2], uint16(len(record)))
	binary.LittleEndian.PutUint16(page[PageSize-2:PageSize], uint16(4))

	remaining_free := PageSize - (12 + len(record))
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, uint16(remaining_free))
	if err := tm.FileManager.Write(tableName+".fsm", int64(len(fsm_binary_data)), buf); err != nil {
		return nil, 0, err
	}

	return page, pages_count + 1, nil
}

func BuildColumnProjection(schema Schema, filters []Filter, selectedColumns SelectedColumns) map[int]ColumnProjection {
	filteredCols := make(map[string]bool)
	filterValues := make(map[string]any)
	filterOperator := make(map[string]string)
	for _, filter := range filters {
		filteredCols[filter.Column] = true
		filterValues[filter.Column] = filter.Value
		filterOperator[filter.Column] = filter.Operator
	}

	projectedCols := make(map[string]bool)
	for _, colName := range selectedColumns.Columns {
		projectedCols[colName] = true
	}

	projection := make(map[int]ColumnProjection)
	for i, column := range schema.Columns {
		isFiltered := filteredCols[column.Name]
		isProjected := projectedCols[column.Name]

		projection[i] = ColumnProjection{
			Name:           column.Name,
			Index:          i,
			IsFiltered:     isFiltered,
			IsProjected:    isProjected,
			MustExtract:    isFiltered || isProjected,
			FilterValue:    filterValues[column.Name],
			FilterOperator: filterOperator[column.Name],
		}
	}

	return projection
}
