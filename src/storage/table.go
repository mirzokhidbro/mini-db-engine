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
	Name      string
	Type      ColumnType
	Length    int
	IsIndexed bool
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
	UseIndex    bool
}

type SelectedColumns struct {
	Columns []string
}

type ColumnProjection struct {
	Name            string
	Index           int
	IsFiltered      bool
	IsProjected     bool
	MustExtract     bool
	FilterValue     any
	FilterOperator  string
	IsIndexedColumn bool
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
	GetDataByLocation(tableName string, schema Schema, locations []RecordLocation, columnProjection map[int]ColumnProjection) []map[string]any

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

	CanUseIndexForFilters(tableName string, schema Schema, filters []Filter) (bool, []Filter)
	GetRecordLocationsFromIndex(tableName string, filter Filter) ([]RecordLocation, error)
	IntersectRecordLocations(locationSets [][]RecordLocation) []RecordLocation
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

	for _, col := range schema.Columns {
		if col.Type == TypeInt && col.IsIndexed {
			if err := tm.CreateIndex(name, col.Name); err != nil {
				return fmt.Errorf("failed to create index for column %s: %w", col.Name, err)
			}
		}
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

	page, page_order, record_id, err := tm.FindOrCreatePage(tableName, serialized_record)

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

	for i, col := range schema.Columns {
		if col.IsIndexed && col.Type == TypeInt {
			value, ok := record.Items[i].Literal.(int)
			if !ok {
				value64, ok := record.Items[i].Literal.(int64)
				if !ok {
					continue
				}
				value = int(value64)
			}

			location := RecordLocation{
				PageID:   int64(page_order),
				RecordID: record_id,
			}

			indexName := tableName + "_" + col.Name + ".index"
			if err := tm.InsetValueToIndex(indexName, int64(value), location); err != nil {
				return fmt.Errorf("failed to update index for column %s: %w", col.Name, err)
			}
		}
	}

	return nil
}

func (tm *TableManager) GetAllData(tableName string, filters []Filter, selectedColumns SelectedColumns) ([]map[string]any, error) {
	schema, err := tm.GetTableSchema(tableName + ".schema")
	if err != nil {
		return nil, err
	}

	canUseIndex, indexFilters := tm.CanUseIndexForFilters(tableName, schema, filters)

	columnProjection := BuildColumnProjection(schema, indexFilters, selectedColumns)

	if canUseIndex {

		locations, err := tm.getRecordLocationsFromIndexedFilters(tableName, indexFilters)

		if err != nil {
			panic(err)
		}

		return tm.GetDataByLocation(tableName, schema, locations, columnProjection), nil
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

func (tm *TableManager) GetDataByLocation(tableName string, schema Schema, locations []RecordLocation, columnProjection map[int]ColumnProjection) []map[string]any {
	data := make([]map[string]any, 0)

	for _, location := range locations {
		offset := int64((location.PageID - 1) * PageSize)
		page, err := tm.FileManager.Read(tableName+".table", offset, int64(PageSize))

		if err != nil {
			fmt.Println(err)
			continue
		}

		slotOffset := PageSize - 4*int(location.RecordID)

		recordLength := binary.LittleEndian.Uint16(page[slotOffset : slotOffset+2])
		recordBeginningAddress := binary.LittleEndian.Uint16(page[slotOffset+2 : slotOffset+4])

		record := DeserializeRecord(schema, page[recordBeginningAddress:recordBeginningAddress+recordLength], columnProjection)

		if record != nil {
			row := make(map[string]any)
			itemIndex := 0
			for colIndex := 0; colIndex < len(schema.Columns); colIndex++ {
				if columnProjection[colIndex].IsProjected {
					row[columnProjection[colIndex].Name] = record.Items[itemIndex].Literal
					itemIndex++
				}
			}
			data = append(data, row)
		}
	}

	return data
}

func (tm *TableManager) FindOrCreatePage(tableName string, record []byte) (page []byte, page_order int, record_id int16, err error) {
	record_size := uint16(len(record))
	fsm_size, err := tm.FileManager.GetFileSize(tableName + ".fsm")

	if err != nil {
		return nil, 0, 0, err
	}

	fsm_binary_data, err := tm.FileManager.Read(tableName+".fsm", 0, fsm_size)

	if err != nil {
		return nil, 0, 0, err
	}

	pages_count := int(len(fsm_binary_data) / 2)
	table_size, err := tm.FileManager.GetFileSize(tableName + ".table")
	if err != nil {
		return nil, 0, 0, err
	}
	table_pages := int(table_size / PageSize)
	if table_pages != pages_count {
		return nil, 0, 0, errors.New("fsm data is not compatible with table")
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
			return nil, 0, 0, err
		}

		record_count := uint16(binary.LittleEndian.Uint16(page[:2]))
		free_space_pointer := uint16(binary.LittleEndian.Uint16(page[2:4]))
		// Calculate actual free space: (record_count + 1) * 4 is used because
		// each record needs 4 bytes for metadata (2 bytes for offset, 2 bytes for length)
		// and +1 accounts for the metadata of the new record being inserted
		actual_free := PageSize - ((int(record_count)+1)*4 + int(free_space_pointer))

		if actual_free != fsm_free {
			return nil, 0, 0, errors.New("fsm and page free space mismatch")
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
			return nil, 0, 0, err
		}

		page_order = i
		return page, page_order, int16(record_count), nil
	}

	page = make([]byte, PageSize)
	binary.LittleEndian.PutUint16(page[:2], uint16(1))              // record count
	binary.LittleEndian.PutUint16(page[2:4], uint16(4+len(record))) // free space pointer
	copy(page[4:], record)
	binary.LittleEndian.PutUint16(page[PageSize-4:PageSize-2], uint16(len(record))) // record length
	binary.LittleEndian.PutUint16(page[PageSize-2:PageSize], uint16(4))             // record beginning address

	remaining_free := PageSize - (12 + len(record))
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, uint16(remaining_free))
	if err := tm.FileManager.Write(tableName+".fsm", int64(len(fsm_binary_data)), buf); err != nil {
		return nil, 0, 0, err
	}

	// Return pages_count + 1 because we obtained the current page count above
	// and we just added a new page to the table
	return page, pages_count + 1, 1, nil
}

func BuildColumnProjection(schema Schema, filters []Filter, selectedColumns SelectedColumns) map[int]ColumnProjection {
	filteredCols := make(map[string]bool)
	filterValues := make(map[string]any)
	filteringIndexedColumns := make(map[string]any)
	filterOperator := make(map[string]string)
	for _, filter := range filters {
		filteredCols[filter.Column] = true
		filterValues[filter.Column] = filter.Value
		filteringIndexedColumns[filter.Column] = filter.UseIndex
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
		isIndexedColumn, _ := filteringIndexedColumns[column.Name].(bool)

		projection[i] = ColumnProjection{
			Name:            column.Name,
			Index:           i,
			IsFiltered:      isFiltered,
			IsProjected:     isProjected,
			MustExtract:     isFiltered || isProjected,
			FilterValue:     filterValues[column.Name],
			FilterOperator:  filterOperator[column.Name],
			IsIndexedColumn: isIndexedColumn,
		}
	}

	return projection
}

func (tm *TableManager) CanUseIndexForFilters(tableName string, schema Schema, filters []Filter) (bool, []Filter) {
	hasIndexedFilter := false

	for i := range filters {
		filter := &filters[i]
		if filter.Operator == "=" {
			for _, col := range schema.Columns {
				if col.Name == filter.Column && col.IsIndexed && col.Type == TypeInt {
					indexName := tableName + "_" + col.Name + ".index"
					if tm.FileManager.FileExists(indexName) {
						filter.UseIndex = true
						hasIndexedFilter = true
						break
					}
				}
			}
		}
	}

	return hasIndexedFilter, filters
}

func (tm *TableManager) GetRecordLocationsFromIndex(tableName string, filter Filter) ([]RecordLocation, error) {
	value, ok := filter.Value.(int)
	if !ok {
		value64, ok := filter.Value.(int64)
		if !ok {
			return nil, fmt.Errorf("unsupported value type for index: %T", filter.Value)
		}
		value = int(value64)
	}

	indexName := tableName + "_" + filter.Column + ".index"
	recordListHead, found, err := tm.SearchValue(indexName, int64(value))
	if err != nil {
		return nil, fmt.Errorf("index search failed: %w", err)
	}

	if !found {
		return []RecordLocation{}, nil
	}

	recordListFileName := tableName + "_" + filter.Column + ".record_list_file"
	locations, err := tm.GetRecordsByValue(recordListFileName, recordListHead)
	if err != nil {
		return nil, fmt.Errorf("failed to get record locations: %w", err)
	}

	return locations, nil
}

func (tm *TableManager) IntersectRecordLocations(locationSets [][]RecordLocation) []RecordLocation {
	if len(locationSets) == 0 {
		return []RecordLocation{}
	}

	if len(locationSets) == 1 {
		return locationSets[0]
	}

	locationCount := make(map[string]int)
	locationMap := make(map[string]RecordLocation)

	for _, locations := range locationSets {
		seen := make(map[string]bool)
		for _, loc := range locations {
			key := fmt.Sprintf("%d-%d", loc.PageID, loc.RecordID)
			if !seen[key] {
				locationCount[key]++
				locationMap[key] = loc
				seen[key] = true
			}
		}
	}

	result := []RecordLocation{}
	requiredCount := len(locationSets)
	for key, count := range locationCount {
		if count == requiredCount {
			result = append(result, locationMap[key])
		}
	}

	return result
}

func (tm *TableManager) getRecordLocationsFromIndexedFilters(tableName string, filters []Filter) ([]RecordLocation, error) {
	var locationSets [][]RecordLocation

	for _, filter := range filters {
		if filter.UseIndex {
			locations, err := tm.GetRecordLocationsFromIndex(tableName, filter)
			if err != nil {
				return nil, err
			}
			locationSets = append(locationSets, locations)
		}
	}

	if len(locationSets) == 0 {
		return []RecordLocation{}, nil
	}

	combinedLocations := tm.IntersectRecordLocations(locationSets)

	return combinedLocations, nil
}
