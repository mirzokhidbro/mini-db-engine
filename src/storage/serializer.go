package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"
)

func SerializeSchema(schema *Schema) []byte {
	size := 2
	for _, col := range schema.Columns {
		size += 2 + len(col.Name)
		size += 2
		size += 2
	}

	buf := make([]byte, size)
	offset := 0

	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(schema.Columns)))
	offset += 2

	for _, col := range schema.Columns {
		binary.LittleEndian.PutUint16(buf[offset:], uint16(len(col.Name)))
		offset += 2

		copy(buf[offset:], []byte(col.Name))
		offset += len(col.Name)

		binary.LittleEndian.PutUint16(buf[offset:], uint16(col.Type))
		offset += 2

		binary.LittleEndian.PutUint16(buf[offset:], uint16(col.Length))
		offset += 2
	}

	return buf
}

func DeserializeSchema(schema []byte) Schema {
	columns := []Column{}
	offset := 2
	column_count := binary.LittleEndian.Uint16(schema[:offset])

	for column_count > 0 {
		name_length := binary.LittleEndian.Uint16(schema[offset : offset+2])
		offset += 2

		column_name := schema[offset : uint16(offset)+name_length]
		offset += int(name_length)

		column_type := binary.LittleEndian.Uint16(schema[offset : offset+2])
		offset += 2

		column_capacity := binary.LittleEndian.Uint16(schema[offset : offset+2])
		offset += 2

		columns = append(columns, Column{Name: string(column_name), Type: ColumnType(column_type), Length: int(column_capacity)})
		column_count--
	}

	return Schema{columns}
}

func SerializeRecord(schema Schema, record Record) []byte {
	column_count := len(schema.Columns)
	var buf bytes.Buffer

	for i := 0; i < column_count; i++ {
		switch schema.Columns[i].Type {
		case TypeInt: // integer
			literal, ok := record.Items[i].Literal.(int)
			if ok {
				binary.Write(&buf, binary.LittleEndian, int64(literal))
			} else {
				fmt.Println("invalid data type for integer")
			}
		case TypeVarchar: // varchar
			literal, ok := record.Items[i].Literal.(string)
			if ok {
				literal_length := len(literal)
				binary.Write(&buf, binary.LittleEndian, int16(literal_length))
				buf.Write([]byte(literal))
			} else {
				fmt.Println("invalid data type for string")
			}
		case TypeDate: // date
			s, ok := record.Items[i].Literal.(string)
			if ok {
				if d, err := daysFromDateString(s); err == nil {
					binary.Write(&buf, binary.LittleEndian, int32(d))
				} else {
					fmt.Println("invalid date format")
				}
			} else {
				fmt.Println("invalid data type for date")
			}
		case TypeTimestamp: // timestamp
			s, ok := record.Items[i].Literal.(string)
			if ok {
				if us, err := microsFromTimestampString(s); err == nil {
					binary.Write(&buf, binary.LittleEndian, int64(us))
				} else {
					fmt.Println("invalid timestamp format")
				}
			} else {
				fmt.Println("invalid data type for timestamp")
			}
		case TypeFloat: // float
			f, ok := record.Items[i].Literal.(float64)
			if ok {
				binary.Write(&buf, binary.LittleEndian, f)
			} else {
				fmt.Println("invalid data type for float")
			}
		case TypeJSON:
			s, ok := record.Items[i].Literal.(string)
			if ok {
				binary.Write(&buf, binary.LittleEndian, int16(len(s)))
				buf.WriteString(s)
			} else {
				fmt.Println("invalid data type for json")
			}
		}
	}

	return buf.Bytes()
}

// TODO: Extract filtering logic into a separate method
func DeserializeRecord(schema Schema, data []byte, columnProjection map[int]ColumnProjection) *Record {
	offset := 0
	items := make([]Item, 0, len(schema.Columns))

	for i := 0; i < len(schema.Columns); i++ {
		must_extract := columnProjection[i].MustExtract
		is_filtered := columnProjection[i].IsFiltered
		is_projected := columnProjection[i].IsProjected

		switch schema.Columns[i].Type {
		case TypeInt:
			if must_extract {
				val := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
				if is_filtered {
					filterValue, ok := columnProjection[i].FilterValue.(int64)
					if !ok {
						return nil
					}
					operator := columnProjection[i].FilterOperator
					matches := false
					switch operator {
					case string(OpEq):
						matches = (val == filterValue)
					case string(OpNe):
						matches = (val != filterValue)
					}
					if !matches {
						return nil
					}
				}
				if is_projected {
					items = append(items, Item{Literal: val})
				}
			}
			offset += 8
		case TypeVarchar: // varchar
			strlen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
			offset += 2
			if must_extract {
				str := string(data[offset : offset+strlen])
				if is_filtered {
					filterValue, ok := columnProjection[i].FilterValue.(string)
					if !ok {
						return nil
					}
					operator := columnProjection[i].FilterOperator
					matches := false
					switch operator {
					case string(OpEq):
						matches = (str == filterValue)
					case string(OpNe):
						matches = (str != filterValue)
					}
					if !matches {
						return nil
					}
				}
				if is_projected {
					items = append(items, Item{Literal: str})
				}
			}

			offset += strlen
		case TypeDate: // date
			if must_extract {
				v := int32(binary.LittleEndian.Uint32(data[offset : offset+4]))
				dateStr := dateStringFromDays(v)
				if is_filtered {
					filterValue, ok := columnProjection[i].FilterValue.(string)
					if !ok {
						return nil
					}
					operator := columnProjection[i].FilterOperator
					matches := false
					switch operator {
					case string(OpEq):
						matches = (dateStr == filterValue)
					case string(OpNe):
						matches = (dateStr != filterValue)
					}
					if !matches {
						return nil
					}
				}
				if is_projected {
					items = append(items, Item{Literal: dateStr})
				}
			}
			offset += 4
		case TypeTimestamp: // timestamp
			if must_extract {
				v := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
				timestampStr := timestampStringFromMicros(v)
				if is_filtered {
					filterValue, ok := columnProjection[i].FilterValue.(string)
					if !ok {
						return nil
					}
					operator := columnProjection[i].FilterOperator
					matches := false
					switch operator {
					case string(OpEq):
						matches = (timestampStr == filterValue)
					case string(OpNe):
						matches = (timestampStr != filterValue)
					}
					if !matches {
						return nil
					}
				}
				if is_projected {
					items = append(items, Item{Literal: timestampStr})
				}
			}
			offset += 8
		case TypeFloat: // float
			if must_extract {
				bits := binary.LittleEndian.Uint64(data[offset : offset+8])
				f := math.Float64frombits(bits)
				if is_filtered {
					filterValue, ok := columnProjection[i].FilterValue.(float64)
					if !ok {
						return nil
					}
					operator := columnProjection[i].FilterOperator
					matches := false
					switch operator {
					case string(OpEq):
						matches = (f == filterValue)
					case string(OpNe):
						matches = (f != filterValue)
					}
					if !matches {
						return nil
					}
				}
				if is_projected {
					items = append(items, Item{Literal: f})
				}
			}
			offset += 8
		case TypeJSON: // json
			strlen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
			offset += 2
			if is_projected {
				raw := data[offset : offset+strlen]
				var v any
				if err := json.Unmarshal(raw, &v); err != nil {
					items = append(items, Item{Literal: string(raw)})
				} else {
					items = append(items, Item{Literal: v})
				}
			}
			offset += strlen
		}
	}

	if len(items) == 0 {
		return nil
	}

	return &Record{Items: items}
}

// bu yerda child bilan value qismiga aniq joy ajratib berilishi kerak
func SerializeIndexNode(node Node) []byte {
	var buf bytes.Buffer

	binary.Write(&buf, binary.LittleEndian, node.NodeType) // 1 byte
	binary.Write(&buf, binary.LittleEndian, node.KeyCount) // 1 byte

	for _, ptr := range node.ChildPointers {
		binary.Write(&buf, binary.LittleEndian, ptr)
	}

	for _, entry := range node.Values {
		binary.Write(&buf, binary.LittleEndian, entry.Value)
		binary.Write(&buf, binary.LittleEndian, entry.RecordListPointer)
	}

	return buf.Bytes()
}

func DeserializeIndexNode(data []byte) Node {
	node := Node{}
	offset := 0

	node.NodeType = NodeType(data[offset])
	offset++

	node.KeyCount = int8(data[offset])
	offset++

	if node.NodeType != NodeTypeLeaf && node.NodeType != NodeTypeRootNoChild {
		childCount := int(node.KeyCount) + 1
		node.ChildPointers = make([]uint64, childCount)

		for i := 0; i < childCount; i++ {
			node.ChildPointers[i] = binary.LittleEndian.Uint64(data[offset : offset+8])
			offset += 8
		}
	}

	node.Values = make([]ValueEntry, node.KeyCount)
	for i := 0; i < int(node.KeyCount); i++ {
		node.Values[i].Value = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8
		node.Values[i].RecordListPointer = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8
	}

	return node
}

func SerializeIndexHeader(header NodeHeader) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, header.RootPointer)
	binary.Write(&buf, binary.LittleEndian, header.FreeSpacePointer)
	binary.Write(&buf, binary.LittleEndian, header.NodeCount)
	binary.Write(&buf, binary.LittleEndian, header.Height)

	return buf.Bytes()
}

func DeserializeIndexHeader(data []byte) NodeHeader {
	node_type_header := NodeHeader{}

	node_type_header.RootPointer = binary.LittleEndian.Uint64(data[0:8])
	node_type_header.FreeSpacePointer = binary.LittleEndian.Uint64(data[8:16])
	node_type_header.NodeCount = binary.LittleEndian.Uint64(data[16:24])
	node_type_header.Height = binary.LittleEndian.Uint64(data[24:32])

	return node_type_header
}

func DeserializeFSM(data []byte) []uint16 {
	if len(data)%2 != 0 {
		return []uint16{}
	}
	count := len(data) / 2
	res := make([]uint16, count)
	off := 0
	for i := 0; i < count; i++ {
		res[i] = binary.LittleEndian.Uint16(data[off : off+2])
		off += 2
	}
	return res
}

func SerializeFSM(size int16) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, size)
	return buf.Bytes()
}

func daysFromDateString(s string) (int32, error) {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return 0, err
	}
	secs := t.UTC().Unix()
	return int32(secs / 86400), nil
}

func dateStringFromDays(d int32) string {
	t := time.Unix(int64(d)*86400, 0).UTC()
	return t.Format("2006-01-02")
}

func microsFromTimestampString(s string) (int64, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0, err
	}
	t = t.UTC()
	return t.Unix()*1000000 + int64(t.Nanosecond()/1000), nil
}

func timestampStringFromMicros(us int64) string {
	sec := us / 1000000
	usec := us % 1000000
	t := time.Unix(sec, usec*1000).UTC()
	return t.Format(time.RFC3339Nano)
}
