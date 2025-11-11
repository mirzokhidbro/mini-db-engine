package main

import (
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"rdbms/src"
	"rdbms/src/storage"
	"rdbms/utils"
	"strings"
	"time"
)

func main() {
	stg, err := src.NewStorage("data")
	if err != nil {
		panic(err)
	}
	stg.Table().CreateIndex("test_data", "score")
	// seedUsers(stg)
	// seedTestData(stg)
}

// func typeAsserter(data any) {
// 	value, ok := int64(data)
// 	fmt.Println(data)
// 	fmt.Println(ok)
// 	fmt.Println(value)
// }

func seedTestData(storage_db src.StorageI) {
	// Create test table with int and float columns
	columns := []storage.Column{
		{Name: "id", Type: storage.TypeInt},
		{Name: "score", Type: storage.TypeInt},
		{Name: "rating", Type: storage.TypeFloat},
		{Name: "price", Type: storage.TypeFloat},
	}
	schema := storage.Schema{Columns: columns}

	err := storage_db.Table().CreateTable("test_data", &schema)
	if err != nil {
		fmt.Printf("Table creation error (might exist): %v\n", err)
	} else {
		fmt.Println("✓ Table 'test_data' created")
	}

	// Seed random data
	rand.Seed(time.Now().UnixNano())
	N := 10000 // 10K records for testing
	start := time.Now()

	fmt.Printf("Inserting %d random records...\n", N)

	for i := 1; i <= N; i++ {
		// Random score: 0-1000
		score := rand.Intn(1001)

		// Random rating: 0.0-5.0
		rating := rand.Float64() * 5.0

		// Random price: 10.0-10000.0
		price := 10.0 + rand.Float64()*9990.0

		items := []storage.Item{
			{Literal: i},
			{Literal: score},
			{Literal: rating},
			{Literal: price},
		}
		record := storage.Record{Items: items}

		if err := storage_db.Table().Insert("test_data", record); err != nil {
			fmt.Printf("Insert error at record %d: %v\n", i, err)
			return
		}

		if i%1000 == 0 {
			fmt.Printf("  Inserted %d records...\n", i)
		}
	}

	elapsed := time.Since(start).Seconds()
	rate := 0.0
	if elapsed > 0 {
		rate = float64(N) / elapsed
	}
	fmt.Printf("✓ Inserted %d records in %.2f seconds (%.0f req/s)\n", N, elapsed, rate)
}

func seedUsers(storage_db src.StorageI) {
	cities := []string{
		"chicago", "berlin", "dublin", "new york", "tashkent", "astana", "osh", "london", "praga",
		"paris", "madrid", "rome", "vienna", "amsterdam", "brussels", "zurich", "geneva", "barcelona",
		"seoul", "tokyo", "osaka", "kyoto", "beijing", "shanghai", "hong kong", "singapore", "bangkok",
		"sydney", "melbourne", "auckland", "toronto", "vancouver", "montreal", "boston", "los angeles",
		"san francisco", "seattle", "houston", "miami", "mexico city", "sao paulo", "buenos aires",
		"cape town", "johannesburg", "nairobi", "cairo", "istanbul", "ankara", "athens", "sofia",
		"warsaw", "krakow", "prague", "budapest", "copenhagen", "stockholm", "oslo", "helsinki",
		"dubai", "abu dhabi", "doha", "riyadh", "tehran", "karachi", "delhi", "mumbai", "bangalore",
	}
	rand.Seed(time.Now().UnixNano())
	N := 100000
	start := time.Now()
	for i := 1; i <= N; i++ {
		b := make([]byte, 4)
		if _, err := crand.Read(b); err != nil {
			fmt.Println(err)
			return
		}
		username := "user" + hex.EncodeToString(b)
		bioBase := strings.Repeat("loremipsum", 50)
		bioLen := 64 + rand.Intn(256-64+1)
		if bioLen > len(bioBase) {
			bioLen = len(bioBase)
		}
		items := []storage.Item{
			{Literal: i},
			{Literal: username},
			{Literal: username + "@example.com"},
			{Literal: cities[rand.Intn(len(cities))]},
			{Literal: bioBase[:bioLen]},
		}
		record := storage.Record{Items: items}
		if err := storage_db.Table().Insert("users", record); err != nil {
			fmt.Println(err)
			return
		}
	}
	elapsed := time.Since(start).Seconds()
	rate := 0.0
	if elapsed > 0 {
		rate = float64(N) / elapsed
	}
	fmt.Printf("Inserted %d records in %.2f seconds (%.0f req/s)\n", N, elapsed, rate)
}

func serializeFSM() []byte {
	i := 8190
	var bytes []byte
	for i > 0 {
		data := storage.SerializeFSM(int16(i))
		i--
		bytes = append(bytes, data...)
	}

	return bytes
}

func fileManager() {
	path := "data"
	entries, err := os.ReadDir(path)
	if err != nil {
		panic(err)
	}

	files := make(map[string]*os.File)

	for _, e := range entries {
		if !e.IsDir() {
			fullPath := filepath.Join(path, e.Name())
			f, err := os.OpenFile(fullPath, os.O_RDWR, 0666)
			if err != nil {
				panic(err)
			}

			fileName := filepath.Base(f.Name())

			fmt.Println(filepath.Base(f.Name()))

			files[fileName] = f
		}
	}

	// fmt.Println(getTableSchema(files["users.schema"]))

}

func SetFilterColumnIndexes(schema storage.Schema, filters []storage.Filter) []storage.Filter {
	schema_map := make(map[string]int)
	for column_index, column := range schema.Columns {
		schema_map[column.Name] = column_index
	}

	for filter_index, filter := range filters {
		i, ok := schema_map[filter.Column]
		if !ok {
			panic("filters are not eligeble")
		}
		filters[filter_index].ColumnIndex = i
	}

	return filters
}

func createTable(name string, tableManager storage.TableManager) {
	columns := []storage.Column{
		{Name: "firstname", Type: storage.TypeVarchar, Length: 255},
		{Name: "lastname", Type: storage.TypeVarchar, Length: 255},
		{Name: "dream", Type: storage.TypeVarchar, Length: 255},
		{Name: "age", Type: storage.TypeInt},
	}
	schema := storage.Schema{Columns: columns}

	tableManager.CreateTable(name, &schema)
}

func getTableSchema(schemaName string, stg src.StorageI) storage.Schema {
	schema, err := stg.Table().GetTableSchema(schemaName)

	if err != nil {
		fmt.Println("error")
		fmt.Println(err)
	}

	return schema
}

func insertTable(storage_db src.StorageI, tableName string) {
	items := []storage.Item{
		{Literal: "Alijonov."},
		{Literal: "Botirali. "},
		{Literal: "Lorem "},
		{Literal: 31},
	}
	// Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.
	record := storage.Record{Items: items}
	err := storage_db.Table().Insert(tableName, record)
	if err != nil {
		fmt.Println("error")
		fmt.Println(err)
	}
	// table.Insert("users.table", record)
}

func getBinaryRecord(schema storage.Schema) []byte {
	items := []storage.Item{
		{Literal: "Jasur a kjdnij. eosineow oew jfoewj fowej foewj fiwej fiewj fiwejf weijf ewijf ewijf iwej"},
		{Literal: "ikromov ifjenwijf wf iewf iwej fiewj fiwej fiwe fiwej fiwe fjiwej fiew jfiewj fiew jfiwej feiwf iwefijwe fiwe"},
		{Literal: " oekw owe foew foe wfokew fowe fowe fowej fowej few foewk foew few foe foekw foe wofw eofjew fowfowe fow "},
		{Literal: 82000},
	}

	record := storage.Record{Items: items}
	return storage.SerializeRecord(schema, record)
}

func getAllData(stg src.StorageI, tableName string) {
	filters := []storage.Filter{
		{Column: "username", Operator: "=", Value: "userefedf4f0"},
		{Column: "id", Operator: "=", Value: 1},
	}

	schema := getTableSchema("users.schema", stg)
	fmt.Println("schema")
	fmt.Println(schema)
	filters, err := utils.SetFilterColumnIndexes(schema, filters)

	if err != nil {
		panic(err)
	}

	selectedColumns := storage.SelectedColumns{Columns: []string{"id", "username", "email", "bio"}}
	data, err := stg.Table().GetAllData(tableName, filters, selectedColumns)

	if err != nil {
		fmt.Println(err)
	}

	for _, row := range data {
		fmt.Println(row)
	}

}

func getFreePage(table storage.TableManager) {
	file_size, err := table.FileManager.GetFileSize("table")
	if err != nil {
		fmt.Println(err.Error())
	}

	fmt.Println("file size")
	fmt.Println(file_size)
}
