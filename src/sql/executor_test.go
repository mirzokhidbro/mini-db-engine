package sql

import (
	"testing"

	"rdbms/src"
)

func TestExecutor(t *testing.T) {
	dataDir := t.TempDir()

	stg, err := src.NewStorage(dataDir)
	if err != nil {
		t.Fatal(err)
	}

	exec := NewExecutor(stg)

	_, err = exec.Execute("CREATE TABLE users (id INT, name VARCHAR(50), age INT)")
	if err != nil {
		t.Fatal(err)
	}

	res, err := exec.Execute("INSERT INTO users (id, name, age) VALUES (1, 'Linus', 20)")
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || res.AffectedRows != 1 {
		t.Fatalf("expected 1 row inserted, got %+v", res)
	}

	res, err = exec.Execute("SELECT id, name FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || len(res.Data) != 1 {
		t.Fatalf("expected 1 row, got %+v", res)
	}

	row := res.Data[0]
	if row["name"] != "Linus" {
		t.Fatalf("expected name=Linus, got %v", row["name"])
	}
	if row["id"] != int64(1) {
		t.Fatalf("expected id=1, got %v (%T)", row["id"], row["id"])
	}

	res, err = exec.Execute("UPDATE users SET name = 'bob' WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || res.AffectedRows != 1 {
		t.Fatalf("expected 1 row updated, got %+v", res)
	}

	res, err = exec.Execute("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || res.AffectedRows != 1 {
		t.Fatalf("expected 1 row deleted, got %+v", res)
	}

	res, err = exec.Execute("SELECT id, name FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("expected non-nil result")
	}
	if len(res.Data) != 0 {
		t.Fatalf("expected 0 rows after delete, got %d", len(res.Data))
	}
}
