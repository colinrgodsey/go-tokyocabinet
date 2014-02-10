package tokyocabinet

import "bytes"
import "io/ioutil"
import "os"
import "testing"

func hdb_assertOpen(t *testing.T, filename string, flags int) HDB {
	var db HDB = *NewHDB()
	if len(filename) == 0 {
		tf, err := ioutil.TempFile("", "tctest")
		if err != nil {
			t.Fatalf("Unable to create temporary file: %s", err)
		}
		filename = tf.Name()
	}
	err := db.Open(filename, flags)
	os.Remove(filename)
	if err != nil {
		t.Fatalf("Unable to open %s: %s", filename, err)
	}
	return db
}

func hdb_assertClose(t *testing.T, db HDB) {
	err := db.Close()
	if err != nil {
		t.Fatalf("Unable to close database: %s", err)
	}
}

func hdb_assertPut(t *testing.T, db HDB, key string, value string) {
	err := db.Put([]byte(key), []byte(value))
	if err != nil {
		t.Fatalf("Unable to assign key %s with value %s: %s", key, value, err)
	}
}

func hdb_assertPutCat(t *testing.T, db HDB, key string, value string) {
	err := db.PutCat([]byte(key), []byte(value))
	if err != nil {
		t.Fatalf("Unable to assign key %s with value %s: %s", key, value, err)
	}
}

func hdb_assertPutKeep(t *testing.T, db HDB, key string, value string) {
	err := db.PutKeep([]byte(key), []byte(value))
	if err != nil {
		t.Fatalf("Unable to assign key %s with value %s: %s", key, value, err)
	}
}

func hdb_assertPutAsync(t *testing.T, db HDB, key string, value string) {
	err := db.PutAsync([]byte(key), []byte(value))
	if err != nil {
		t.Fatalf("Unable to assign key %s with value %s: %s", key, value, err)
	}
}

func hdb_assertGetValue(t *testing.T, db HDB, key string, expected string) {
	value, err := db.Get([]byte(key))
	if err != nil {
		t.Fatalf("Unable to retrieve value for key %s: %s", key, err)
	}
	if bytes.Compare([]byte(expected), value) != 0 {
		t.Fatalf("Value for key %s came back incorrect (expected: %s; got: %s)", key, []byte(expected), value)
	}
}

func hdb_assertGetSize(t *testing.T, db HDB, key string) (size int) {
	size, err := db.Size([]byte(key))
	if err != nil {
		t.Fatalf("Unable to retrieve size of key %s: %s", key, err)
	}
	return
}

func hdb_assertSync(t *testing.T, db HDB) {
	err := db.Sync()
	if err != nil {
		t.Fatalf("Unable to sync database: %s", err)
	}
}

func hdb_assertAddInt(t *testing.T, db HDB, key string, increment int, desiredNewValue int) {
	newValue, err := db.AddInt([]byte(key), increment)
	if err != nil {
		t.Fatalf("Unable to add integer to key: %s", err)
	}
	if newValue != desiredNewValue {
		t.Fatalf("Unexpected value for %s: desired %d, got %d", key, newValue, desiredNewValue)
	}
}

func hdb_assertAddDouble(t *testing.T, db HDB, key string, increment float64, desiredNewValue float64) {
	newValue, err := db.AddDouble([]byte(key), increment)
	if err != nil {
		t.Fatalf("Unable to add integer to key: %s", err)
	}
	if newValue != desiredNewValue {
		t.Fatalf("Unexpected value for %s: desired %f, got %f", key, newValue, desiredNewValue)
	}
}

func hdb_assertIterKeySet(t *testing.T, db HDB, expectedKeys []string) {
	seenKeys := make(map[string]bool)
	result_chan, err_chan := db.IterKeys()
	for {
		if result_chan == nil && err_chan == nil {
			break
		}
		select {
		case err, ok := <-err_chan:
			if ok == false {
				err_chan = nil
			} else {
				t.Fatalf("Error while iterating over keys: %s", err)
			}
		case result, ok := <-result_chan:
			if ok == false {
				result_chan = nil
			} else {
				seenKeys[string(result)] = true
			}
		}
	}
	if len(seenKeys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys, found %d during iteration", len(expectedKeys), len(seenKeys))
	}
	for i := 0; i < len(expectedKeys); i++ {
		if !seenKeys[expectedKeys[i]] {
			t.Fatalf("Did not see expected key %s during iteration", expectedKeys[i])
		}
	}
}

func hdb_assertBeginTxn(t *testing.T, db HDB) {
	err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Unable to begin transaction: %s", err)
	}
}

func hdb_assertCommitTxn(t *testing.T, db HDB) {
	err := db.CommitTxn()
	if err != nil {
		t.Fatalf("Unable to commit transaction: %s", err)
	}
}

func hdb_assertAbortTxn(t *testing.T, db HDB) {
	err := db.AbortTxn()
	if err != nil {
		t.Fatalf("Unable to abort transaction: %s", err)
	}
}

func TestHDBMath(t *testing.T) {
	db := hdb_assertOpen(t, "testmath.hdb", HDBOWRITER|HDBOCREAT|HDBOTRUNC)
	defer hdb_assertClose(t, db)

	hdb_assertAddInt(t, db, "int", 1, 1)
	hdb_assertAddInt(t, db, "int", 1, 2)
	hdb_assertAddDouble(t, db, "double", 2.5, 2.5)
	hdb_assertAddDouble(t, db, "double", 2.5, 5.0)
}

func TestHDBIter(t *testing.T) {
	db := hdb_assertOpen(t, "testiter.hdb", HDBOWRITER|HDBOCREAT|HDBOTRUNC)
	defer hdb_assertClose(t, db)

	hdb_assertPut(t, db, "hello", "world")
	hdb_assertPut(t, db, "goodbye", "world")

	hdb_assertIterKeySet(t, db, []string{"hello", "goodbye"})
}

func TestHDBPut(t *testing.T) {
	db := hdb_assertOpen(t, "testput.hdb", HDBOWRITER|HDBOCREAT|HDBOTRUNC)
	defer hdb_assertClose(t, db)

	// Put, PutCat
	hdb_assertPut(t, db, "hello", "world")
	hdb_assertGetValue(t, db, "hello", "world")
	hdb_assertPutCat(t, db, "hello", "!")
	hdb_assertGetValue(t, db, "hello", "world!")

	// PutAsync
	hdb_assertPutAsync(t, db, "async", "value")
	hdb_assertSync(t, db)
	hdb_assertGetValue(t, db, "async", "value")

	// PutKeep
	hdb_assertPutKeep(t, db, "keep", "first")
	hdb_assertPutKeep(t, db, "keep", "second")
	hdb_assertGetValue(t, db, "keep", "first")
}

func TestHDBTransactions(t *testing.T) {
	db := hdb_assertOpen(t, "testtxn.hdb", HDBOWRITER|HDBOCREAT|HDBOTRUNC)
	defer hdb_assertClose(t, db)

	hdb_assertPut(t, db, "txn-1", "set-outside-txn")
	hdb_assertBeginTxn(t, db)
	hdb_assertPut(t, db, "txn-1", "set-inside-txn")
	hdb_assertAbortTxn(t, db)
	hdb_assertGetValue(t, db, "txn-1", "set-outside-txn")

	hdb_assertPut(t, db, "txn-2", "set-outside-txn")
	hdb_assertBeginTxn(t, db)
	hdb_assertPut(t, db, "txn-2", "set-inside-txn")
	hdb_assertCommitTxn(t, db)
	hdb_assertGetValue(t, db, "txn-2", "set-inside-txn")
}
