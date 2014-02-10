package tokyocabinet

import "bytes"
import "io/ioutil"
import "os"
import "testing"

func bdb_assertOpen(t *testing.T, filename string, flags int) BDB {
    var db BDB = *NewBDB()
    if len(filename) == 0 {
        tf, err := ioutil.TempFile("", "tctest")
        if(err != nil) {
            t.Fatalf("Unable to create temporary file: %s", err)
        }
        filename = tf.Name()
    }
    err := db.Open(filename, flags)
    os.Remove(filename)
    if(err != nil) {
        t.Fatalf("Unable to open %s: %s", filename, err)
    }
    return db
}

func bdb_assertClose(t *testing.T, db BDB) {
    err := db.Close()
    if(err != nil) {
        t.Fatalf("Unable to close database: %s", err)
    }
}

func bdb_assertPut(t *testing.T, db BDB, key string, value string) {
    err := db.Put([]byte(key), []byte(value))
    if(err != nil) {
        t.Fatalf("Unable to assign key %s with value %s: %s", key, value, err)
    }
}

func bdb_assertPutCat(t *testing.T, db BDB, key string, value string) {
    err := db.PutCat([]byte(key), []byte(value))
    if(err != nil) {
        t.Fatalf("Unable to assign key %s with value %s: %s", key, value, err)
    }
}

func bdb_assertPutKeep(t *testing.T, db BDB, key string, value string) {
    err := db.PutKeep([]byte(key), []byte(value))
    if(err != nil) {
        t.Fatalf("Unable to assign key %s with value %s: %s", key, value, err)
    }
}

func bdb_assertGetValue(t *testing.T, db BDB, key string, expected string) {
    value, err := db.Get([]byte(key))
    if(err != nil) {
        t.Fatalf("Unable to retrieve value for key %s: %s", key, err)
    }
    if(bytes.Compare([]byte(expected), value) != 0) {
        t.Fatalf("Value for key %s came back incorrect (expected: %s; got: %s)", key, []byte(expected), value)
    }
}

func bdb_assertGetSize(t *testing.T, db BDB, key string) (size int) {
    size, err := db.Size([]byte(key))
    if(err != nil) {
        t.Fatalf("Unable to retrieve size of key %s: %s", key, err)
    }
    return
}

func bdb_assertSync(t *testing.T, db BDB) {
    err := db.Sync()
    if(err != nil) {
        t.Fatalf("Unable to sync database: %s", err)
    }
}

func bdb_assertAddInt(t *testing.T, db BDB, key string, increment int, desiredNewValue int) {
    newValue, err := db.AddInt([]byte(key), increment)
    if(err != nil) {
        t.Fatalf("Unable to add integer to key: %s", err)
    }
    if(newValue != desiredNewValue) {
        t.Fatalf("Unexpected value for %s: desired %d, got %d", key, newValue, desiredNewValue)
    }
}

func bdb_assertAddDouble(t *testing.T, db BDB, key string, increment float64, desiredNewValue float64) {
    newValue, err := db.AddDouble([]byte(key), increment)
    if(err != nil) {
        t.Fatalf("Unable to add integer to key: %s", err)
    }
    if(newValue != desiredNewValue) {
        t.Fatalf("Unexpected value for %s: desired %f, got %f", key, newValue, desiredNewValue)
    }
}

func bdb_assertBeginTxn(t *testing.T, db BDB) {
    err := db.BeginTxn()
    if(err != nil) {
        t.Fatalf("Unable to begin transaction: %s", err)
    }
}

func bdb_assertCommitTxn(t *testing.T, db BDB) {
    err := db.CommitTxn()
    if(err != nil) {
        t.Fatalf("Unable to commit transaction: %s", err)
    }
}

func bdb_assertAbortTxn(t *testing.T, db BDB) {
    err := db.AbortTxn()
    if(err != nil) {
        t.Fatalf("Unable to abort transaction: %s", err)
    }
}

func TestBDBMath(t *testing.T) {
    db := bdb_assertOpen(t, "testmath.hdb", BDBOWRITER | BDBOCREAT | BDBOTRUNC)
    defer bdb_assertClose(t, db)

    bdb_assertAddInt(t, db, "int", 1, 1)
    bdb_assertAddInt(t, db, "int", 1, 2)
    bdb_assertAddDouble(t, db, "double", 2.5, 2.5)
    bdb_assertAddDouble(t, db, "double", 2.5, 5.0)
}

func TestBDBPut(t *testing.T) {
    db := bdb_assertOpen(t, "testput.hdb", BDBOWRITER | BDBOCREAT | BDBOTRUNC)
    defer bdb_assertClose(t, db)

    // Put, PutCat
    bdb_assertPut(t, db, "hello", "world")
    bdb_assertGetValue(t, db, "hello", "world")
    bdb_assertPutCat(t, db, "hello", "!")
    bdb_assertGetValue(t, db, "hello", "world!")

    // PutKeep
    bdb_assertPutKeep(t, db, "keep", "first")
    bdb_assertPutKeep(t, db, "keep", "second")
    bdb_assertGetValue(t, db, "keep", "first")
}

func TestBDBTransactions(t *testing.T) {
    db := bdb_assertOpen(t, "testtxn.hdb", BDBOWRITER | BDBOCREAT | BDBOTRUNC)
    defer bdb_assertClose(t, db)

    bdb_assertPut(t, db, "txn-1", "set-outside-txn")
    bdb_assertBeginTxn(t, db)
    bdb_assertPut(t, db, "txn-1", "set-inside-txn")
    bdb_assertAbortTxn(t, db)
    bdb_assertGetValue(t, db, "txn-1", "set-outside-txn")

    bdb_assertPut(t, db, "txn-2", "set-outside-txn")
    bdb_assertBeginTxn(t, db)
    bdb_assertPut(t, db, "txn-2", "set-inside-txn")
    bdb_assertCommitTxn(t, db)
    bdb_assertGetValue(t, db, "txn-2", "set-inside-txn")
}
