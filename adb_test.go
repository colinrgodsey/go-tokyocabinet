package tokyocabinet

import "bytes"
import "os"
import "testing"

func adb_assertOpen(t *testing.T, filename string) ADB {
    var db ADB = *NewADB()
    err := db.Open(filename)
    if filename[0] != '+' && filename[0] != '*' {
        os.Remove(filename)
    }
    if(err != nil) {
        t.Fatalf("Unable to open %s: %s", filename, err)
    }
    return db
}

func adb_assertClose(t *testing.T, db ADB) {
    err := db.Close()
    if(err != nil) {
        t.Fatalf("Unable to close database: %s", err)
    }
}

func adb_assertPut(t *testing.T, db ADB, key string, value string) {
    err := db.Put([]byte(key), []byte(value))
    if(err != nil) {
        t.Fatalf("Unable to assign key %s with value %s: %s", key, value, err)
    }
}

func adb_assertPutCat(t *testing.T, db ADB, key string, value string) {
    err := db.PutCat([]byte(key), []byte(value))
    if(err != nil) {
        t.Fatalf("Unable to assign key %s with value %s: %s", key, value, err)
    }
}

func adb_assertPutKeep(t *testing.T, db ADB, key string, value string) {
    err := db.PutKeep([]byte(key), []byte(value))
    if(err != nil) {
        t.Fatalf("Unable to assign key %s with value %s: %s", key, value, err)
    }
}

func adb_assertGetValue(t *testing.T, db ADB, key string, expected string) {
    value, err := db.Get([]byte(key))
    if(err != nil) {
        t.Fatalf("Unable to retrieve value for key %s: %s", key, err)
    }
    if(bytes.Compare([]byte(expected), value) != 0) {
        t.Fatalf("Value for key %s came back incorrect (expected: %s; got: %s)", key, []byte(expected), value)
    }
}

func adb_assertGetSize(t *testing.T, db ADB, key string) (size int) {
    size, err := db.Size([]byte(key))
    if(err != nil) {
        t.Fatalf("Unable to retrieve size of key %s: %s", key, err)
    }
    return
}

func adb_assertSync(t *testing.T, db ADB) {
    err := db.Sync()
    if(err != nil) {
        t.Fatalf("Unable to sync database: %s", err)
    }
}

func adb_assertAddInt(t *testing.T, db ADB, key string, increment int, desiredNewValue int) {
    newValue, err := db.AddInt([]byte(key), increment)
    if(err != nil) {
        t.Fatalf("Unable to add integer to key: %s", err)
    }
    if(newValue != desiredNewValue) {
        t.Fatalf("Unexpected value for %s: desired %d, got %d", key, newValue, desiredNewValue)
    }
}

func adb_assertAddDouble(t *testing.T, db ADB, key string, increment float64, desiredNewValue float64) {
    newValue, err := db.AddDouble([]byte(key), increment)
    if(err != nil) {
        t.Fatalf("Unable to add integer to key: %s", err)
    }
    if(newValue != desiredNewValue) {
        t.Fatalf("Unexpected value for %s: desired %f, got %f", key, newValue, desiredNewValue)
    }
}

func adb_assertIterKeySet(t *testing.T, db ADB, expectedKeys []string) {
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
    if(len(seenKeys) != len(expectedKeys)) {
        t.Fatalf("Expected %d keys, found %d during iteration", len(expectedKeys), len(seenKeys))
    }
    for i := 0; i < len(expectedKeys); i++ {
        if(!seenKeys[expectedKeys[i]]) {
            t.Fatalf("Did not see expected key %s during iteration", expectedKeys[i])
        }
    }
}

func adb_assertBeginTxn(t *testing.T, db ADB) {
    err := db.BeginTxn()
    if(err != nil) {
        t.Fatalf("Unable to begin transaction: %s", err)
    }
}

func adb_assertCommitTxn(t *testing.T, db ADB) {
    err := db.CommitTxn()
    if(err != nil) {
        t.Fatalf("Unable to commit transaction: %s", err)
    }
}

func adb_assertAbortTxn(t *testing.T, db ADB) {
    err := db.AbortTxn()
    if(err != nil) {
        t.Fatalf("Unable to abort transaction: %s", err)
    }
}


func TestADBMath(t *testing.T) {
    db := adb_assertOpen(t, "*")
    defer adb_assertClose(t, db)

    adb_assertAddInt(t, db, "int", 1, 1)
    adb_assertAddInt(t, db, "int", 1, 2)
    adb_assertAddDouble(t, db, "double", 2.5, 2.5)
    adb_assertAddDouble(t, db, "double", 2.5, 5.0)
}

func TestADBIter(t *testing.T) {
    db := adb_assertOpen(t, "*")
    defer adb_assertClose(t, db)

    adb_assertPut(t, db, "hello", "world")
    adb_assertPut(t, db, "goodbye", "world")

    adb_assertIterKeySet(t, db, []string{"hello", "goodbye"})
}

func TestADBPut(t *testing.T) {
    db := adb_assertOpen(t, "*")
    defer adb_assertClose(t, db)

    // Put, PutCat
    adb_assertPut(t, db, "hello", "world")
    adb_assertGetValue(t, db, "hello", "world")
    adb_assertPutCat(t, db, "hello", "!")
    adb_assertGetValue(t, db, "hello", "world!")

    // PutKeep
    adb_assertPutKeep(t, db, "keep", "first")
    adb_assertPutKeep(t, db, "keep", "second")
    adb_assertGetValue(t, db, "keep", "first")
}

func TestADBTransactions(t *testing.T) {
    // transactions not supported for in-memory hash databases?
    db := adb_assertOpen(t, "testadbtxn.tch")
    defer adb_assertClose(t, db)

    adb_assertPut(t, db, "txn-1", "set-outside-txn")
    adb_assertBeginTxn(t, db)
    adb_assertPut(t, db, "txn-1", "set-inside-txn")
    adb_assertAbortTxn(t, db)
    adb_assertGetValue(t, db, "txn-1", "set-outside-txn")

    adb_assertPut(t, db, "txn-2", "set-outside-txn")
    adb_assertBeginTxn(t, db)
    adb_assertPut(t, db, "txn-2", "set-inside-txn")
    adb_assertCommitTxn(t, db)
    adb_assertGetValue(t, db, "txn-2", "set-inside-txn")
}
