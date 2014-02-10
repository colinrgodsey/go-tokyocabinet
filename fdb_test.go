package tokyocabinet

import "bytes"
import "io/ioutil"
import "os"
import "testing"

func fdb_assertOpen(t *testing.T, filename string, flags int) FDB {
    var db FDB = *NewFDB()
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

func fdb_assertClose(t *testing.T, db FDB) {
    err := db.Close()
    if(err != nil) {
        t.Fatalf("Unable to close database: %s", err)
    }
}

func fdb_assertPut(t *testing.T, db FDB, key int64, value string) {
    err := db.Put(key, []byte(value))
    if(err != nil) {
        t.Fatalf("Unable to assign key %d with value %s: %s", key, value, err)
    }
}

func fdb_assertPutCat(t *testing.T, db FDB, key int64, value string) {
    err := db.PutCat(key, []byte(value))
    if(err != nil) {
        t.Fatalf("Unable to assign key %d with value %s: %s", key, value, err)
    }
}

func fdb_assertPutKeep(t *testing.T, db FDB, key int64, value string) {
    err := db.PutKeep(key, []byte(value))
    if(err != nil) {
        t.Fatalf("Unable to assign key %d with value %s: %s", key, value, err)
    }
}

func fdb_assertGetValue(t *testing.T, db FDB, key int64, expected string) {
    value, err := db.Get(key)
    if(err != nil) {
        t.Fatalf("Unable to retrieve value for key %d: %s", key, err)
    }
    if(bytes.Compare([]byte(expected), value) != 0) {
        t.Fatalf("Value for key %d came back incorrect (expected: %s; got: %s)", key, []byte(expected), value)
    }
}

func fdb_assertGetSize(t *testing.T, db FDB, key int64) (size int) {
    size, err := db.Size(key)
    if(err != nil) {
        t.Fatalf("Unable to retrieve size of key %d: %s", key, err)
    }
    return
}

func fdb_assertSync(t *testing.T, db FDB) {
    err := db.Sync()
    if(err != nil) {
        t.Fatalf("Unable to sync database: %s", err)
    }
}

func fdb_assertAddInt(t *testing.T, db FDB, key int64, increment int, desiredNewValue int) {
    newValue, err := db.AddInt(key, increment)
    if(err != nil) {
        t.Fatalf("Unable to add integer to key: %s", err)
    }
    if(newValue != desiredNewValue) {
        t.Fatalf("Unexpected value for %d: desired %d, got %d", key, newValue, desiredNewValue)
    }
}

func fdb_assertAddDouble(t *testing.T, db FDB, key int64, increment float64, desiredNewValue float64) {
    newValue, err := db.AddDouble(key, increment)
    if(err != nil) {
        t.Fatalf("Unable to add integer to key: %s", err)
    }
    if(newValue != desiredNewValue) {
        t.Fatalf("Unexpected value for %d: desired %f, got %f", key, newValue, desiredNewValue)
    }
}

func fdb_assertIterKeySet(t *testing.T, db FDB, expectedKeys []int64) {
    seenKeys := make(map[int64]bool)
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
                    seenKeys[result] = true
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

func fdb_assertBeginTxn(t *testing.T, db FDB) {
    err := db.BeginTxn()
    if(err != nil) {
        t.Fatalf("Unable to begin transaction: %s", err)
    }
}

func fdb_assertCommitTxn(t *testing.T, db FDB) {
    err := db.CommitTxn()
    if(err != nil) {
        t.Fatalf("Unable to commit transaction: %s", err)
    }
}

func fdb_assertAbortTxn(t *testing.T, db FDB) {
    err := db.AbortTxn()
    if(err != nil) {
        t.Fatalf("Unable to abort transaction: %s", err)
    }
}

func TestFDBMath(t *testing.T) {
    db := fdb_assertOpen(t, "testmath.hdb", FDBOWRITER | FDBOCREAT | FDBOTRUNC)
    defer fdb_assertClose(t, db)

    fdb_assertAddInt(t, db, 1, 1, 1)
    fdb_assertAddInt(t, db, 1, 1, 2)
    fdb_assertAddDouble(t, db, 2, 2.5, 2.5)
    fdb_assertAddDouble(t, db, 2, 2.5, 5.0)
}

func TestFDBIter(t *testing.T) {
    db := fdb_assertOpen(t, "testiter.hdb", FDBOWRITER | FDBOCREAT | FDBOTRUNC)
    defer fdb_assertClose(t, db)

    fdb_assertPut(t, db, 1, "hello")
    fdb_assertPut(t, db, 2, "goodbye")

    fdb_assertIterKeySet(t, db, []int64{1,2})
}

func TestFDBPut(t *testing.T) {
    db := fdb_assertOpen(t, "testput.hdb", FDBOWRITER | FDBOCREAT | FDBOTRUNC)
    defer fdb_assertClose(t, db)

    // Put, PutCat
    fdb_assertPut(t, db, 1, "world")
    fdb_assertGetValue(t, db, 1, "world")
    fdb_assertPutCat(t, db, 1, "!")
    fdb_assertGetValue(t, db, 1, "world!")

    // PutKeep
    fdb_assertPutKeep(t, db, 2, "first")
    fdb_assertPutKeep(t, db, 2, "second")
    fdb_assertGetValue(t, db, 2, "first")
}

func TestFDBTransactions(t *testing.T) {
    db := fdb_assertOpen(t, "testtxn.hdb", FDBOWRITER | FDBOCREAT | FDBOTRUNC)
    defer fdb_assertClose(t, db)

    fdb_assertPut(t, db, 1, "set-outside-txn")
    fdb_assertBeginTxn(t, db)
    fdb_assertPut(t, db, 1, "set-inside-txn")
    fdb_assertAbortTxn(t, db)
    fdb_assertGetValue(t, db, 1, "set-outside-txn")

    fdb_assertPut(t, db, 2, "set-outside-txn")
    fdb_assertBeginTxn(t, db)
    fdb_assertPut(t, db, 2, "set-inside-txn")
    fdb_assertCommitTxn(t, db)
    fdb_assertGetValue(t, db, 2, "set-inside-txn")
}
