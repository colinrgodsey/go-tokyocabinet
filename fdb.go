package tokyocabinet

// #cgo pkg-config: tokyocabinet
// #include <math.h>
// #include <tcfdb.h>
import "C"

import "unsafe"

const FDBFOPEN int = C.FDBFOPEN
const FDBFFATAL int = C.FDBFFATAL

const FDBOREADER int = C.FDBOREADER
const FDBOWRITER int = C.FDBOWRITER
const FDBOCREAT int = C.FDBOCREAT
const FDBOTRUNC int = C.FDBOTRUNC
const FDBONOLCK int = C.FDBONOLCK
const FDBOLCKNB int = C.FDBOLCKNB

func ECodeNameFDB(ecode int) string {
	return C.GoString(C.tcfdberrmsg(C.int(ecode)))
}

type FDB struct {
	c_db *C.TCFDB
}

func NewFDB() *FDB {
	c_db := C.tcfdbnew()
	return &FDB{c_db}
}

func (db *FDB) Del() {
	C.tcfdbdel(db.c_db)
}

func (db *FDB) LastECode() int {
	return int(C.tcfdbecode(db.c_db))
}

func (db *FDB) LastError() error {
	code := db.LastECode()
	return NewTokyoCabinetError(code, ECodeNameFDB(code))
}

func (db *FDB) Open(path string, omode int) (err error) {
	c_path := C.CString(path)
	defer C.free(unsafe.Pointer(c_path))
	if !C.tcfdbopen(db.c_db, c_path, C.int(omode)) {
		err = db.LastError()
	}
	return
}

func (db *FDB) Close() (err error) {
	if !C.tcfdbclose(db.c_db) {
		err = db.LastError()
	}
	return
}

func (db *FDB) BeginTxn() (err error) {
	if !C.tcfdbtranbegin(db.c_db) {
		err = db.LastError()
	}
	return
}

func (db *FDB) CommitTxn() (err error) {
	if !C.tcfdbtrancommit(db.c_db) {
		err = db.LastError()
	}
	return
}

func (db *FDB) AbortTxn() (err error) {
	if !C.tcfdbtranabort(db.c_db) {
		err = db.LastError()
	}
	return
}

func (db *FDB) Put(key int64, value []byte) (err error) {
	if !C.tcfdbput(db.c_db,
		C.int64_t(key),
		unsafe.Pointer(&value[0]), C.int(len(value))) {
		err = db.LastError()
	}
	return
}

func (db *FDB) PutKeep(key int64, value []byte) (err error) {
	if !C.tcfdbputkeep(db.c_db,
		C.int64_t(key),
		unsafe.Pointer(&value[0]), C.int(len(value))) {
		if db.LastECode() == TCEKEEP {
			return
		}
		err = db.LastError()
	}
	return
}

func (db *FDB) PutCat(key int64, value []byte) (err error) {
	if !C.tcfdbputcat(db.c_db,
		C.int64_t(key),
		unsafe.Pointer(&value[0]), C.int(len(value))) {
		err = db.LastError()
	}
	return
}

func (db *FDB) AddInt(key int64, value int) (newvalue int, err error) {
	res := C.tcfdbaddint(db.c_db,
		C.int64_t(key),
		C.int(value))
	if res == C.INT_MIN {
		err = db.LastError()
	}
	newvalue = int(res)
	return
}

func (db *FDB) AddDouble(key int64, value float64) (newvalue float64, err error) {
	res := C.tcfdbadddouble(db.c_db,
		C.int64_t(key),
		C.double(value))
	if isnan(res) {
		err = db.LastError()
	}
	newvalue = float64(res)
	return
}

func (db *FDB) Remove(key int64) (err error) {
	if !C.tcfdbout(db.c_db, C.int64_t(key)) {
		err = db.LastError()
	}
	return
}

func (db *FDB) Get(key int64) (out []byte, err error) {
	var size C.int
	rec := C.tcfdbget(db.c_db, C.int64_t(key), &size)
	if rec != nil {
		defer C.free(unsafe.Pointer(rec))
		out = C.GoBytes(rec, size)
	} else {
		err = db.LastError()
	}
	return
}

func (db *FDB) Size(key int64) (out int, err error) {
	res := C.tcfdbvsiz(db.c_db, C.int64_t(key))
	if res < 0 {
		err = db.LastError()
	} else {
		out = int(res)
	}
	return
}

/* note that only one iterator can be active at a time for a given database */
func (db *FDB) IterKeys() (c chan int64, e chan error) {
	c = make(chan int64)
	e = make(chan error)
	if !C.tcfdbiterinit(db.c_db) {
		e <- db.LastError()
		close(c)
		close(e)
		return
	}
	go func() {
		defer close(c)
		defer close(e)
		for {
			rec := C.tcfdbiternext(db.c_db)
			if rec != 0 {
				c <- int64(rec)
				continue
			}
			if db.LastECode() != TCENOREC {
				e <- db.LastError()
			}
			break
		}
	}()
	return
}

func (db *FDB) Sync() (err error) {
	if !C.tcfdbsync(db.c_db) {
		err = db.LastError()
	}
	return
}
