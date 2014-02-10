package tokyocabinet

// #cgo pkg-config: tokyocabinet
// #include <tchdb.h>
import "C"

import "errors"
import "unsafe"

const HDBFOPEN int = C.HDBFOPEN
const HDBFFATAL int = C.HDBFFATAL

const HDBTLARGE int = C.HDBTLARGE
const HDBTDEFLATE int = C.HDBTLARGE
const HDBTBZIP int = C.HDBTBZIP
const HDBTTCBS int = C.HDBTTCBS
const HDBTEXCODEC int = C.HDBTEXCODEC

const HDBOREADER int = C.HDBOREADER
const HDBOWRITER int = C.HDBOWRITER
const HDBOCREAT int = C.HDBOCREAT
const HDBOTRUNC int = C.HDBOTRUNC
const HDBONOLCK int = C.HDBONOLCK
const HDBOLCKNB int = C.HDBOLCKNB

func ECodeNameHDB(ecode int) string {
	return C.GoString(C.tchdberrmsg(C.int(ecode)))
}

type HDB struct {
	c_db *C.TCHDB
}

func NewHDB() *HDB {
	c_db := C.tchdbnew()
	return &HDB{c_db}
}

func (db *HDB) Del() {
	C.tchdbdel(db.c_db)
}

func (db *HDB) LastECode() int {
	return int(C.tchdbecode(db.c_db))
}

func (db *HDB) LastError() error {
	return errors.New(ECodeNameHDB(db.LastECode()))
}

func (db *HDB) Open(path string, omode int) (err error) {
	c_path := C.CString(path)
	defer C.free(unsafe.Pointer(c_path))
	if !C.tchdbopen(db.c_db, c_path, C.int(omode)) {
		err = db.LastError()
	}
	return
}

func (db *HDB) Close() (err error) {
	if !C.tchdbclose(db.c_db) {
		err = db.LastError()
	}
	return
}

func (db *HDB) BeginTxn() (err error) {
	if !C.tchdbtranbegin(db.c_db) {
		err = db.LastError()
	}
	return
}

func (db *HDB) CommitTxn() (err error) {
	if !C.tchdbtrancommit(db.c_db) {
		err = db.LastError()
	}
	return
}

func (db *HDB) AbortTxn() (err error) {
	if !C.tchdbtranabort(db.c_db) {
		err = db.LastError()
	}
	return
}

func (db *HDB) Put(key []byte, value []byte) (err error) {
	if !C.tchdbput(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		unsafe.Pointer(&value[0]), C.int(len(value))) {
		err = db.LastError()
	}
	return
}

func (db *HDB) PutKeep(key []byte, value []byte) (err error) {
	if !C.tchdbputkeep(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		unsafe.Pointer(&value[0]), C.int(len(value))) {
		if db.LastECode() == TCEKEEP {
			return
		}
		err = db.LastError()
	}
	return
}

func (db *HDB) PutCat(key []byte, value []byte) (err error) {
	if !C.tchdbputcat(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		unsafe.Pointer(&value[0]), C.int(len(value))) {
		err = db.LastError()
	}
	return
}

func (db *HDB) AddInt(key []byte, value int) (newvalue int, err error) {
	res := C.tchdbaddint(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		C.int(value))
	if res == C.INT_MIN {
		err = db.LastError()
	}
	newvalue = int(res)
	return
}

func (db *HDB) AddDouble(key []byte, value float64) (newvalue float64, err error) {
	res := C.tchdbadddouble(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		C.double(value))
	if C.isnan(res) != 0 {
		err = db.LastError()
	}
	newvalue = float64(res)
	return
}

func (db *HDB) Remove(key []byte) (err error) {
	if !C.tchdbout(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key))) {
		err = db.LastError()
	}
	return
}

func (db *HDB) Get(key []byte) (out []byte, err error) {
	var size C.int
	rec := C.tchdbget(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		&size)
	if rec != nil {
		defer C.free(unsafe.Pointer(rec))
		out = C.GoBytes(rec, size)
	} else {
		err = db.LastError()
	}
	return
}

func (db *HDB) Size(key []byte) (out int, err error) {
	res := C.tchdbvsiz(db.c_db, unsafe.Pointer(&key[0]), C.int(len(key)))
	if res < 0 {
		err = db.LastError()
	} else {
		out = int(res)
	}
	return
}

/* note that only one iterator can be active at a time for a given database */
func (db *HDB) IterKeys() (c chan []byte, e chan error) {
	c = make(chan []byte)
	e = make(chan error)
	if !C.tchdbiterinit(db.c_db) {
		e <- db.LastError()
		close(c)
		close(e)
		return
	}
	go func() {
		defer close(c)
		defer close(e)
		for {
			var size C.int
			rec := C.tchdbiternext(db.c_db, &size)
			if rec != nil {
				c <- C.GoBytes(rec, size)
				C.free(rec)
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

func (db *HDB) Sync() (err error) {
	if !C.tchdbsync(db.c_db) {
		err = db.LastError()
	}
	return
}
