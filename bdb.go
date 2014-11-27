package tokyocabinet

// #cgo pkg-config: tokyocabinet
// #include <math.h>
// #include <tcbdb.h>
import "C"

import "errors"
import "unsafe"

const BDBFOPEN int = C.BDBFOPEN
const BDBFFATAL int = C.BDBFFATAL

const BDBTLARGE int = C.BDBTLARGE
const BDBTDEFLATE int = C.BDBTLARGE
const BDBTBZIP int = C.BDBTBZIP
const BDBTTCBS int = C.BDBTTCBS
const BDBTEXCODEC int = C.BDBTEXCODEC

const BDBOREADER int = C.BDBOREADER
const BDBOWRITER int = C.BDBOWRITER
const BDBOCREAT int = C.BDBOCREAT
const BDBOTRUNC int = C.BDBOTRUNC
const BDBONOLCK int = C.BDBONOLCK
const BDBOLCKNB int = C.BDBOLCKNB

func ECodeNameBDB(ecode int) string {
	return C.GoString(C.tcbdberrmsg(C.int(ecode)))
}

type BDB struct {
	c_db *C.TCBDB
}

func NewBDB() *BDB {
	c_db := C.tcbdbnew()
	return &BDB{c_db}
}

func (db *BDB) Del() {
	C.tcbdbdel(db.c_db)
}

func (db *BDB) LastECode() int {
	return int(C.tcbdbecode(db.c_db))
}

func (db *BDB) LastError() error {
	//return errors.New(ECodeNameBDB(db.LastECode()))
	code := db.LastECode()
	return NewTokyoCabinetError(code, ECodeNameBDB(code))
}

func (db *BDB) Open(path string, omode int) (err error) {
	c_path := C.CString(path)
	defer C.free(unsafe.Pointer(c_path))
	if !C.tcbdbopen(db.c_db, c_path, C.int(omode)) {
		err = db.LastError()
	}
	return
}

func (db *BDB) Close() (err error) {
	if !C.tcbdbclose(db.c_db) {
		err = db.LastError()
	}
	return
}

func (db *BDB) BeginTxn() (err error) {
	if !C.tcbdbtranbegin(db.c_db) {
		err = db.LastError()
	}
	return
}

func (db *BDB) CommitTxn() (err error) {
	if !C.tcbdbtrancommit(db.c_db) {
		err = db.LastError()
	}
	return
}

func (db *BDB) AbortTxn() (err error) {
	if !C.tcbdbtranabort(db.c_db) {
		err = db.LastError()
	}
	return
}

func (db *BDB) Put(key []byte, value []byte) (err error) {
	if !C.tcbdbput(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		unsafe.Pointer(&value[0]), C.int(len(value))) {
		err = db.LastError()
	}
	return
}

func (db *BDB) PutKeep(key []byte, value []byte) (err error) {
	if !C.tcbdbputkeep(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		unsafe.Pointer(&value[0]), C.int(len(value))) {
		if db.LastECode() == TCEKEEP {
			return
		}
		err = db.LastError()
	}
	return
}

func (db *BDB) PutCat(key []byte, value []byte) (err error) {
	if !C.tcbdbputcat(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		unsafe.Pointer(&value[0]), C.int(len(value))) {
		err = db.LastError()
	}
	return
}

func (db *BDB) AddInt(key []byte, value int) (newvalue int, err error) {
	res := C.tcbdbaddint(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		C.int(value))
	if res == C.INT_MIN {
		err = db.LastError()
	}
	newvalue = int(res)
	return
}

func (db *BDB) AddDouble(key []byte, value float64) (newvalue float64, err error) {
	res := C.tcbdbadddouble(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		C.double(value))
	if isnan(res) {
		err = db.LastError()
	}
	newvalue = float64(res)
	return
}

func (db *BDB) Remove(key []byte) (err error) {
	if !C.tcbdbout(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key))) {
		err = db.LastError()
	}
	return
}

func (db *BDB) Get(key []byte) (out []byte) {
	var size C.int
	rec := C.tcbdbget(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		&size)
	if rec != nil {
		defer C.free(unsafe.Pointer(rec))
		out = C.GoBytes(rec, size)
	}

	return
}

func (db *BDB) Size(key []byte) (out int, err error) {
	res := C.tcbdbvsiz(db.c_db, unsafe.Pointer(&key[0]), C.int(len(key)))
	if res < 0 {
		err = db.LastError()
	} else {
		out = int(res)
	}
	return
}

/* negative max for infinite */
func (db *BDB) Range(startKey []byte, startInclusive bool, endKey []byte,
	endInclusive bool, max int) (keys [][]byte, err error) {

	var startKeyLen int = 0
	var startKeyC unsafe.Pointer
	if startKey != nil {
		startKeyLen = len(startKey)
		startKeyC = unsafe.Pointer(&startKey[0])
	}

	var endKeyLen int = 0
	var endKeyC unsafe.Pointer
	if endKey != nil {
		endKeyLen = len(startKey)
		endKeyC = unsafe.Pointer(&endKey[0])
	}

	resList := C.tcbdbrange(
		db.c_db,
		startKeyC, C.int(startKeyLen), C.bool(startInclusive),
		endKeyC, C.int(endKeyLen), C.bool(endInclusive),
		C.int(max))

	num := int(C.tclistnum(resList))

	keys = make([][]byte, 0)
	for i := 0; i < num; i++ {
		var size C.int
		keyDat := C.tclistval(resList, C.int(i), &size)
		var key []byte

		if keyDat != nil {
			defer C.free(unsafe.Pointer(keyDat))
			key = C.GoBytes(keyDat, size)
		} else {
			err = errors.New("Failed to retrieve tclist value!")
			keys = nil
			return
		}

		keys = append(keys, key)
	}

	return
}

func (db *BDB) Sync() (err error) {
	if !C.tcbdbsync(db.c_db) {
		err = db.LastError()
	}
	return
}
