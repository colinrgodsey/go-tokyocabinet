package tokyocabinet

// #cgo pkg-config: tokyocabinet
// #include <tcadb.h>
import "C"

import "unsafe"

// abstract database doesn't provide detailed error messages
const ERR_MSG string = "Database operation failed"

type ADB struct {
	c_db *C.TCADB
}

func NewADB() *ADB {
	c_db := C.tcadbnew()
	return &ADB{c_db}
}

func (db *ADB) Del() {
	C.tcadbdel(db.c_db)
}

func (db *ADB) Open(path string) (err error) {
	c_path := C.CString(path)
	defer C.free(unsafe.Pointer(c_path))
	if !C.tcadbopen(db.c_db, c_path) {
		err = NewTokyoCabinetError(0, ERR_MSG)
	}
	return
}

func (db *ADB) Close() (err error) {
	if !C.tcadbclose(db.c_db) {
		err = NewTokyoCabinetError(0, ERR_MSG)
	}
	return
}

func (db *ADB) BeginTxn() (err error) {
	if !C.tcadbtranbegin(db.c_db) {
		err = NewTokyoCabinetError(0, ERR_MSG)
	}
	return
}

func (db *ADB) CommitTxn() (err error) {
	if !C.tcadbtrancommit(db.c_db) {
		err = NewTokyoCabinetError(0, ERR_MSG)
	}
	return
}

func (db *ADB) AbortTxn() (err error) {
	if !C.tcadbtranabort(db.c_db) {
		err = NewTokyoCabinetError(0, ERR_MSG)
	}
	return
}

func (db *ADB) Put(key []byte, value []byte) (err error) {
	if !C.tcadbput(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		unsafe.Pointer(&value[0]), C.int(len(value))) {
		err = NewTokyoCabinetError(0, ERR_MSG)
	}
	return
}

func (db *ADB) PutKeep(key []byte, value []byte) (err error) {
	// Unfortunately, ADB doesn't provide a way to check the error code
	// ...so we can't ignore only already-present keys.
	// ...so, we're just going to ignore *all* errors. Yeeeah.
	C.tcadbputkeep(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		unsafe.Pointer(&value[0]), C.int(len(value)))
	return
}

func (db *ADB) PutCat(key []byte, value []byte) (err error) {
	if !C.tcadbputcat(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		unsafe.Pointer(&value[0]), C.int(len(value))) {
		err = NewTokyoCabinetError(0, ERR_MSG)
	}
	return
}

func (db *ADB) AddInt(key []byte, value int) (newvalue int, err error) {
	res := C.tcadbaddint(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		C.int(value))
	if res == C.INT_MIN {
		err = NewTokyoCabinetError(0, ERR_MSG)
	}
	newvalue = int(res)
	return
}

func (db *ADB) AddDouble(key []byte, value float64) (newvalue float64, err error) {
	res := C.tcadbadddouble(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		C.double(value))
	if isnan(res) {
		err = NewTokyoCabinetError(0, ERR_MSG)
	}
	newvalue = float64(res)
	return
}

func (db *ADB) Remove(key []byte) (err error) {
	if !C.tcadbout(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key))) {
		err = NewTokyoCabinetError(0, ERR_MSG)
	}
	return
}

func (db *ADB) Get(key []byte) (out []byte, err error) {
	var size C.int
	rec := C.tcadbget(db.c_db,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		&size)
	if rec != nil {
		defer C.free(unsafe.Pointer(rec))
		out = C.GoBytes(rec, size)
	} else {
		err = NewTokyoCabinetError(0, ERR_MSG)
	}
	return
}

func (db *ADB) Size(key []byte) (out int, err error) {
	res := C.tcadbvsiz(db.c_db, unsafe.Pointer(&key[0]), C.int(len(key)))
	if res < 0 {
		err = NewTokyoCabinetError(0, ERR_MSG)
	} else {
		out = int(res)
	}
	return
}

func (db *ADB) IterKeys() (c chan []byte, e chan error) {
	c = make(chan []byte)
	e = make(chan error)
	if !C.tcadbiterinit(db.c_db) {
		e <- NewTokyoCabinetError(0, ERR_MSG)
		close(c)
		close(e)
		return
	}
	go func() {
		defer close(c)
		defer close(e)
		for {
			var size C.int
			rec := C.tcadbiternext(db.c_db, &size)
			if rec != nil {
				c <- C.GoBytes(rec, size)
				C.free(rec)
				continue
			}
			break
		}
	}()
	return
}

func (db *ADB) Sync() (err error) {
	if !C.tcadbsync(db.c_db) {
		err = NewTokyoCabinetError(0, ERR_MSG)
	}
	return
}
