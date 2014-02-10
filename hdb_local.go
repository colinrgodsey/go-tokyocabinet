package tokyocabinet

// #cgo pkg-config: tokyocabinet
// #include <tchdb.h>
import "C"
import "unsafe"

func (db *HDB) Tune(bnum int64, apow int8, fpow int8, opts uint8) (err error) {
    if ! C.tchdbtune(db.c_db, C.int64_t(bnum), C.int8_t(apow), C.int8_t(fpow), C.uint8_t(opts)) {
        err = db.LastError()
    }
    return
}

func (db *HDB) SetCache(rcnum int32) (err error) {
    if ! C.tchdbsetcache(db.c_db, C.int32_t(rcnum)) {
        err = db.LastError()
    }
    return
}

func (db *HDB) SetExtraMemorySize(xmsiz int64) (err error) {
    if ! C.tchdbsetxmsiz(db.c_db, C.int64_t(xmsiz)) {
        err = db.LastError()
    }
    return
}

func (db *HDB) SetDefragStepSize(dfunit int32) (err error) {
    if ! C.tchdbsetdfunit(db.c_db, C.int32_t(dfunit)) {
        err = db.LastError()
    }
    return
}

func (db *HDB) PutAsync(key []byte, value []byte) (err error) {
    if ! C.tchdbputasync(db.c_db,
            unsafe.Pointer(&key[0]), C.int(len(key)),
            unsafe.Pointer(&value[0]), C.int(len(value))) {
        err = db.LastError()
    }
    return
}

