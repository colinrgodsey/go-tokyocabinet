package tokyocabinet

// #cgo pkg-config: tokyocabinet
// #include <tcutil.h>
import "C"

const TCESUCCESS int = C.TCESUCCESS
const TCETHREAD int = C.TCETHREAD
const TCINVALID int = C.TCEINVALID
const TCENOFILE int = C.TCENOFILE
const TCENOPERM int = C.TCENOPERM
const TCEMETA int = C.TCEMETA
const TCERHEAD int = C.TCERHEAD
const TCEOPEN int = C.TCEOPEN
const TCECLOSE int = C.TCECLOSE
const TCETRUNC int = C.TCETRUNC
const TCESYNC int = C.TCESYNC
const TCESTAT int = C.TCESTAT
const TCESEEK int = C.TCESEEK
const TCEREAD int = C.TCEREAD
const TCEWRITE int = C.TCEWRITE
const TCEMMAP int = C.TCEMMAP
const TCELOCK int = C.TCELOCK
const TCEUNLINK int = C.TCEUNLINK
const TCERENAME int = C.TCERENAME
const TCEMKDIR int = C.TCEMKDIR
const TCERMDIR int = C.TCERMDIR
const TCEKEEP int = C.TCEKEEP
const TCENOREC int = C.TCENOREC
const TCEMISC int = C.TCEMISC

const TCDBTHASH int = C.TCDBTHASH
const TCDBTBTREE int = C.TCDBTBTREE
const TCDBTFIXED int = C.TCDBTFIXED
const TCDBTTABLE int = C.TCDBTTABLE

func ECodeName(ecode int) string {
	return C.GoString(C.tcerrmsg(C.int(ecode)))
}
