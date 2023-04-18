package CouloyDB

const (
	mergeDirName = "merge"
)

var (
	MERGE_FIN_Key   = []byte{0x07}
	TX_COMMENT_KEY  = []byte{0x04}
	TX_ROLLBACK_KEY = []byte{0x04}
)

var NO_TX_ID uint64 = 0
