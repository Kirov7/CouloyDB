package public

const (
	MergeDirName          = "merge"
	FileLockName          = "flock"
	DataFileNameSuffix    = ".cly"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
	TxIDFileName          = "merge-finished"
)

var (
	// MERGE_FIN_Key This key is used to mark the completion of the merge
	MERGE_FIN_Key = []byte{0x07}

	// TX_COMMIT_KEY This key is used to mark the commit of the transaction
	TX_COMMIT_KEY = []byte{0x04}
)

var (
	NO_TX_ID int64 = 0
)
