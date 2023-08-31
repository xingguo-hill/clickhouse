package clickhouse

type RecordTable struct {
	ID        uint32
	Kind      string
	Val       string
	FromCount uint32
	InCount   uint32
	Suss      uint8
	Stime     string
	Etime     string
}

func getImportRecordTable() []string {
	return []string{
		"id",
		"kind",
		"val",
		"from_count",
		"in_count",
		"suss",
		"stime",
		"etime",
	}
}
