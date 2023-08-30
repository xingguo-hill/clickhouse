/*
 * @Description:用来做clickhouse数据导入的校验
 */
package clickhouse

import "reflect"

type RecordTable struct {
	ID        uint32 `db:"id"`
	Kind      string `db:"kind"`
	Val       string `db:"val"`
	FromCount uint32 `db:"from_count"`
	InCount   uint32 `db:"http_x_forwarded_for"`
	Suss      uint8  `db:"suss"`
	Stime     string `db:"stime"`
	Etime     string `db:"etime"`
}

func getImportRecordTable() []string {
	return []string{
		"id",
		"kind",
		"val",
		"from_count",
		"in_count",
		"suss",
		"etime",
		"ctime",
	}
}

/**
 * @description: 从导入记录表生成最新的Id
 * @param {string} tableName
 * @param {RecordTable} s
 * @return error
 */
func (client *ClientDao) InsertImportRecord(tableName string, s RecordTable) error {
	var params []any
	v := reflect.ValueOf(s)
	if v.Kind() != reflect.Struct {
		panic("BatchInser data must struct slice")
	}
	for i := 0; i < v.NumField(); i++ {
		params = append(params, v.Field(i).Interface())
	}
	sInsertQuery := generateBatchSQLHead(tableName, getImportRecordTable())
	return client.SingleTransaction(sInsertQuery, params)
}

/**
 * @description: 统计一次导入的总数
 * @param {string} tableName
 * @param {uint32} sourceId
 * @return uint32, error 返回当前操作导入数据库记录数，以及原数据与导入数据的比对结果
 */
func (client *ClientDao) GetTotalByImportId(tableName string, sourceId uint32) (uint32, error) {
	type total struct {
		Total uint32 `db:"total"`
	}
	var t []total
	err := client.SingleSelect(&t, "select count(1) as total from "+tableName+" where source_id=?", []any{sourceId})
	if err != nil {
		return 0, err
	}
	return t[0].Total, nil
}
