/*
 * @Description:用来做clickhouse数据导入的校验
 */
package clickhouse

/**
 * @description:从导入记录表生成最新的Id
 * @param {string} tableName
 * @param {*uint32} id
 * @param {string} kind
 * @param {string} val
 * @return error
 */
func (client *ClientDao) GenImportID(tableName string, id *uint32, kind string, val string) error {

	//生成自增id
	sInsertQuery := `INSERT INTO ` + tableName + ` (id, kind, val)
	 SELECT COALESCE(MAX(id), 0) + 1, '` + kind + `', '` + val + `' FROM ` + tableName + `;`
	if err := client.SingleTransaction(sInsertQuery, []any{}); err != nil {
		return err
	}
	//获取自增id信息
	type idGenRecord struct {
		ID uint32 `db:"id"`
	}
	idR := []idGenRecord{}
	err := client.SingleSelect(&idR, "SELECT id FROM "+tableName+" where kind=? and val=? ORDER BY id DESC LIMIT 1", []any{kind, val})
	if err != nil {
		return err
	}
	*id = idR[0].ID
	return nil
}

/**
 * @description: 统计一次导入的总数
 * @param {string} tableName
 * @param {uint32} sourceTotal
 * @param {uint32} importId
 * @return uint32, bool 返回当前操作导入数据库记录数，以及原数据与导入数据的比对结果
 */
func (client *ClientDao) CheckTotalByImportId(tableName string, sourceTotal uint32, importId uint32) (uint32, bool) {
	type total struct {
		Total uint32 `db:"total"`
	}
	flag := false
	var t []total
	err := client.SingleSelect(&t, "select count(1) as total from "+tableName+" where source_id=?", []any{importId})
	if err != nil {
		return 0, flag
	}
	intotal := t[0].Total
	if sourceTotal == intotal {
		flag = true
	}
	return intotal, flag
}

/**
* @description: 更新导入记录表结果
* @param {string} tableName
* @param {uint32} id
* @param {uint32} sourceTotal
* @param {uint32} intotal
* @return  error
 */
func (client *ClientDao) UpdateImportStausByImportId(tableName string, id uint32, sourceTotal uint32, intotal uint32) error {
	var suss uint8
	if sourceTotal == intotal {
		suss = 1
	} else {
		suss = 2
	}
	return client.SingleTransaction("alter table "+tableName+" update from_count=?,in_count=?,suss=? where id=?",
		[]any{sourceTotal, intotal, suss, id})
}
