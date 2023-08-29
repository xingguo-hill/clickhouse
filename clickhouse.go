package clickhouse

import (
	"fmt"
	"reflect"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
)

type ClientDao struct {
	db *sqlx.DB
}

func NewClient(dsn string) *ClientDao {
	db := sqlx.MustConnect("clickhouse", dsn)
	return &ClientDao{
		db: db,
	}
}

/**
 * @description: 单条记录操作增删改事务操作
 * @param {string} sSql
 * @param {[]any} param
 * @return error
 */
func (client *ClientDao) SingleTransaction(sSql string, param []any) error {
	//数据预处理写入
	tx := client.db.MustBegin()
	stmt, err := tx.Prepare(sSql)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(param...)
	if err != nil {
		return err
	}
	return tx.Commit()
}

/**
 * @description: 查询语句
 * @param {any} res 返回结果集结构体切片
 * @param {string} sSelect 查询语句
 * @param {[]any} params
 * @return error
 */
func (client *ClientDao) SingleSelect(res any, sSelect string, params []any) error {
	return client.db.Select(res, sSelect, params...)
}

/**
 * @description: 批量插入结构体数组
 * @param {string} tableName
 * @param {[]string} feilds
 * @param {*[]any} datas
 * @return error
 */
func (client *ClientDao) BatchInsert(tableName string, feilds []string, datas *[]any) error {
	//数据预处理写入
	sInsertQuery := generateBatchSQLHead(tableName, feilds)
	tx := client.db.MustBegin()
	defer tx.Rollback()
	stmt, err := tx.Prepare(sInsertQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, record := range *datas {
		var params []any
		v := reflect.ValueOf(record)
		if v.Kind() != reflect.Struct {
			panic("BatchInser data must struct slice")
		}
		for i := 0; i < v.NumField(); i++ {
			params = append(params, v.Field(i).Interface())
		}
		_, err := stmt.Exec(params...)
		if err != nil {
			return err
		}
		params = nil
	}
	return tx.Commit()
}

/**
 * @description: 生成批处理的 SQL 语句前缀
 * @param {string} tableName
 * @param {[]string} feilds
 * @return string
 */
func generateBatchSQLHead(tableName string, feilds []string) string {
	var rowNames []string
	var valuesPattern []string
	for _, v := range feilds {
		rowNames = append(rowNames, fmt.Sprintf("%v", v))
		valuesPattern = append(valuesPattern, "?")
	}
	sql := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, tableName, strings.Join(rowNames, ","), strings.Join(valuesPattern, ","))
	return sql
}

/**
 * @description: 关闭clickhouse链接
 * @return error
 */
func (client *ClientDao) Close() error {
	return client.db.Close()
}
