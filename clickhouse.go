package clickhouse

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
)

type Total struct {
	Total int `db:"total"`
}
type ClientDao struct {
	db *sqlx.DB
}

func NewClient(dsn string, debug bool) *ClientDao {
	if debug == false {
		setDebug()
	}
	db := sqlx.MustConnect("clickhouse", dsn)
	return &ClientDao{
		db: db,
	}
}

/**
 * @description: 设置数据库操作输出调试日志
 * @return
 */
func setDebug() {
	sqlfile, err := os.OpenFile("/dev/null", os.O_RDWR|os.O_APPEND, 0755)
	if err != nil {
		panic(err)
	}
	clickhouse.SetLogOutput(sqlfile)
}

/**
 * @description:批量插入结构体数组
 * @param {*sqlx.DB} db
 * @param {string} tableName
 * @param {*[]any} alog
 * @return error
 */
func (client *ClientDao) BatchInserLog(tableName string, feilds []string, alogs *[]any) error {
	//数据预处理写入
	sInsertQuery := generateBatchSQLHead(tableName, feilds)
	tx := client.db.MustBegin()
	defer tx.Rollback()
	stmt, err := tx.Prepare(sInsertQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, record := range *alogs {
		var params []any
		v := reflect.ValueOf(record)
		if v.Kind() != reflect.Struct {
			panic("BatchInserLog must struct slice")
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
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
	}
	return err
}

/**
 * @description: 生成批处理的 SQL 语句前缀
 * @param {*[]any} data
 * @param {string} tableName
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
 * @description:统计一次导入的总数
 * @param {*sqlx.DB} db
 * @param {*} tableName
 * @param {string} sourceFile
 * @return int,error
 */
func (client *ClientDao) GetTableTotalBySourceFile(tableName, sourceFile string) (int, error) {
	sql := fmt.Sprintf("select count(1) as total from %s where source_file='%s';", tableName, sourceFile)
	var t []Total
	if err := client.db.Select(&t, sql); err != nil {
		return 0, err
	}
	return t[0].Total, nil
}

/**
 * @description: 关联clickhouse 链接
 * @return error
 */
func (client *ClientDao) Close() error {
	return client.db.Close()
}
