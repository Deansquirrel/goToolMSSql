package goToolMSSql

import (
	"database/sql"
	"fmt"
	_ "github.com/alexbrainman/odbc"
)

var connFormatter string

var dbMap map[string]*sql.DB

type MSSqlConfig struct {
	Server string
	Port   int
	DbName string
	User   string
	Pwd    string
}

func init() {
	dbMap = make(map[string]*sql.DB)
	connFormatter = "Driver={SQL Server};Server=%s,%d;Database=%s;Uid=%s;Pwd=%s;Network=DbMsSoCn;"
}

//根据配置获取数据库连接
func GetConn(config *MSSqlConfig) (*sql.DB, error) {
	var conn *sql.DB
	connString := getConnStr(config)
	if _, ok := dbMap[connString]; ok {
		conn = dbMap[connString]
		if IsValid(conn) {
			return conn, nil
		} else {
			delete(dbMap, connString)
			return GetConn(config)
		}
	}
	conn, err := getConn(connString)
	if err != nil {
		return nil, err
	}
	dbMap[connString] = conn
	return GetConn(config)
}

//获取连接字符串
func getConnStr(config *MSSqlConfig) string {
	return fmt.Sprintf(connFormatter, config.Server, config.Port, config.DbName, config.User, config.Pwd)
}

//根据配置获取数据库连接
func getConn(connString string) (*sql.DB, error) {
	conn, err := sql.Open("odbc", connString)
	//fmt.Println(connString)
	if err != nil {
		return nil, err
	}

	err = conn.Ping()
	if err != nil {
		return nil, err
	}

	//conn.SetMaxIdleConns(30)
	//conn.SetMaxOpenConns(30)
	//conn.SetConnMaxLifetime(time.Second * 60 * 10)
	return conn, nil
}

//检查数据库连接是否有效
func IsValid(db *sql.DB) bool {
	if db != nil {
		err := db.Ping()
		if err == nil {
			return true
		}
	}
	return false
}
