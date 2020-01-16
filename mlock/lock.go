package mlock

import (
	"context"
	"database/sql"
	"errors"

	_ "github.com/go-sql-driver/mysql"
)

var notLockedErr = errors.New("this lock is not locked")

// 基于mysql lock functions实现的一个简单全局锁
// 仅支持基本的锁功能, 进程崩溃会自动释放锁，
// 协程不安全，每个协程应该单独创建该锁，并执行Lock获取锁
// 如果没有释放锁，并且丢失锁，那么除非在数据库操作，该锁无法被释放
type MLock struct {
	pool *sql.DB
	conn *sql.Conn
	name string
}

func NewLock(lockName, dataSourceName string) (*MLock, error) {
	pool, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return nil, err
	}

	// 只用一个链接
	pool.SetMaxOpenConns(1)
	pool.SetMaxIdleConns(0)

	return &MLock{name: lockName, pool:pool}, nil
}

func (lock *MLock) Lock(timeout int) (locked bool,  err error) {
	if lock.conn == nil {
		// todo: 改善context来源
		lock.conn, err = lock.pool.Conn(context.Background())
		if err != nil {
			return
		}
	}

	locked, err = lock.execute("select get_lock(?, ?)", lock.name, timeout)
	if err != nil {
		// todo: 封装原始err
		if err = lock.dropConn(); err != nil {
			return
		}
		return
	}

	// 没有获取锁时应该释放链接
	if !locked {
		if err = lock.dropConn(); err != nil {
			return
		}
	}

	return
}

func (lock *MLock) UnLock() error {
	if lock.conn == nil {
		return notLockedErr
	}

	released, err := lock.execute("select release_lock(?)", lock.name)
	if err != nil {
		return err
	}

	if !released {
		// 说明数据库里没锁上，可能是bug导致，或者在多协程环境下使用了锁
		return  notLockedErr
	}

	return lock.dropConn()
}

// todo: 测试垃圾回收时，没有Close的链接会被如何处理
func (lock *MLock) dropConn() error {
	defer func() {
		lock.conn = nil
	}()

	if lock.conn == nil {
		return nil
	}

	// todo: 重试
	return lock.conn.Close()
}

func (lock *MLock) execute(query string, args ...interface{}) (bool, error) {
	rowsAffected := 0
	err := lock.conn.QueryRowContext(context.TODO(), query, args...).Scan(&rowsAffected)
	if err != nil {
		return false, err
	}

	return rowsAffected == 1, nil
}
