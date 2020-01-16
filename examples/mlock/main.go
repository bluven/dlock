package main

import (
	"fmt"
	"time"

	"github.com/bluven/dlock/mlock"
)

func main() {
	lock, err := mlock.NewLock("test", "root:root@tcp(localhost:3306)/?parseTime=true")
	if err != nil {
		panic(err)
	}

	go test("bluven", lock)
	go test("blabber", lock)

	select {
	}
}

func test(id string, lock *mlock.MLock) {
	fmt.Printf("%s begin to get lock\n", id)

	locked, err := lock.Lock(5)
	if err != nil {
		panic(err)
	}

	if !locked {
		fmt.Printf("%s didn't get lock\n", id)
		return
	}

	fmt.Printf("%s got lock\n", id)

	time.Sleep(10 * time.Second)

	fmt.Printf("%s try to released lock\n", id)
	err = lock.UnLock()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s released lock\n", id)
}
