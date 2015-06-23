package main

import (
    "fmt"
    "github.com/nsaje/dagger/dagger"
)

func main() {
	p := Producer{Name: "test producer"}
	p.Init()
	go p.Produce()
	for {
		val := <-p.Stream
		fmt.Printf("%v\n", val)
	}
}
