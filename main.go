package main

import ("fmt"
"bitbucket.org/nsaje/dagger/producers")

func main() {
	p := ProducerPlugin{Name: "test producer"}
	p.Init()
	go p.Produce()
	for {
		val := <-p.Streams["test stream"]
		fmt.Printf("%v\n", val)
	}
}
