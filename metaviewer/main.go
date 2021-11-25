package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/mojura/kiroku"
)

func main() {
	flag.Parse()
	filename := flag.Arg(0)
	if len(filename) == 0 {
		fmt.Println("filename is empty, please provide a filename")
		os.Exit(1)
	}

	err := kiroku.Read(filename, func(r *kiroku.Reader) (err error) {
		meta := r.Meta()
		fmt.Printf("%+v\n", meta)
		return
	})

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
