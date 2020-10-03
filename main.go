package main

import "os"

func main() {
	start := ""
	if len(os.Args) > 1 {
		start = os.Args[1]
	}
	Run(start)
}
