package raft

import "log"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func Min(x int, y int) int {
	if x <= y {
		return x
	}
	return y
}

func Max(x int, y int) int {
	if x >= y {
		return y
	}
	return x
}
