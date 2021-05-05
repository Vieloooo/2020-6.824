package raft

import "log"

// Debugging
const Debug = 1
const leaderdebug = 1
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
func LPrintf(format string, a ...interface{}) (n int, err error) {
	if leaderdebug > 0 {
		log.Printf(format, a...)
	}
	return
}
