package main

import "shardkv"
import "flag"

var command = flag.String("command", "Read", "Command can be Read, Write, Remove")
var filename = flag.String("filename", "", "Provide a filename to read, write, remove")
var contents = flat.Int("contents", "", "In write command, the contents to write to file")
var bytes = flag.Int("bytes", -1, "Number of bytes to read, or none to read whole file")
var offset = flag.Int("offset", 0, "Offset to start reading from")
var stale = flag.Bool("stale", false, "In read command, set to true to do a stale read")
var doAppend = flag.Bool("doAppend", false, "In write command, set to true to append instead of overwriting a file")
var doHash = flag.Bool("doHash", false, "In write command, set to true to write hashed of previous value")

func main() {
  flag.Parse()
  if *filename == "" {
    println("must specify a filename")
    return
  }
  //TODO: actually implement this...
  ck := MakeClerk([]string{})
  switch *command {
  case shardkv.Read:
    v := ck.Read(*filename, *bytes, int64(*offset), *stale)
    println(string(v))
  case shardkv.Write:
    if doHash {
      ck.WriteHash(*filename, *contents, *doAppend, *stale)
    } else {
      ck.Write(*filename, *contents, *doAppend, *stale)
    }
    v := ck.Read(*filename, *bytes, int64(*offset), *stale)
    println("wrote:")
    println(string(v))
  case shardkv.Remove:
    ck.Remove(*filename)
  }
}
