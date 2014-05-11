package paxos

const (
	KIND_GO_START = iota
  KIND_UPDATE_DONE_VALS = iota
  KIND_UPDATE_INSTANCE_DECIDED = iota
  KIND_UPDATE_INSTANCE_ACCEPTED = iota
)

type Record interface {
  Kind() int
  Apply()
}

type GoStart struct {
}

func (rec *GoStart) Kind() int {
  return 0
}

func (rec *GoStart) Apply() {
  
}

/*
Record
  Type
  
  GoStart
  UpdateDoneVals
  UpdateInstanceDecided
  UpdateInstanceAccepted

RedoLog
  px
  filename
  fd
  enabled

Startup(px, filename)
  open file for 
  if file exists
    enabled = false
    apply it
    enabled = true
  if file doesn't exist
    create it
    enabled = true

ApplyLog()
  for each line
    read
    convert to json
    if fail
      truncate bytesread
      break
    update bytesread
    apply record

ApplyRecord(p)

Log(Record)
  convert to json
  append to file
  append "\n"
  flush/sync
*/