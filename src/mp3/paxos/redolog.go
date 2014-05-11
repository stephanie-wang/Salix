package paxos

import "os"

const (
	KIND_GO_START = iota
  KIND_ADVANCE_VIEW = iota
  KIND_UPDATE_INSTANCE = iota
)

type Record interface {
  Kind() int
  Apply(px *Paxos)
}

//Record: GoStart --------------------------

type GoStart struct {
  KindEnum int
}

func MakeGoStart() *GoStart {
  return &GoStart {
    KindEnum: KIND_GO_START,
  }
}

func (rec *GoStart) Kind() int {
  return rec.KindEnum
}

func (rec *GoStart) Apply(px *Paxos) {
}

//Record: AdvanceView --------------------------

type AdvanceView struct {
  KindEnum int
  
  View int
}

func MakeAdvanceView(view int) *AdvanceView {
  return &AdvanceView {
    KindEnum: KIND_ADVANCE_VIEW,
    View: view,
  }
}

func (rec *AdvanceView) Kind() int {
  return rec.KindEnum
}

func (rec *AdvanceView) Apply(px *Paxos) {
  px.view = rec.View
}

//Record: UpdateInstance --------------------------

type UpdateInstance struct {
  KindEnum int
}

func MakeUpdateInstance() *UpdateInstance {
  return &UpdateInstance {
    KindEnum: KIND_UPDATE_INSTANCE,
  }
}

func (rec *UpdateInstance) Kind() int {
  return rec.KindEnum
}

func (rec *UpdateInstance) Apply(px *Paxos) {  
}

//RedoLog class

type RedoLog struct {
  px *Paxos
  filename string
  fd *os.File
  Enabled bool
}

// returns whether file or directory exists
func exists(name string) bool {
  if _, err := os.Stat(name); err != nil {
    if os.IsNotExist(err) {
      return false
    }
  }
  return true
}

func Startup(px *Paxos, filename string) *RedoLog {
  rlog := &RedoLog {
    px: px,
    filename: filename,
    Enabled: false,
  }

  /*
  if file exists
    open it
    enabled = false
    apply it
    enabled = true
  if file doesn't exist
    create it
    enabled = true
  */
    
  //_, _ := os.OpenFile(rlog.filename, os.O_RDWR, 0666)
  
  return rlog
}

func (rlog *RedoLog) applyLog() {
  /*
  for each line
    bytesread = 0
    read json and convert to object
    if fail
      truncate to bytesread
      break
    update bytesread
    apply record
  */
  
}

func (rlog *RedoLog) Log(record *Record) {
  /*
  convert to json
  append to file
  append "\n"
  flush/sync
  */
  
}
