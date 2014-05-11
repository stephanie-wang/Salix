package paxos

import "os"
import "log"
import "encoding/json"
import "bufio"
import "fmt"

const (
	KIND_GO_START = "GoStart"
  KIND_ADVANCE_VIEW = "AdvanceView"
  KIND_UPDATE_INSTANCE = "UpdateInstance"
)

type Record interface {
  Apply(px *Paxos)
}

type GenericRecord struct {
  Kind string
}

///////////////////////////////////////

type GoStart struct {
  Kind string
  Seq int
  V interface{}
}

func MakeGoStart(seq int, v interface{}) *GoStart {
  return &GoStart {
    Kind: KIND_GO_START,
    Seq: seq,
    V: v,
  }
}

func (rec *GoStart) Apply(px *Paxos) {
  px.Start(seq, v)
}

///////////////////////////////////////

type AdvanceView struct {
  Kind string  
  View int
}

func MakeAdvanceView(view int) *AdvanceView {
  return &AdvanceView {
    Kind: KIND_ADVANCE_VIEW,
    View: view,
  }
}

func (rec *AdvanceView) Apply(px *Paxos) {
  px.view = rec.View
}

///////////////////////////////////////

type UpdateInstance struct {
  Kind string
  
  Seq int
  
  Accepted bool
  View_a int
  V_a interface{}
  
  Decided bool
  DecidedVal interface{} 
}

func MakeInstanceAccepted(seq int, view_a int, v_a interface{}) *UpdateInstance {
  rec := makeUpdateInstance(seq)
  rec.Accepted = true
  rec.View_a = view_a
  rec.V_a = v_a
  return rec
}

func MakeInstanceDecided(seq int, decidedVal interface{}) *UpdateInstance {
  rec := makeUpdateInstance(seq)
  rec.decided = true
  rec.decidedVal = decidedVal
  return rec
}

func makeUpdateInstance(seq int) *UpdateInstance {
  return &UpdateInstance {
    Kind: KIND_UPDATE_INSTANCE,
    Seq: seq,
  }
}

func (rec *UpdateInstance) Apply(px *Paxos) {  
  inst := px.GetInstance(seq)
  if rec.Accepted {
    inst.Accepted = rec.Accepted
    inst.View_a = rec.View_a
    inst.V_a = rec.V_a
  }
  if rec.Decided {
    inst.Decided = rec.Decided
    inst.DecidedVal = rec.DecidedVal
  }
}

///////////////////////////////////////

type RedoLog struct {
  px *Paxos
  filename string
  f *os.File
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

  existed := exists(rlog.filename)

  f, err := os.OpenFile(rlog.filename, os.O_CREATE | os.O_RDWR, 0666)
  if err != nil {
    log.Fatalf("cannot open file: %v", err)
  }
  rlog.f = f

  if existed {
    rlog.Enabled = false
    rlog.applyLog()
  }
  rlog.Enabled = true
  
  return rlog
}

func (rlog *RedoLog) applyLog() {
  rlog.f.Seek(0, 0)
  
  bytesRead := 0
  scanner := bufio.NewScanner(rlog.f)
  
  for scanner.Scan() {
    line := string(scanner.Bytes())
    fmt.Println("LINE:",line)
    
    var unknown GenericRecord
    err := json.Unmarshal(scanner.Bytes(), &unknown)
		if err != nil {
      //truncate to bytesread
			break
		}

    var rec Record

    switch unknown.Kind {
      case KIND_GO_START:
        var specific GoStart
        err = json.Unmarshal(scanner.Bytes(), &specific)
        rec = &specific
      case KIND_ADVANCE_VIEW:
        var specific AdvanceView
        err = json.Unmarshal(scanner.Bytes(), &specific)
        rec = &specific
      case KIND_UPDATE_INSTANCE:
        var specific UpdateInstance
        err = json.Unmarshal(scanner.Bytes(), &specific)
        rec = &specific
      default:
        log.Fatal("unrecognized record type")
        return
    }
    
    //update bytesread
    _ = bytesRead
    
    fmt.Println("DESERIALIZED:", rec)
    if rlog.px != nil {
      rec.Apply(rlog.px)
    }
  }
  
  //truncate to bytesread
  //add a checkpoint
}

func (rlog *RedoLog) Log(record Record) {  
  b, err := json.Marshal(record)
  if err != nil {
		log.Fatal(err)
    return
	}
  
  _, err = rlog.f.WriteString(string(b) + "\n")
  rlog.f.Sync()
  
  //if too many records
  //truncate and add a checkpoint
}
