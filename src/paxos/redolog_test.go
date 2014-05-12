package paxos

import "testing"
import "fmt"
import "os"

func TestBasic(t *testing.T) {

  fmt.Println("hello world")
  
  //create a fresh log
  os.Remove("t.txt")
  rlog := Startup(nil, "t.txt")
  
  //add things to it
  rlog.Log(MakeAdvanceView(5))
  
  //open it again to see log
  rlog = Startup(nil, "t.txt")
}
