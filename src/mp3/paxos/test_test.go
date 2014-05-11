package paxos

import "testing"
import "fmt"

func TestBasic(t *testing.T) {

  fmt.Println("hello world")
  
  //create a fresh log
  os.Remove("t.txt")
  rlog := Start(nil, "t.txt")
  
  //add things to it
  rlog.Log(MakeAdvanceView(5))
  
  //open it again to see log
  rlog = Start(nil, "t.txt")
}
