Salix
=====

Getting started:

1. Set your gopath to the salix directory.
2. Run this command from inside salix/src to start up an instance.
    
    `$ go run salix.go -command=Start`
    
    This command will print out some mysterious-looking lines. The first line
    to look for begins with "smh:". These are the host addresses for the
    shardmasters. You'll need that to do your filesystem ops.

    This command will loop until "exit" is typed into stdin.

3. To run a command, use the same syntax as above, this time with command from
   the following list: Read, Write, Remove. You'll need to specify a filename
   for any of these, and don't forget to include the smh value from above!

   `$ go run salix.go -command=[Read, Write, Remove] -filename=[filename here
   :)] -smh=[random string from Start output]`

   A write command will print out the desired file's contents immediately after
   the write op is finished. 

4. Check out this command for help with the filesystem command flags:

    `$go run salix.go -help`
