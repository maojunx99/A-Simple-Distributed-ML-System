## How to run server

* exectue command `javac server/Server.java` under folder `cs425-mp`, this command will rebuild `Server.java` and all its relevant `.java` files
* exectue command `java server/Server` under folder `cs425-mp`
* default port of Server is 8866

## Function of server

* it returns the result of any Linux command
* if it receives the command "test", server will make a log file named 'test.log' under folder `cs425-mp`, and send this log file to the client
* test log file has 100 random lines and three fixed lines
