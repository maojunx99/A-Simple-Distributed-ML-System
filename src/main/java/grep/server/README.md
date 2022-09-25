## How to run grep.server

* exectue command `javac grep.server/Server.java` under folder `cs425-mp`, this command will rebuild `Server.java` and all its relevant `.java` files
* exectue command `java grep.server/Server` under folder `cs425-mp`
* default port of Server is 8866

## Function of grep.server

* it returns the result of any Linux command
* if it receives the command "grep.test", grep.server will make a log file named 'grep.test.log' under folder `cs425-mp`, and send this log file to the grep.client
* grep.test log file has 100 random lines and three fixed lines
