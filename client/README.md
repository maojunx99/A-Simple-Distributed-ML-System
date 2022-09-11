## How to run client

* clone this repo to your vm
* include your server vms in `client/serverAddress.properties`
    * `amount` - this client will call `amount` servers from top to down in the `serverAddress.properties`
    * `serveri` - ith server's address
    * `porti` - ith server's port
    * `statei` -ith server's state (initialize it with `ACTIVE`)
    * **note that i should start from 0 and max i equals to or greater than `amount` - 1 (servers with number that larger than `amount` - 1 will be ignored by client)**
* (optional) exectue command `javac client/Client.java` under folder `cs425-mp`, this command will rebuild `Client.java` and all its relevant `.java` files
* exectue command `java client/Client` under folder `cs425-mp`
* input grep command following instructions, for example `grep -c "GET\|PUT" ~/mp1/vm*.log` replace **~/mp1/vm\*.log** with the location you put your log files on other vms
* input `CTRL+C` to stop the process if you want to