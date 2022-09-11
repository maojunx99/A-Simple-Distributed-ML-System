## How to run GrepTests

* clone this repo to your vm
* include your server vms in `test/activeServerAddress.properties` and `test/partiallyFailedServerAddress.properties`
    * `test/activeServerAddress.properties` is used by tests that all vms are active
    * `test/partiallyFailedServerAddress.properties` is used by tests that vms are partially failed, remember to add some failed vms into this file
    * `amount` - this client will call `amount` servers from top to down in the `serverAddress.properties`
    * `serveri` - ith server's address
    * `porti` - ith server's port
    * `statei` -ith server's state (initialize it with `ACTIVE`)
    * **note that i should start from 0 and max i equals to or greater than `amount` - 1 (servers with number that larger than `amount` - 1 will be ignored by client)**
* (optional) exectue command `javac test/GrepTests.java` under folder `cs425-mp`, this command will rebuild `GrepTests.java` and all its relevant `.java` files
* exectue command `java test/GrepTests` under folder `cs425-mp`
* input commands following instructions to run tests that you want to
* input `EOF` or `CTRL+C` to stop the process if you want to