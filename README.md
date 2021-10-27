# Simple TCP PubSub Server

Simulate a Redis PubSub server using Netty and Java.

Can be test on netcat:

```shell
$ nc localhost 1234
subscribe topic1 topic2
Subscribed to topic1, topic2
publish topic1 hello
hello
Published to topic topic1
```
