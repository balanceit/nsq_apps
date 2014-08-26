#logging using nsq and rethinkdb

## Dependencies
* __nsq__ ```brew install nsq```
* __nodejs__ ```brew install node```
* __docker__ ```brew install docker```
* __rethinkdb__ ```brew install rethinkdb```
* __boot2docker__ ```brew install boot2docker```

## Configure and setup

### nsq and docker

* get the offical nsq docker images for both:
  * __nsqd__ ```docker pull nsqio/nsqd```
  * __nsqlookupd__ ```docker pull nsqio/nsqlookupd```
* map the vm ports
 ```
   4150 -> 4150
   4151 -> 4151
   4160 -> 4160
   4161 -> 4161
```

> __note__: the vm will have to be stopped `boot2docker down` inorder to map the ports

* start the container instances
  * `docker run -d --name nsqd -p 4150:4150 -p 4151:4151     nsqio/nsqd     --broadcast-address=127.0.0.1     --lookupd-tcp-address=172.17.42.1:4160`
  * `docker run -d --name lookupd -p 4160:4160 -p 4161:4161 nsqio/nsqlookupd`

> __note__: this assumes that the vm is running on `172.17.42.1`

### rethinkdb
Start rethinkdb with `rethinkdb` then browse to http://localhost:8080

### rethinkdb and docker

* `docker pull dockerfile/rethinkdb`
* run it up in docker `docker run -d -p 8080:8080 -p 28015:28015 -p 29015:29015 dockerfile/rethinkdb`
* map the ports
```
8080:8080
28015:28015
29015:29015
```
* check all is well by browsing to http://localhost:8080
