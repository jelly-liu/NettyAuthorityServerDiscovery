# Netty Authority Serveice Discovery Project

An simple authority project write with java language, and Netty, ZooKeeper, Curator, Curator-Service-Discovery

## Technical Architecture
1. Netty
2. ZooKeeper
3. Curator, Curator Service Discovery
4. Google ProtoBuf

## How to build

* Intellij IDEA 2016
* JDK1.8+
* import module with Maven3

## How to run
1. run NettyAuthorityClient, only one times, this client will watch all the time if new server created or not
2. for each time when you run NettyAuthorityServer, will start 1 new Server and register to zookeeper, then client will discovery new server
3. that's all, see the log

##Architecture
![alt text](http://img.blog.csdn.net/20160825123128945)
