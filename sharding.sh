#!/bin/bash

# start config server
mongod --configsvr --replSet cfgrs --port 10001 --dbpath /data/db
mongod --configsvr --replSet cfgrs --port 10002 --dbpath /data/db
mongod --configsvr --replSet cfgrs --port 10003 --dbpath /data/db

# start shard server
mongod --shardsvr --replSet shard1rs --port 20001 --dbpath /data/db
mongod --shardsvr --replSet shard1rs --port 20002 --dbpath /data/db
mongod --shardsvr --replSet shard1rs --port 20003 --dbpath /data/db

# start query router
mongos --configdb cfgrs/10.0.2.15:10001,10.0.2.15:10002,10.0.2.15:10003 --bind_ip 0.0.0.0 --port 30000

# connect one mongos
mongo mongodb://10.0.2.15:20001

# Initiate the replica set 
rs.initiate(
  {
    _id: "shard1rs",
    members: [
      { _id : 0, host : "10.0.2.15:20001" },
      { _id : 1, host : "10.0.2.15:20002" },
      { _id : 2, host : "10.0.2.15:20003" }
    ]
  }
)

#  Connect to the Sharded Cluster
mongo mongodb://[mongos-ip-address]:[mongos-port]

# Add Shards to the Cluster
sh.addShard("[shard-replica-set-name]/[shard-replica-1-ip]:[port],[shard-replica-2-ip]:[port],[shard-replica-3-ip]:[port]")
sh.status()

# Enable Sharding for a Database
sh.enableSharding("[database-name")

# Shard a Collection
sh.shardCollection("[database].[collection]", { [field]: 1 } )
sh.shardCollection("[database].[collection]", { [field]: "hashed" } )


