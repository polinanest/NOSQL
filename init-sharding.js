rs.initiate({
  _id: "configReplSet",
  configsvr: true,
  members: [
    { _id: 0, host: "configsvr1:27017" },
    { _id: 1, host: "configsvr2:27017" },
    { _id: 2, host: "configsvr3:27017" }
  ]
})

rs.initiate({
  _id: "shard1ReplSet",
  members: [
    { _id: 0, host: "shard1a:27017" },
    { _id: 1, host: "shard1b:27017" }
  ]
})


rs.initiate({
  _id: "shard2ReplSet",
  members: [
    { _id: 0, host: "shard2a:27017" },
    { _id: 1, host: "shard2b:27017" }
  ]
})

sh.addShard("shard1ReplSet/shard1a:27017,shard1b:27017")
sh.addShard("shard2ReplSet/shard2a:27017,shard2b:27017")


sh.enableSharding("university")

sh.shardCollection("university.students", { "group_id": "hashed" })


sh.status()