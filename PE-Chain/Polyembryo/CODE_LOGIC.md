### 协议逻辑

中继分片 id: 1 2 ...
业务分片 id: 1001 1002 1003 ...

分片安全由验证者聚合签名保证 多个中继分片存区块头 

所有节点都加入所有topic并只订阅自己分片的topic 中继分片节点根据区块头中outboundDst在相应的topic中广播区块头

共识中 nodeId % nodeNumPerShard == 0 的是leader（config中设置的）

commit后shard中各节点都需要广播到对方shard

inboundChunk收到后先放到shardstate里面，如果对应的header已经存在则验证一下通过就放到txpool里，如果对应的header不在则等到header收到时验证再放到txpool里 

往外发的时候，outboundchunk中交易数为0就不发了，outboundbitmap也要设好

blockchain里createNewBlock时outboundChunks的key加1001才是shardId

### 冷启动

bls/keygen/keygen.go 生成公私钥存在bls/keygen/*.keystore

组网后，节点首先根据config.json的nodeId计算shardId加入并订阅topic

### 片内共识 HotStuff

非流水线hotstuff，prepare pre-commit commit 三阶段
