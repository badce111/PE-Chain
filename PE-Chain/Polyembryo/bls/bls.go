package mitosisbls

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/KyrinCode/Mitosis/config"
	"github.com/KyrinCode/Mitosis/types"
	"github.com/herumi/bls-eth-go-binary/bls"
	logger "github.com/sirupsen/logrus"
)

var logBLS = logger.WithField("store", "BLS")

type BLSKeyPair struct {
	Sec bls.SecretKey  `json:"secretKey"` // 私钥
	Pub *bls.PublicKey `json:"publicKey"` // 公钥
}

type BLS struct {
	KeyPair BLSKeyPair // 公私钥结构体

	PubKeys *PubKeyStore // 分片公钥结构体指针
}

func NewKeyPair() (string, string) {
	bls.Init(bls.BLS12_381)
	bls.SetETHmode(bls.EthModeDraft07)
	var sec bls.SecretKey
	sec.SetByCSPRNG()
	fmt.Printf("sec: %s\n", sec.SerializeToHexStr())
	pub := sec.GetPublicKey()
	fmt.Printf("pub: %s\n", pub.SerializeToHexStr())
	return sec.SerializeToHexStr(), pub.SerializeToHexStr()
}

func LoadKeyPair(filename string) (*BLSKeyPair, bool) {
	bls.Init(bls.BLS12_381)
	bls.SetETHmode(bls.EthModeDraft07)

	kpStr := make(map[string]string)
	data, err := os.ReadFile(filename)
	if err != nil {
		logger.Error("Read keystore file error 读取密钥库文件错误")
		return nil, false
	}
	keyJson := []byte(data)
	err = json.Unmarshal(keyJson, &kpStr)
	if err != nil {
		logger.Error("Unmarshal json data error 解压缩 json 数据错误")
		return nil, false
	}

	var sec bls.SecretKey
	err = sec.DeserializeHexStr(kpStr["secretKey"])
	if err != nil {
		logger.Error("反序列化密钥错误")
		return nil, false
	}

	return &BLSKeyPair{
		Sec: sec,
		Pub: sec.GetPublicKey(),
	}, true
}

func SaveKeyPair(filename, secStr, pubStr string) error {
	kpStr := make(map[string]string)
	kpStr["secretKey"] = secStr
	kpStr["publicKey"] = pubStr

	var prettyJSON bytes.Buffer
	data, err := json.Marshal(kpStr)
	err = json.Indent(&prettyJSON, data, "", "\t")
	if err != nil {
		logger.Fatal(err)
	}
	fp, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		logger.Fatal("4:", err)
		return err
	}
	_, err = fp.Write(prettyJSON.Bytes())
	if err != nil {
		log.Fatal("5:", err)
		return err
	}
	return nil
}

func NewBLS(kp BLSKeyPair, topo config.Topology, nodes map[uint32][]uint32) *BLS {
	pubKeys := NewPubKeyStore(topo)
	pubKeys.Reset(nodes) // 映射公钥
	return &BLS{
		KeyPair: kp,
		PubKeys: pubKeys,
	}
}

func (b *BLS) SignBytes(msg []byte) []byte {
	sig := b.KeyPair.Sec.SignByte(msg)
	return sig.Serialize()
}

// 对 msg 进行签名
func (b *BLS) Sign(msg []byte) *bls.Sign {
	sig := b.KeyPair.Sec.SignByte(msg)
	return sig
}

// 初始化 bls 后应用 LoadKeyPair 从文件中载入所有其他节点的 pubkey
func (b *BLS) AddPubKey(shardId, nodeId uint32, pubByte []byte) bool { // 传 pub.Serialize()
	return b.PubKeys.AddPubKey(shardId, nodeId, pubByte)
}

// 验证公钥与消息的正确性
func (b *BLS) VerifyAggregateSig(sigBytes, msg []byte, bitmap types.Bitmap, shardId, offset uint32) bool {
	var sig bls.Sign
	err := sig.Deserialize(sigBytes) // 反序列化（解码），将字节切片反序列化为 BLS 签名
	if err != nil {
		fmt.Println(err)
	}
	pubKey := b.PubKeys.GetAggregatePubKey(shardId, bitmap, offset)
	return sig.VerifyByte(pubKey, msg) // 验证消息是否被提供公钥所对应的私钥签名
}

// 验证签名
func (b *BLS) VerifySign(sigBytes, msg []byte, nodeId, shardId uint32) bool {
	var sig bls.Sign
	err := sig.Deserialize(sigBytes) // 反序列化（解码），将字节切片反序列化为 BLS 签名
	if err != nil {
		fmt.Println(err)
	}
	pubKey := b.PubKeys.GetPubKey(shardId, nodeId)
	return sig.VerifyByte(pubKey, msg) // 验证消息是否被提供公钥所对应的私钥签名
}

func (b *BLS) GetPubKey() []byte {
	return b.KeyPair.Pub.Serialize()
}

func (b *BLS) GetPubKeyWithNodeId(shardId, nodeId uint32) []byte {
	return b.PubKeys.GetPubKey(shardId, nodeId).Serialize()
}
