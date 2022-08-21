package dao

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/rqzrqh/sync_filecoin/model"
	"github.com/shopspring/decimal"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

const (
	CacheTimeout time.Duration = 3600 * time.Second
)

type HeadChainInfo struct {
	ID           uint64 `json:"id"`
	ParentID     uint64 `json:"parent_id"`
	Height       int64  `json:"height"`
	ParentHeight int64  `json:"parent_height"`
}

type MessageDigest struct {
	Value decimal.Decimal `json:"value"`
}

type ExecutionTraceMessageDigest struct {
	Value decimal.Decimal `json:"value"`
}

type BlockHeaderDigest struct {
	Height                  int64           `json:"height"`
	Timestamp               int64           `json:"timestamp"`
	Miner                   string          `json:"miner"`
	Size                    int             `json:"size"`
	Reward                  decimal.Decimal `json:"reward"`
	Penalty                 decimal.Decimal `json:"penalty"`
	WinCount                int64           `json:"wincount"`
	MessageCount            int             `json:"msg_count"`
	UnProcessedMessageCount int             `json:"unprocessed_msg_count"`
	ParentWeight            decimal.Decimal `json:"parentWeight"`
	ParentStateRoot         string          `json:"parentStateRoot"`
	ParentBaseFee           decimal.Decimal `json:"parentBaseFee"`
	ParentHeight            int64           `json:"parentHeight"`
}

type TipSetDigest struct {
	ID        uint64 `json:"id"`
	ParentID  uint64 `json:"parent_id"`
	Height    int64  `json:"height"`
	Timestamp int64  `json:"timestamp"`

	Blocks                  []string `json:"blocks"`
	BlockCount              int      `json:"blockcount"`
	TotalMessageCount       int      `json:"total_msg_count"`
	DedupedMessageCount     int      `json:"deduped_msg_count"`
	UnProcessedMessageCount int      `json:"unprocessed_msg_count"`

	ParentStateRoot string          `json:"parent_stateroot"`
	ParentWeight    decimal.Decimal `json:"parent_weight"`
	ParentBaseFee   decimal.Decimal `json:"parent_basefee"`

	Reward  decimal.Decimal `json:"reward"`
	Penalty decimal.Decimal `json:"penalty"`
}

type HeightData struct {
	ID           uint64 `json:"id"`
	Height       int64  `json:"height"`
	ParentID     uint64 `json:"parent_id"`
	ParentHeight int64  `json:"parent_height"`
}

var messageDigestKey = "msg_digest"
var messageInBlockKey = "msg_in_block"
var blockDigestKey = "blk_digest"
var blockMessageList = "blk_msg_list"

var minerBlockList = "miner_block_list"
var accountMessageList = "account_msg_list"
var accountBookList = "account_book_list"

var addressToIdKey = "addr_to_id"
var idToAddressKey = "id_to_addr"

var tipSetDigestKey = "tipset_digest"

var chainMessageList = "chain_msg_list"
var chainHeightList = "chain_height_list"
var chainHeadKey = "chainhead"

var accountMsgTypeSend = "send"
var accountMsgTypeRecv = "recv"

var chainNotify = "chain_notify"

func BuildMessageDigestKey(msgCid string) string {
	return messageDigestKey + "_" + msgCid
}

func BuildMessageInBlockKey(msgCid string) string {
	return messageInBlockKey + "_" + msgCid
}

func BuildBlockDigestKey(blkCid string) string {
	return blockDigestKey + "_" + blkCid
}

func BuildBlockMessageListKey(blkCid string) string {
	return blockMessageList + "_" + blkCid
}

func BuildMinerBlockListKey(minerCid string) string {
	return minerBlockList + "_" + minerCid
}

func BuildAccountMessageListKey(accountCid string) string {
	return accountMessageList + "_" + accountCid
}

func BuildAccountMessageSendListKey(accountCid string) string {
	return accountMessageList + "_" + accountCid + "_" + accountMsgTypeSend
}

func BuildAccountMessageRecvListKey(accountCid string) string {
	return accountMessageList + "_" + accountCid + "_" + accountMsgTypeRecv
}

func BuildAccountBookListKey(accountCid string) string {
	return accountBookList + "_" + accountCid
}

func BuildAddressToIdKey(addr string) string {
	return addressToIdKey + "_" + addr
}

func BuildIdToAddressKey(id string) string {
	return idToAddressKey + "_" + id
}

func BuildAccountBookMethodListKey(accountCid string, method uint64) string {
	return accountBookList + "_" + accountCid + "_" + strconv.FormatUint(method, 10)
}

func BuildTipSetDigestKey(height int64) string {
	return tipSetDigestKey + "_" + strconv.FormatInt(height, 10)
}

func BuildChainMessageListKey() string {
	return chainMessageList
}

func BuildChainHeightListKey() string {
	return chainHeightList
}

func BuildChainHeadKey() string {
	return chainHeadKey
}

func BuildChainNotifyKey() string {
	return chainNotify
}

func getFullTipSet(ctx context.Context, db *gorm.DB, height int64, tsID uint64) (*model.TipSet, []*model.BlockHeader, map[string]string, map[string]string, [][]*model.UserMessage, []*model.ExecutionTraceMessage, error) {
	var tipset model.TipSet
	result := db.Model(&model.TipSet{}).Where("id = ?", tsID).Take(&tipset)
	if result.Error != nil {
		panic("get tipset failed")
	}
	if result.RowsAffected != 1 {
		panic("get tipset failed. count error")
	}

	// get blocks
	var tipsetBlockList []model.TipSetBlock
	if err := db.Model(&model.TipSetBlock{}).Where("height = ? AND tipset_id = ?", height, tsID).Find(&tipsetBlockList).Error; err != nil {
		log.Errorf("buildTipSet failed:%v", err)
		return nil, nil, nil, nil, nil, nil, err
	}

	blockHeaderSet := make(map[uint64]uint16)
	blockHeaderIDList := make([]uint64, 0)
	for _, v := range tipsetBlockList {
		blockHeaderSet[v.BlockID] = v.IndexInTipSet
		blockHeaderIDList = append(blockHeaderIDList, v.BlockID)
	}

	var results []model.BlockHeader
	if err := db.Where("id in (?)", blockHeaderIDList).Find(&results).Error; err != nil {
		log.Errorf("buildTipSet failed:%v", err)
		return nil, nil, nil, nil, nil, nil, err
	}

	blocks := make([]*model.BlockHeader, len(results))
	for _, v := range results {
		idx, exist := blockHeaderSet[v.ID]
		if !exist {
			log.Errorf("buildTipSet failed, blockid not found index %v", v.ID)
			return nil, nil, nil, nil, nil, nil, xerrors.New("blockid not found index")
		}

		modelBlock := model.BlockHeader{
			ID:           v.ID,
			Height:       v.Height,
			Cid:          v.Cid,
			Miner:        v.Miner,
			Size:         v.Size,
			Reward:       v.Reward,
			Penalty:      v.Penalty,
			WinCount:     v.WinCount,
			MessageCount: v.MessageCount,
			IsOrphan:     v.IsOrphan,
			Raw:          v.Raw,
		}
		blocks[idx] = &modelBlock
	}

	/*
		var parentBlockCids []string
		if err := db.Model(&model.BlockHeader{}).Select("cid").Where("height = ? AND id = ?", height, tipset.ParentTipSetID).Order("index_in_tipset").Scan(&parentBlockCids).Error; err != nil {
			return nil, nil, nil, nil, nil, err
		}
	*/
	var modelInitActorList []model.InitActor
	if err := db.Model(&model.InitActor{}).Where("height = ? AND tipset_id = ?", height, tsID).Find(&modelInitActorList).Error; err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}

	addressToIdMap := make(map[string]string)
	idToAddressMap := make(map[string]string)
	for _, v := range modelInitActorList {
		addressToIdMap[v.Address] = v.ActorID
		idToAddressMap[v.ActorID] = v.Address
	}

	// find block messages
	allBlockMessages := make([][]*model.UserMessage, len(blocks))
	for i, v := range blocks {
		var blockMessageRelation []model.BlockMessageRelation

		if err := db.Model(&model.BlockMessageRelation{}).Where("block_id = ?", v.ID).Order("index_in_block").Find(&blockMessageRelation).Error; err != nil {
			return nil, nil, nil, nil, nil, nil, err
		}

		var messageIDList []uint64
		messageIndexMap := make(map[uint64]uint16)
		for _, v := range blockMessageRelation {
			messageIDList = append(messageIDList, v.MessageID)
			messageIndexMap[v.MessageID] = uint16(v.IndexInBlock)
		}

		var modelBlockUserMessages []model.UserMessage
		if err := db.Model(&model.UserMessage{}).Where("id in (?)", messageIDList).Find(&modelBlockUserMessages).Error; err != nil {
			return nil, nil, nil, nil, nil, nil, err
		}

		blockMessages := make([]*model.UserMessage, len(modelBlockUserMessages))
		for _, msg := range modelBlockUserMessages {
			idx, exist := messageIndexMap[msg.ID]
			if !exist {
				panic(0)
			}

			blockMessages[idx] = &msg
		}

		allBlockMessages[i] = blockMessages
	}

	var traceMessages []*model.ExecutionTraceMessage
	if err := db.Model(&model.ExecutionTraceMessage{}).Where("height = ? AND tipset_id = ?", height, tsID).Find(&traceMessages).Error; err != nil {
		panic("get trace message failed")
	}

	return &tipset, blocks, addressToIdMap, idToAddressMap, allBlockMessages, traceMessages, nil
}

type AppendInfo struct {
	Key   string
	Value string
}

func getFromMessage(height int64, msg *model.UserMessage) (map[string]string, []*AppendInfo) {

	set := make(map[string]string)
	addTo := make([]*AppendInfo, 0)

	// raw msg
	{
		key := BuildMessageDigestKey(msg.Cid)
		msgDigest := MessageDigest{
			Value: msg.Value,
		}
		byteMsgDigest, _ := json.Marshal(&msgDigest)
		set[key] = string(byteMsgDigest)
	}

	{
		key := BuildAccountMessageListKey(msg.From)
		addTo = append(addTo, &AppendInfo{Key: key, Value: msg.Cid})
	}

	{
		key := BuildAccountMessageSendListKey(msg.From)
		addTo = append(addTo, &AppendInfo{Key: key, Value: msg.Cid})
	}

	{
		key := BuildAccountMessageListKey(msg.To)
		addTo = append(addTo, &AppendInfo{Key: key, Value: msg.Cid})
	}

	{
		key := BuildAccountMessageRecvListKey(msg.To)
		addTo = append(addTo, &AppendInfo{Key: key, Value: msg.Cid})
	}

	{
		key := BuildChainMessageListKey()
		addTo = append(addTo, &AppendInfo{Key: key, Value: msg.Cid})
	}

	return set, addTo
}

func getFromExecutionTraceMessage(msg *model.ExecutionTraceMessage) (map[string]string, []*AppendInfo) {
	set := make(map[string]string)
	addTo := make([]*AppendInfo, 0)

	{
		// raw
		key := BuildMessageDigestKey(msg.Cid)
		etMsgDigest := ExecutionTraceMessageDigest{
			Value: msg.Value,
		}

		byteEtMsgDigest, _ := json.Marshal(&etMsgDigest)
		set[key] = string(byteEtMsgDigest)
	}

	{
		key := BuildAccountBookListKey(msg.From)
		addTo = append(addTo, &AppendInfo{Key: key, Value: msg.Cid})
	}

	{
		key := BuildAccountBookMethodListKey(msg.From, msg.Method)
		addTo = append(addTo, &AppendInfo{Key: key, Value: msg.Cid})
	}

	{
		key := BuildAccountBookListKey(msg.To)
		addTo = append(addTo, &AppendInfo{Key: key, Value: msg.Cid})
	}

	{
		key := BuildAccountBookMethodListKey(msg.To, msg.Method)
		addTo = append(addTo, &AppendInfo{Key: key, Value: msg.Cid})
	}

	return set, addTo
}

func getHeightChangedData(ctx context.Context, db *gorm.DB, height int64, tsID uint64) (map[string]string, []*AppendInfo, error) {
	tipset, blocks, addressToIdMap, idToAddressMap, allBlockMessages, traceMessages, err := getFullTipSet(ctx, db, height, tsID)
	if err != nil {
		panic(err)
	}

	allSet := make(map[string]string)
	allAddTo := make([]*AppendInfo, 0)

	messageInBlocks := make(map[string][]string)
	for i, blockMessages := range allBlockMessages {
		blockCid := blocks[i].Cid
		for _, msg := range blockMessages {
			blockList, exist := messageInBlocks[msg.Cid]
			if exist {
				blockList = append(blockList, blockCid)
				messageInBlocks[msg.Cid] = blockList
			} else {
				var blockList []string
				blockList = append(blockList, blockCid)
				messageInBlocks[msg.Cid] = blockList
			}
		}
	}
	for k, v := range messageInBlocks {
		key := BuildMessageInBlockKey(k)
		value, _ := json.Marshal(v)
		allSet[key] = string(value)
	}

	for k, v := range addressToIdMap {
		key := BuildAddressToIdKey(k)
		allSet[key] = string(v)
	}
	for k, v := range idToAddressMap {
		key := BuildIdToAddressKey(k)
		allSet[key] = string(v)
	}

	allUserMessages := make(map[string]*model.UserMessage)
	for _, blockMessages := range allBlockMessages {
		for _, msg := range blockMessages {
			allUserMessages[msg.Cid] = msg
		}
	}

	for _, msg := range allUserMessages {
		set, addTo := getFromMessage(height, msg)

		for k, v := range set {
			allSet[k] = v
		}

		allAddTo = append(allAddTo, addTo...)
	}

	for _, msg := range traceMessages {

		set, addTo := getFromExecutionTraceMessage(msg)

		for k, v := range set {
			allSet[k] = v
		}

		allAddTo = append(allAddTo, addTo...)
	}

	for i, blk := range blocks {

		{
			key := BuildBlockDigestKey(blk.Cid)
			digest := BlockHeaderDigest{
				Height:       blk.Height,
				Timestamp:    tipset.Timestamp,
				Miner:        blk.Miner,
				Size:         blk.Size,
				Reward:       blk.Reward,
				Penalty:      blk.Penalty,
				WinCount:     blk.WinCount,
				MessageCount: blk.MessageCount,
				//UnProcessedMessageCount: blk.UnProcessedMessageCount,
				ParentWeight:    tipset.ParentWeight,
				ParentStateRoot: tipset.ParentStateRoot,
				ParentBaseFee:   tipset.ParentBaseFee,
				ParentHeight:    tipset.ParentHeight,
			}
			value, _ := json.Marshal(digest)
			allSet[key] = string(value)
		}

		{
			key := BuildMinerBlockListKey(blk.Miner)
			value := blk.Cid
			allAddTo = append(allAddTo, &AppendInfo{Key: key, Value: value})
		}

		{
			key := BuildBlockMessageListKey(blk.Cid)
			for _, msg := range allBlockMessages[i] {
				allAddTo = append(allAddTo, &AppendInfo{Key: key, Value: msg.Cid})
			}
		}
	}

	strBlks := make([]string, len(blocks))
	for idx, blk := range blocks {
		strBlks[idx] = blk.Cid
	}

	{
		key := BuildTipSetDigestKey(height)
		ts := TipSetDigest{
			ID:                      tipset.ID,
			ParentID:                tipset.ParentTipSetID,
			Height:                  tipset.Height,
			Timestamp:               tipset.Timestamp,
			Blocks:                  strBlks,
			BlockCount:              tipset.BlockCount,
			TotalMessageCount:       tipset.TotalMessageCount,
			DedupedMessageCount:     tipset.DedupedMessageCount,
			UnProcessedMessageCount: tipset.UnProcessedMessageCount,
			ParentStateRoot:         tipset.ParentStateRoot,
			ParentWeight:            tipset.ParentWeight,
			ParentBaseFee:           tipset.ParentBaseFee,
			Reward:                  tipset.Reward,
			Penalty:                 tipset.Penalty,
		}
		value, _ := json.Marshal(ts)
		allSet[key] = string(value)
	}

	{
		key := BuildChainHeightListKey()
		heightData := HeightData{
			ID:           tipset.ID,
			Height:       tipset.Height,
			ParentID:     tipset.ParentTipSetID,
			ParentHeight: tipset.ParentHeight,
		}
		value, _ := json.Marshal(heightData)
		allAddTo = append(allAddTo, &AppendInfo{Key: key, Value: string(value)})
	}

	return allSet, allAddTo, nil
}

func GetBatchHeightChangedData(ctx context.Context, db *gorm.DB, headChainList []*HeadChainInfo) (map[string]string, []*AppendInfo, error) {

	allSet := make(map[string]string)
	allAddTo := make([]*AppendInfo, 0)

	for _, ts := range headChainList {
		set, addTo, _ := getHeightChangedData(ctx, db, ts.Height, ts.ID)

		for k, v := range set {
			allSet[k] = v
		}

		allAddTo = append(allAddTo, addTo...)
	}

	return allSet, allAddTo, nil
}
