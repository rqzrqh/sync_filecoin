package dao

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	"github.com/rqzrqh/sync_filecoin/common"
	"github.com/rqzrqh/sync_filecoin/dao/fulltipset"
	"github.com/rqzrqh/sync_filecoin/model"
	"github.com/rqzrqh/sync_filecoin/util"
	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func GetDatabaseLock(db *gorm.DB) error {

	err := db.Migrator().CreateTable(&model.PidFile{})
	if err != nil {
		log.Errorf("GetDatabaseLock failed:%v", err)
	}

	return err
}

func ReleaseDatabaseLock(db *gorm.DB) error {
	err := db.Migrator().DropTable(&model.PidFile{})
	log.Infof("delete pid_file result:%v", err)
	return err
}

func CleanupDatabaseChain(ctx context.Context, db *gorm.DB) error {

	var invalidReversibleTipSets []model.ReversibleTipSet
	if err := db.Where("status != ?", model.TipSetStatusValid).Find(&invalidReversibleTipSets).Error; err != nil {
		log.Errorf("Cleanup failed:%v", err)
		return err
	}

	for _, model_reversible_tipset := range invalidReversibleTipSets {
		tsID := model_reversible_tipset.TipSetID
		tsk := model_reversible_tipset.Tsk
		height := model_reversible_tipset.Height
		parentHeight := model_reversible_tipset.ParentHeight
		parentTsID := model_reversible_tipset.ParentTipSetID

		var count int64
		if err := db.Model(&model.ReversibleTipSet{}).Where("parent_tsk = ?", model_reversible_tipset.ParentTsk).Count(&count).Error; err != nil {
			log.Errorf("Cleanup failed:%v", err)
			return err
		}

		deletePrevTsActorState := false
		if count == 1 {
			deletePrevTsActorState = true
		}

		log.Infof("###cleanup invalid fulltipset. tsID=%v height=%v status=%v tsk=%v", tsID, height, model_reversible_tipset.Status, tsk)

		if err := removeReversibleFullTipSet(ctx, db, tsID, tsk, height, parentTsID, parentHeight, deletePrevTsActorState); err != nil {
			log.Errorf("Cleanup err=%v", err)
			return err
		}
	}

	return nil
}

func GetDatabaseChainState(db *gorm.DB) (*types.TipSet, *types.TipSet, []*types.TipSet, map[types.TipSetKey][]address.Address, []*types.TipSet, error) {

	var genesisTs types.TipSet
	var latestIrreversibleTs *types.TipSet
	var reversibleTsList []*types.TipSet
	reversibleTsChangedAddressList := make(map[types.TipSetKey][]address.Address)
	var headChainTsList []*types.TipSet

	{
		var model_genesis_ts model.GenesisTipSet
		err := db.Take(&model_genesis_ts).Error
		if err != nil {
			log.Errorf("get genesis ts failed:%v", err)
			return nil, nil, nil, nil, nil, err
		}

		if err := genesisTs.UnmarshalJSON(model_genesis_ts.Raw); err != nil {
			log.Errorf("get genesis ts failed:%v", err)
			return nil, nil, nil, nil, nil, err
		}
	}

	{
		var modelIrreversibleList []model.TipSet
		result := db.Select([]string{"id", "height", "block_count"}).Where("height in (?)", db.Model(&model.Irreversible{}).Select("height")).Find(&modelIrreversibleList)
		if result.Error != nil {
			log.Errorf("restore failed ", result.Error)
			return nil, nil, nil, nil, nil, result.Error
		}

		if result.RowsAffected != 1 {
			log.Errorf("irreversible count error:%v", result.RowsAffected)
			return nil, nil, nil, nil, nil, xerrors.New("irreversible count error")
		}

		var err error
		latestIrreversibleTs, err = buildTipSet(db, modelIrreversibleList[0].ID, modelIrreversibleList[0].Height, modelIrreversibleList[0].BlockCount)
		if err != nil {
			log.Errorf("GetDatabaseChainState failed:%v", err)
			return nil, nil, nil, nil, nil, err
		}
	}

	{
		var tsList []model.TipSet

		if err := db.Where("id in (?)", db.Model(&model.ReversibleTipSet{}).Select("tipset_id").Where("status = ?", model.TipSetStatusValid)).Order("height asc").Find(&tsList).Error; err != nil {
			log.Errorf("GetDatabaseChainState failed:%v", err)
			return nil, nil, nil, nil, nil, err
		}

		for _, v := range tsList {
			ts, err := buildTipSet(db, v.ID, v.Height, v.BlockCount)
			if err != nil {
				log.Errorf("GetDatabaseChainState failed:%v", err)
				return nil, nil, nil, nil, nil, err
			}
			reversibleTsList = append(reversibleTsList, ts)

			if v.InHeadChain {
				headChainTsList = append(headChainTsList, ts)
			}
		}
	}

	strTskToTsk := make(map[string]types.TipSetKey)

	for _, v := range reversibleTsList {
		strTskToTsk[v.Key().String()] = v.Key()
	}

	{
		var modelReversibleTipSetList []model.ReversibleTipSet
		if err := db.Model(&model.ReversibleTipSet{}).Find(&modelReversibleTipSetList).Error; err != nil {
			log.Errorf("GetDatabaseChainState failed:%v", err)
			return nil, nil, nil, nil, nil, err
		}

		for _, v := range modelReversibleTipSetList {
			var changedAddressList []address.Address
			if err := json.Unmarshal([]byte(v.ChangedAddressList), &changedAddressList); err != nil {
				log.Errorf("get changedAddrList failed, unmarshal failed")
				return nil, nil, nil, nil, nil, err
			}

			tsk, exist := strTskToTsk[v.Tsk]
			if !exist {
				log.Errorf("get changedAddrList failed, get tsk failed")
				return nil, nil, nil, nil, nil, xerrors.New("get changedAddrList failed, get tsk failed")
			}

			// must use tipset.Key(), not tipset.Key().String()
			reversibleTsChangedAddressList[tsk] = changedAddressList
		}
	}

	return &genesisTs, latestIrreversibleTs, reversibleTsList, reversibleTsChangedAddressList, headChainTsList, nil
}

func buildTipSet(db *gorm.DB, tsID uint64, height int64, blockCount int) (*types.TipSet, error) {

	var tipsetBlockList []model.TipSetBlock
	if err := db.Model(&model.TipSetBlock{}).Where("height = ? AND tipset_id = ?", height, tsID).Find(&tipsetBlockList).Error; err != nil {
		log.Errorf("buildTipSet failed:%v", err)
		return nil, err
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
		return nil, err
	}

	tipsetBlocks := make([]*types.BlockHeader, blockCount)
	for _, v := range results {
		indexInTipSet, exist := blockHeaderSet[v.ID]
		if !exist {
			log.Errorf("buildTipSet failed: not found block in tipset")
			return nil, xerrors.New("not found block in tipset")
		}

		bh, err := types.DecodeBlock(v.Raw)
		if err != nil {
			log.Errorf("buildTipSet failed:%v", err)
			return nil, err
		}

		tipsetBlocks[indexInTipSet] = bh
	}

	for idx, v := range tipsetBlocks {
		if v == nil {
			log.Errorf("block is empty,idx=", idx)
			return nil, xerrors.New("block is empty")
		}
	}

	ts, err := types.NewTipSet(tipsetBlocks)
	if err != nil {
		log.Errorf("buildTipSet failed:%v", err)
		return nil, err
	}

	return ts, nil
}

func GetDatabaseChainConfig(db *gorm.DB) (int, error) {

	var model_chain_config model.ChainConfig
	if err := db.Take(&model_chain_config).Error; err != nil {
		log.Error(err)
		return 0, err
	}

	return model_chain_config.HeadChainLimit, nil
}

func WriteBlockMessages(db *gorm.DB, bh *types.BlockHeader, userBlsMessages []*types.Message, userSecpMessages []*types.SignedMessage) (map[string]uint64, error) {

	height := int64(bh.Height)

	start := time.Now()
	defer func() {
		log.Debugw("WriteBlockMessages", "duration", time.Since(start).String(), "height", height, "cid", bh.Cid())
	}()

	var msgCidList []string

	// no need to consider order
	var modelMessages []*model.UserMessage
	for _, msg := range userBlsMessages {
		modelMessage := fulltipset.BuildMsg(msg)
		modelMessages = append(modelMessages, modelMessage)
		msgCidList = append(msgCidList, msg.Cid().String())
	}

	for _, msg := range userSecpMessages {
		modelMessage := fulltipset.BuildSignedMsg(msg)
		modelMessages = append(modelMessages, modelMessage)
		msgCidList = append(msgCidList, msg.Cid().String())
	}

	if len(modelMessages) > 0 {
		if err := db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "cid"}},
			DoNothing: true}).CreateInBatches(&modelMessages, 100).Error; err != nil {
			log.Errorw("WriteBlockMessages", "err", err, "height", height)
			return nil, err
		}
	}

	// must query
	var modelMessages2 []*model.UserMessage
	if err := db.Select("id, cid").Where("cid in (?)", msgCidList).Find(&modelMessages2).Error; err != nil {
		log.Errorw("WriteBlockMessages", "err", err, "height", height)
		return nil, err
	}

	msgCidToMessageID := make(map[string]uint64)
	for _, m := range modelMessages2 {
		msgCidToMessageID[m.Cid] = m.ID
	}

	return msgCidToMessageID, nil
}

func WriteBlock(db *gorm.DB, bh *types.BlockHeader, cids []cid.Cid) (uint64, error) {

	height := int64(bh.Height)

	start := time.Now()
	defer func() {
		log.Debugw("WriteBlock", "duration", time.Since(start).String(), "height", height, "cid", bh.Cid())
	}()

	var winCount int64
	if bh.ElectionProof != nil {
		winCount = bh.ElectionProof.WinCount
	}

	data, _ := bh.Serialize()
	modelBlock := model.BlockHeader{
		Height:       height,
		Cid:          bh.Cid().String(),
		Miner:        bh.Miner.String(),
		Size:         len(data),
		WinCount:     winCount,
		MessageCount: len(cids),
		IsOrphan:     false,
		Raw:          data,
	}

	if err := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "cid"}},
		DoNothing: true}).CreateInBatches(&modelBlock, 5).Error; err != nil {
		log.Errorw("WriteBlock", "err", err, "height", height)
		return 0, err
	}

	// must query
	var modelBlock2 model.BlockHeader
	result := db.Select("id").Where("cid = ?", bh.Cid().String()).Take(&modelBlock2)
	if result.Error != nil {
		log.Errorw("WriteBlock", "err", result.Error, "height", height)
		return 0, result.Error
	}
	if result.RowsAffected != 1 {
		log.Errorw("WriteBlock", "err", xerrors.New("row count error"), "height", height)
		return 0, result.Error
	}

	return modelBlock2.ID, nil
}

func WriteBlockMessageRelations(ctx context.Context, db *gorm.DB, block *types.BlockHeader, blockMessages *api.BlockMessages, blockID uint64, userMsgCidToMessageID map[string]uint64) error {

	height := int64(block.Height)

	start := time.Now()
	defer func() {
		log.Debugw("WriteBlockMessageRelations", "duration", time.Since(start).String(), "height", height, "cid", block.Cid())
	}()

	var modelBlockMessageRelations []*model.BlockMessageRelation
	for i, cid := range blockMessages.Cids {
		msgID, exist := userMsgCidToMessageID[cid.String()]
		if !exist {
			panic(0)
		}

		modelBlockMessageRelation := model.BlockMessageRelation{
			Height:       height,
			BlockID:      blockID,
			MessageID:    msgID,
			IndexInBlock: i,
		}

		modelBlockMessageRelations = append(modelBlockMessageRelations, &modelBlockMessageRelation)
	}

	if len(modelBlockMessageRelations) > 0 {

		err := db.Transaction(func(tx *gorm.DB) error {

			var count int64
			if err := tx.Model(&model.BlockMessageRelation{}).Where("block_id = ?", blockID).Count(&count).Error; err != nil {
				return err
			}

			if count > 0 {
				return nil
			}

			// tidb use CreateInBatch failed in transaction
			if err := tx.Create(modelBlockMessageRelations).Error; err != nil {
				log.Errorf("WriteBlockMessageRelations failed %v", err)
				return err
			}

			return nil
		})

		return err
	}

	return nil
}

func Grow(ctx context.Context, db *gorm.DB, fts *common.FullTipSet, headChangeInfo *common.HeadChangeInfo, writePrevTsActorState bool) error {

	prevTs := fts.PrevTs
	curTs := fts.CurTs

	tsk := curTs.Key().String()
	height := int64(curTs.Height())

	start := time.Now()
	defer func() {
		log.Infow("grow", "duration", time.Since(start).String(), "height", height, "tsk", tsk)
	}()

	var parentTsID uint64
	var tsID uint64
	var reversibleTsID uint64
	parentTsk := curTs.Parents().String()
	blockCount := len(curTs.Blocks())
	parentWeight := decimal.NewFromBigInt(curTs.ParentWeight().Int, 0)

	totalMessageCount := 0
	for _, v := range fts.AllBlockMessages {
		totalMessageCount += len(v.Cids)
	}

	byteCurTsChangeAddressList, _ := json.Marshal(fts.CurTsChangeAddressList)

	if curTs.Height() != 0 {
		var emptyHeightList []model.EmptyHeight

		for h := prevTs.Height() + 1; h < curTs.Height(); h++ {
			m := model.EmptyHeight{
				Height:    int64(h),
				Timestamp: util.EpochToTimestamp(h),
			}
			emptyHeightList = append(emptyHeightList, m)
		}

		if len(emptyHeightList) > 0 {
			if err := db.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "height"}},
				DoNothing: true}).Create(&emptyHeightList).Error; err != nil {
				log.Errorw("grow", "err", err, "height", height, "tsk", tsk)
				return err
			}
		}
	}

	// 1. write tipset and branch
	if err := db.Transaction(func(tx *gorm.DB) error {

		// self must not exist
		var count int64
		if err := tx.Model(&model.ReversibleTipSet{}).Where("tsk = ?", tsk).Count(&count).Error; err != nil {
			log.Errorw("grow", "err", err, "height", height, "tsk", tsk)
			return err
		}

		if count > 0 {
			str := "reversible tipset is exist"
			log.Errorw("grow", "err", str, "height", height, "tsk", tsk)
			return xerrors.New(str)
		}

		// parent must exist and status is valid
		var modelPts model.ReversibleTipSet
		if err := tx.Model(&model.ReversibleTipSet{}).Where("tsk = ?", parentTsk).Take(&modelPts).Error; err != nil {
			// TODO log
			if errors.Is(err, gorm.ErrRecordNotFound) {
				//str := fmt.Sprintf("reversible tipset's parent is not exist. tsk=%v height=%v ptsk=%v", tsk, height, parentTsk)
			}
			log.Errorw("grow", "err", err, "height", height, "tsk", tsk)
			return err
		}

		if modelPts.Status != model.TipSetStatusValid {
			str := fmt.Sprintf("reversible tipset's parent status is %v. tsk=%v height=%v ptsk=%v", modelPts.Status, tsk, height, parentTsk)
			return xerrors.New(str)
		}

		parentTsID = modelPts.TipSetID
		parentHeight := modelPts.Height

		var decimalReward decimal.Decimal
		if fts.TotalReward.Int == nil {
			decimalReward = decimal.Zero
		} else {
			decimalReward = decimal.NewFromBigInt(fts.TotalReward.Int, 0)
		}

		var decimalPenalty decimal.Decimal
		if fts.Penalty.Int == nil {
			decimalPenalty = decimal.Zero
		} else {
			decimalPenalty = decimal.NewFromBigInt(fts.Penalty.Int, 0)
		}

		model_ts := model.TipSet{
			Height:                  height,
			ParentTipSetID:          parentTsID,
			ParentHeight:            parentHeight,
			Timestamp:               util.EpochToTimestamp(curTs.Height()),
			BlockCount:              blockCount,
			TotalMessageCount:       totalMessageCount,
			DedupedMessageCount:     len(fts.UserBlsMessages) + len(fts.UserSecpMessages),
			UnProcessedMessageCount: len(fts.UnProcessedMsgCidSet),

			ParentStateRoot: curTs.ParentState().String(),
			ParentWeight:    parentWeight,
			ParentBaseFee:   decimal.NewFromBigInt(curTs.Blocks()[0].ParentBaseFee.Int, 0),

			Reward:        decimalReward,
			Penalty:       decimalPenalty,
			IsValid:       false,
			InHeadChain:   false,
			HasActorState: false,
		}
		if err := tx.Create(&model_ts).Error; err != nil {
			log.Errorw("grow", "err", err, "height", height, "tsk", tsk)
			return err
		}

		tsID = model_ts.ID

		modelReversibleTipSet := model.ReversibleTipSet{
			TipSetID:           tsID,
			Height:             height,
			ParentHeight:       parentHeight,
			Status:             model.TipSetStatusWriting,
			Tsk:                tsk,
			ParentTsk:          parentTsk,
			ParentTipSetID:     parentTsID,
			ParentWeight:       parentWeight,
			ChangedAddressList: string(byteCurTsChangeAddressList),
		}
		if err := tx.Create(&modelReversibleTipSet).Error; err != nil {
			log.Errorw("grow", "err", err, "height", height, "tsk", tsk)
			return err
		}

		reversibleTsID = modelReversibleTipSet.ID

		return nil
	}); err != nil {
		log.Errorw("grow", "err", err, "height", height, "tsk", tsk)
		return err
	}

	// 2. write prevTs's actor state and curTs's messages and blocks
	grp1, _ := errgroup.WithContext(ctx)

	// write prevTs's actor state
	if writePrevTsActorState && height > 0 {
		grp1.Go(func() error {
			start := time.Now()
			defer func() {
				log.Debugw("writeChainPower ", "duration", time.Since(start).String())
			}()

			return fulltipset.WriteChainPower(db, parentTsID, prevTs, fts.ChainPower)
		})
		grp1.Go(func() error {
			start := time.Now()
			defer func() {
				log.Debugw("writeChainReward ", "duration", time.Since(start).String())
			}()

			return fulltipset.WriteChainReward(db, parentTsID, prevTs, fts.ChainReward)
		})
		grp1.Go(func() error {
			start := time.Now()
			defer func() {
				log.Debugw("writeMarketDealProposals ", "duration", time.Since(start).String())
			}()

			return fulltipset.WriteMarketDealProposals(db, parentTsID, prevTs, fts.MarketDealProposals)
		})
		grp1.Go(func() error {
			start := time.Now()
			defer func() {
				log.Debugw("writeMarketDealStates ", "duration", time.Since(start).String())
			}()

			return fulltipset.WriteMarketDealStates(db, parentTsID, prevTs, fts.MarketDealStates)
		})
		grp1.Go(func() error {
			start := time.Now()
			defer func() {
				log.Debugw("writeInitActor ", "duration", time.Since(start).String())
			}()

			return fulltipset.WriteInitActor(db, parentTsID, prevTs, fts.ChainInitActor)
		})
		grp1.Go(func() error {
			start := time.Now()
			defer func() {
				log.Debugw("writeMultiSig ", "duration", time.Since(start).String())
			}()

			return fulltipset.WriteMultiSig(db, parentTsID, prevTs, fts.MultiSigTxs)
		})
		grp1.Go(func() error {
			start := time.Now()
			defer func() {
				log.Debugw("writePaymentChannels ", "duration", time.Since(start).String())
			}()

			return fulltipset.WritePaymentChannels(db, parentTsID, prevTs, fts.PaymentChannels)
		})
		grp1.Go(func() error {
			start := time.Now()
			defer func() {
				log.Debugw("writeMinerPowers ", "duration", time.Since(start).String())
			}()

			return fulltipset.WriteMinerPowers(db, parentTsID, prevTs, fts)
		})
		grp1.Go(func() error {
			start := time.Now()
			defer func() {
				log.Debugw("writeMiners ", "duration", time.Since(start).String())
			}()

			return fulltipset.WriteMiners(db, parentTsID, prevTs, fts)
		})
		grp1.Go(func() error {
			start := time.Now()
			defer func() {
				log.Debugw("writeCommonActors ", "duration", time.Since(start).String())
			}()

			return fulltipset.WriteCommonActors(db, parentTsID, prevTs, fts.ChangedActors)
		})
		grp1.Go(func() error {
			start := time.Now()
			defer func() {
				log.Debugw("writeSectorInfos ", "duration", time.Since(start).String())
			}()

			return fulltipset.WriteSectorInfos(db, parentTsID, prevTs, fts.SectorInfos)
		})
		grp1.Go(func() error {
			start := time.Now()
			defer func() {
				log.Debugw("writeSectorPrecommitInfos ", "duration", time.Since(start).String())
			}()

			return fulltipset.WriteSectorPrecommitInfos(db, parentTsID, prevTs, fts.SectorPrecommitInfos)
		})
		grp1.Go(func() error {
			start := time.Now()
			defer func() {
				log.Debugw("writeMinerSectorEvents ", "duration", time.Since(start).String())
			}()

			return fulltipset.WriteMinerSectorEvents(db, parentTsID, prevTs, fts.MinerSectorEvents)
		})
		grp1.Go(func() error {
			start := time.Now()
			defer func() {
				log.Debugw("writeSectorDealEvents ", "duration", time.Since(start).String())
			}()

			return fulltipset.WriteSectorDealEvents(db, parentTsID, prevTs, fts.SectorDealEvents)
		})
	}

	// write curTs's messages and blocks
	blockHeaderIDs := make(map[string]uint64)

	mtxWriteBlock := sync.Mutex{}
	for i, blk := range curTs.Blocks() {

		idx := i
		bh := blk

		grp1.Go(func() error {

			start := time.Now()
			defer func() {
				log.Debugw("grow writeBlock", "duration", time.Since(start).String(), "idx", idx, "cid", bh.Cid())
			}()

			blockID, err := WriteBlock(db, bh, fts.AllBlockMessages[idx].Cids)
			if err != nil {
				return err
			}

			mtxWriteBlock.Lock()
			blockHeaderIDs[bh.Cid().String()] = blockID
			mtxWriteBlock.Unlock()

			return nil
		})
	}

	userMsgCidToMessageID := make(map[string]uint64)
	grp1.Go(func() error {
		start := time.Now()
		defer func() {
			log.Debugw("writeUserMessages", "duration", time.Since(start).String())
		}()

		result, err := fulltipset.WriteUserMessages(db, tsID, curTs, fts.UserBlsMessages, fts.UserSecpMessages)
		if err != nil {
			return err
		}

		userMsgCidToMessageID = result

		return nil
	})

	systemMsgCidToMessageID := make(map[string]uint64)
	grp1.Go(func() error {
		start := time.Now()
		defer func() {
			log.Debugw("writeSystemMessages", "duration", time.Since(start).String())
		}()

		result, err := fulltipset.WriteSystemMessages(db, tsID, curTs, fts.SystemMessages)
		if err != nil {
			return err
		}

		systemMsgCidToMessageID = result

		return nil
	})

	if err := grp1.Wait(); err != nil {
		log.Errorw("database grow failed", err)
		return err
	}

	grp2, _ := errgroup.WithContext(ctx)

	grp2.Go(func() error {
		start := time.Now()
		defer func() {
			log.Debugw("writeTipSetBlocks", "duration", time.Since(start).String())
		}()

		if err := fulltipset.WriteTipSetBlocks(db, tsID, curTs, fts.AllBlockMessages, fts.UnProcessedMsgCidSet, fts.BlockRewards, fts.Penalty, fts.AllBlockMessageSet, blockHeaderIDs); err != nil {
			log.Errorf("WriteTipSetBlocks failed %v", err)
			return err
		}
		return nil
	})

	grp2.Go(func() error {
		start := time.Now()
		defer func() {
			log.Debugw("writeMessageInvokeResults", "duration", time.Since(start).String())
		}()

		if err := fulltipset.WriteMessageInvokeResults(db, tsID, curTs, fts.UserBlsMessages, fts.UserSecpMessages, fts.SystemMessages, userMsgCidToMessageID, systemMsgCidToMessageID); err != nil {
			log.Errorf("WriteMessageInvokeResults failed %v", err)
			return err
		}

		return nil
	})

	// curTs's block message relation
	for i, blk := range curTs.Blocks() {

		idx := i
		bh := blk

		grp2.Go(func() error {
			start := time.Now()
			defer func() {
				log.Debugw("writeBlockMessageRelations ", "duration", time.Since(start).String(), "cid", bh.Cid())
			}()

			blockID, exist := blockHeaderIDs[bh.Cid().String()]
			if !exist {
				log.Errorf("database grow failed, blk not found %v", bh.Cid())
				return xerrors.New("database grow failed, blk not found")
			}

			if err := WriteBlockMessageRelations(ctx, db, bh, fts.AllBlockMessages[idx], blockID, userMsgCidToMessageID); err != nil {
				log.Errorf("WriteBlockMessageRelations failed %v", err)
				return err
			}

			return nil
		})
	}

	// execution trace messages
	grp2.Go(func() error {
		start := time.Now()
		defer func() {
			log.Debugw("writeExecutionTraces ", "duration", time.Since(start).String())
		}()

		if err := fulltipset.WriteExecutionTraceMessages(db, tsID, curTs, fts.ExecutionTraceMesages, userMsgCidToMessageID, systemMsgCidToMessageID); err != nil {
			return err
		}

		return nil
	})

	if err := grp2.Wait(); err != nil {
		log.Errorw("database grow2 failed", err)
		return err
	}

	// 3. update status, update headchain
	if err := db.Transaction(func(tx *gorm.DB) error {

		if err := tx.Model(&model.ReversibleTipSet{}).Where("id = ?", reversibleTsID).Update("status", model.TipSetStatusValid).Error; err != nil {
			log.Errorw("grow", "err", err, "height", height, "tsk", tsk)
			return err
		}

		if writePrevTsActorState && height > 0 {
			if err := tx.Model(&model.TipSet{}).Where("id = ?", parentTsID).Update("has_actor_state", true).Error; err != nil {
				log.Errorw("grow", "err", err, "height", height, "tsk", tsk)
				return err
			}
		}

		if err := tx.Model(&model.TipSet{}).Where("id = ?", tsID).Update("is_valid", true).Error; err != nil {
			log.Errorw("grow", "err", err, "height", height, "tsk", tsk)
			return err
		}

		// update old headchain from forkpoint
		{
			count := len(headChangeInfo.OldHeadChain)
			if count > 0 {
				tskList := make([]string, count)
				for i, ts := range headChangeInfo.OldHeadChain {
					tskList[i] = ts.Key().String()
				}

				if err := updateHeadChain(tx, tskList, false); err != nil {
					log.Errorw("grow", "err", err, "height", height, "tsk", tsk)
					return err
				}
			}
		}

		// update new headchain from forkpoint
		{
			count := len(headChangeInfo.NewHeadChain)
			if count > 0 {
				tskList := make([]string, count)
				for i, ts := range headChangeInfo.NewHeadChain {
					tskList[i] = ts.Key().String()
				}

				if err := updateHeadChain(tx, tskList, true); err != nil {
					log.Errorw("grow", "err", err, "height", height, "tsk", tsk)
					return err
				}
			}
		}

		return nil
	}); err != nil {
		log.Errorw("grow", "err", err, "height", height, "tsk", tsk)
		return err
	}

	return nil
}

func updateHeadChain(tx *gorm.DB, tskList []string, inHeadChain bool) error {
	if err := tx.Model(&model.TipSet{}).Where("id in (?)", tx.Model(&model.ReversibleTipSet{}).Select("tipset_id").Where("tsk in (?)", tskList)).Update("in_head_chain", inHeadChain).Error; err != nil {
		return err
	}

	return nil
}

func Prune(ctx context.Context, db *gorm.DB, ts *types.TipSet, sameHeightHeadChainTs *types.TipSet, deletePrevTsActorState bool) error {

	tsk := ts.Key().String()
	height := int64(ts.Height())

	start := time.Now()
	defer func() {
		log.Infow("prune", "duration", time.Since(start).String(), "height", height, "tsk", tsk)
	}()

	var tsID uint64
	var parentHeight int64
	var parentTsID uint64

	// 1. check and set deleting status
	if err := db.Transaction(func(tx *gorm.DB) error {

		// 1. must in reversible_tipset
		var modelReversibleTipSet model.ReversibleTipSet
		result := tx.Where("tsk = ?", tsk).Take(&modelReversibleTipSet)
		if result.Error != nil {
			log.Errorw("prune", "err", result.Error, "height", height, "tsk", tsk)
			return result.Error
		}

		if result.RowsAffected != 1 {
			str := fmt.Sprintf("prune can not find tipset. tsk=%v height=%v", tsk, height)
			return xerrors.New(str)
		}

		tsID = modelReversibleTipSet.TipSetID
		parentHeight = modelReversibleTipSet.ParentHeight
		parentTsID = modelReversibleTipSet.ParentTipSetID

		// 2. set prevTs's flag
		if deletePrevTsActorState {
			if err := tx.Model(&model.TipSet{}).Where("id = ?", parentTsID).Update("has_actor_state", false).Error; err != nil {
				return err
			}
		}

		// 3. delete tipset
		if err := tx.Where("id = ?", tsID).Delete(&model.TipSet{}).Error; err != nil {
			return err
		}

		// 4. set status to deleting
		if err := tx.Model(&model.ReversibleTipSet{}).Where("id = ?", modelReversibleTipSet.ID).Update("status", model.TipSetStatusDeleting).Error; err != nil {
			log.Errorw("prune", "err", err, "height", height, "tsk", tsk)
			return err
		}

		return nil
	}); err != nil {
		log.Errorw("prune", "err", err, "height", height, "tsk", tsk)
		return err
	}

	// 2. remove
	if err := removeReversibleFullTipSet(ctx, db, tsID, tsk, height, parentTsID, parentHeight, deletePrevTsActorState); err != nil {
		log.Errorw("prune", "err", err, "height", height, "tsk", tsk)
		return err
	}

	return nil
}

func Pin(ctx context.Context, db *gorm.DB, tsk types.TipSetKey, height int64) error {

	start := time.Now()
	defer func() {
		log.Infow("pin", "duration", time.Since(start).String(), "height", height, "tsk", tsk)
	}()

	cidList := make([]string, 0)
	for _, v := range tsk.Cids() {
		cidList = append(cidList, v.String())
	}

	// get orphan list
	var blockList []model.BlockHeader
	if err := db.Where("height=? AND cid NOT IN (?)", height, cidList).Find(&blockList).Error; err != nil {
		log.Errorw("pin", "err", err, "height", height, "tsk", tsk)
		return err
	}

	orphanBlockList := make([]*model.OrphanBlock, len(blockList))
	for i, blk := range blockList {
		orphanBlock := model.OrphanBlock{
			Height: blk.Height,
			Cid:    blk.Cid,
			Miner:  blk.Miner,
		}

		orphanBlockList[i] = &orphanBlock
	}

	return db.Transaction(func(tx *gorm.DB) error {

		// 1. should be the first one in branch
		var modelReversibleTipSetList []model.ReversibleTipSet
		if err := tx.Order("height asc").Limit(2).Find(&modelReversibleTipSetList).Error; err != nil {
			log.Errorw("pin", "err", err, "height", height, "tsk", tsk)
			return err
		}

		count := len(modelReversibleTipSetList)

		if count == 1 {
			str := fmt.Sprintf("pin reversible ts only one. height=%v", height)
			return xerrors.New(str)
		}

		if modelReversibleTipSetList[0].Tsk != tsk.String() {
			return xerrors.New("pin tipset not found")
		}

		tsID := modelReversibleTipSetList[0].TipSetID
		parentTsID := modelReversibleTipSetList[0].ParentTipSetID

		if err := tx.Model(&model.BlockHeader{}).Where("height=? AND cid NOT IN (?)", height, cidList).Update("is_orphan", true).Error; err != nil {
			log.Errorw("pin", "err", err, "height", height, "tsk", tsk)
			return err
		}

		if len(orphanBlockList) > 0 {
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "cid"}},
				DoNothing: true}).Create(&orphanBlockList).Error; err != nil {
				log.Errorw("pin", "err", err, "height", height, "tsk", tsk)
				return err
			}
		}

		var result *gorm.DB

		// 2. remove parent from irreversible
		result = tx.Where("tipset_id = ?", parentTsID).Delete(&model.Irreversible{})
		if result.Error != nil {
			log.Errorw("pin", "err", result.Error, "height", height, "tsk", tsk)
			return result.Error
		}

		if result.RowsAffected != 1 {
			return xerrors.New("pin delete parent failed")
		}

		// 3. remove it from reversible
		result = tx.Where("tipset_id = ?", tsID).Delete(&model.ReversibleTipSet{})
		if result.Error != nil {
			log.Errorw("pin", "err", result.Error, "height", height, "tsk", tsk)
			return result.Error
		}

		if result.RowsAffected != 1 {
			return xerrors.New("pin delete branch failed")
		}

		// 4. add it to irreversible
		modelIrreversible := model.Irreversible{
			TipSetID: tsID,
			Height:   height,
		}
		if err := tx.Create(&modelIrreversible).Error; err != nil {
			log.Errorw("pin", "err", err, "height", height, "tsk", tsk)
			return err
		}

		return nil
	})
}

func removeReversibleFullTipSet(ctx context.Context, db *gorm.DB, tsID uint64, tsk string, height int64, parentTsID uint64, parentHeight int64, deletePrevTsActorState bool) error {

	start := time.Now()
	defer func() {
		log.Debugw("removeReversibleFullTipSet", "duration", time.Since(start).String(), "height", height, "tsk", tsk)
	}()

	// if use foreign key, must consider the forign key dependences and the order of remove tables.

	grp, _ := errgroup.WithContext(ctx)

	if deletePrevTsActorState {
		grp.Go(func() error {
			err := db.Where("height = ? AND tipset_id = ?", parentHeight, parentTsID).Delete(&model.ChainPower{}).Error
			if err != nil {
				log.Errorw("err", err)
			}
			return err
		})
		grp.Go(func() error {
			err := db.Where("height = ? AND tipset_id = ?", parentHeight, parentTsID).Delete(&model.ChainReward{}).Error
			if err != nil {
				log.Errorw("err", err)
			}
			return err
		})
		grp.Go(func() error {
			err := db.Where("height = ? AND tipset_id = ?", parentHeight, parentTsID).Delete(&model.InitActor{}).Error
			if err != nil {
				log.Errorw("err", err)
			}
			return err
		})
		grp.Go(func() error {
			err := db.Where("height = ? AND tipset_id = ?", parentHeight, parentTsID).Delete(&model.MultiSig{}).Error
			if err != nil {
				log.Errorw("err", err)
			}
			return err
		})
		grp.Go(func() error {
			err := db.Where("height = ? AND tipset_id = ?", parentHeight, parentTsID).Delete(&model.CommonActor{}).Error
			if err != nil {
				log.Errorw("err", err)
			}
			return err
		})
		grp.Go(func() error {
			err := db.Where("height = ? AND tipset_id = ?", parentHeight, parentTsID).Delete(&model.PaymentChannel{}).Error
			if err != nil {
				log.Errorw("err", err)
			}
			return err
		})
		grp.Go(func() error {
			err := db.Where("height = ? AND tipset_id = ?", parentHeight, parentTsID).Delete(&model.Miner{}).Error
			if err != nil {
				log.Errorw("err", err)
			}
			return err
		})
		grp.Go(func() error {
			err := db.Where("height = ? AND tipset_id = ?", parentHeight, parentTsID).Delete(&model.MinerPower{}).Error
			if err != nil {
				log.Errorw("err", err)
			}
			return err
		})
		grp.Go(func() error {
			err := db.Where("height = ? AND tipset_id = ?", parentHeight, parentTsID).Delete(&model.MarketDealProposal{}).Error
			if err != nil {
				log.Errorw("err", err)
			}
			return err
		})
		grp.Go(func() error {
			err := db.Where("height = ? AND tipset_id = ?", parentHeight, parentTsID).Delete(&model.MarketDealState{}).Error
			if err != nil {
				log.Errorw("err", err)
			}
			return err
		})
	}

	// not remove block,usermessage,block_message_relationship

	grp.Go(func() error {
		err := db.Where("height = ? AND tipset_id = ?", height, tsID).Delete(&model.SystemMessage{}).Error
		if err != nil {
			log.Errorw("err", err)
		}
		return err
	})

	grp.Go(func() error {
		err := db.Where("height = ? AND tipset_id = ?", height, tsID).Delete(&model.MessageInvokeResult{}).Error
		if err != nil {
			log.Errorw("err", err)
		}
		return err
	})

	// if use forign key constraint, below must be serial,
	grp.Go(func() error {
		if err := db.Where("height = ? AND tipset_id = ?", height, tsID).Delete(&model.ExecutionTraceMessage{}).Error; err != nil {
			log.Errorw("err", err)
			return err
		}

		return nil
	})

	grp.Go(func() error {

		// delete tipset_block
		if err := db.Where("height = ? AND tipset_id = ?", height, tsID).Delete(&model.TipSetBlock{}).Error; err != nil {
			log.Errorw("err", err)
			return err
		}

		return nil
	})

	if err := grp.Wait(); err != nil {
		log.Errorw("removeReversibleFullTipSet err=", err)
		return err
	}

	if err := db.Where("tipset_id = ?", tsID).Delete(&model.ReversibleTipSet{}).Error; err != nil {
		log.Errorw("err", err)
		return err
	}

	return nil
}
