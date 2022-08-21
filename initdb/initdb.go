package initdb

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/go-redis/redis/v8"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/rqzrqh/sync_filecoin/common"
	"github.com/rqzrqh/sync_filecoin/dao"
	"github.com/rqzrqh/sync_filecoin/local_chain"
	"github.com/rqzrqh/sync_filecoin/migrate"
	"github.com/rqzrqh/sync_filecoin/model"
	"github.com/shopspring/decimal"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

var log = logging.Logger("initdb")

func InitDatabase(ctx context.Context, db *gorm.DB, rds *redis.Client, node api.FullNode, initHeight int64, headChainLimit int) error {

	if checkExist(db) {
		return xerrors.New("database has been initialized")
	}

	if err := createTables(db); err != nil {
		return err
	}

	if err := fillTables(ctx, db, node, initHeight, headChainLimit); err != nil {
		return err
	}

	if err := checkDB(ctx, db); err != nil {
		return err
	}

	if err := initCache(ctx, db, rds); err != nil {
		return err
	}

	return nil
}

func checkExist(db *gorm.DB) bool {
	return db.Migrator().HasTable(&model.ChainConfig{})
}

func createTables(db *gorm.DB) error {

	startTime := time.Now()
	defer func() {
		log.Infow("createTables", "duration", time.Since(startTime).String())
	}()

	err := db.Debug().AutoMigrate(
		// 1.chain struct
		&model.TipSet{},
		&model.Irreversible{},
		&model.ReversibleTipSet{},
		&model.BlockHeader{},

		// 2. input
		&model.UserMessage{},
		&model.SystemMessage{},
		&model.BlockMessageRelation{},
		&model.TipSetBlock{},

		// 3. output
		&model.MessageInvokeResult{},
		&model.ExecutionTraceMessage{},

		// 4. increment state
		&model.CommonActor{},
		&model.ChainPower{},
		&model.ChainReward{},
		&model.Miner{},
		&model.MinerPower{},
		&model.MarketDealProposal{},
		&model.MarketDealState{},
		&model.PaymentChannel{},
		&model.InitActor{},
		&model.SectorInfo{},
		&model.SectorPrecommitInfo{},
		&model.MinerSectorEvent{},
		&model.SectorDealEvent{},
		&model.MultiSig{},

		// 5. other
		&model.GenesisTipSet{},
		&model.ChainConfig{},
		&model.OrphanBlock{},
		&model.EmptyHeight{},
		// PidFile Table is created at the time of program starts
	)

	if err != nil {
		return err
	}

	return err
}

func fillTables(ctx context.Context, db *gorm.DB, node api.FullNode, initHeight int64, headChainLimit int) error {

	fmt.Println("init height:", initHeight)
	fmt.Println("headchain limit size:", headChainLimit)

	if initHeight < 0 {
		return xerrors.New("height should >= 0")
	}

	if headChainLimit < 1 {
		return xerrors.New("reversibleRangeSize should >= 1")
	}

	// 1. write necessary data first
	genesisTs, err := node.ChainGetGenesis(ctx)
	if err != nil {
		return err
	}
	rawdata, _ := genesisTs.MarshalJSON()
	model_genesis_ts := model.GenesisTipSet{
		Raw: rawdata,
	}
	if err := db.Create(&model_genesis_ts).Error; err != nil {
		return err
	}

	// 2. write fts
	{
		ts1, err := node.ChainGetTipSetAfterHeight(ctx, abi.ChainEpoch(initHeight), types.EmptyTSK)
		if err != nil {
			return err
		}

		ts2, err := node.ChainGetTipSetAfterHeight(ctx, abi.ChainEpoch(ts1.Height()+1), types.EmptyTSK)
		if err != nil {
			return err
		}

		if ts1.Key() != ts2.Parents() {
			panic("initTs key not continuous")
		}

		if err := writeTipSets(ctx, db, node, ts1, ts2); err != nil {
			return err
		}
	}

	// write config
	model_chain_config := model.ChainConfig{
		HeadChainLimit: headChainLimit,
	}
	if err := db.Create(&model_chain_config).Error; err != nil {
		return err
	}

	return nil
}

func writeTipSets(ctx context.Context, db *gorm.DB, node api.FullNode, ts1 *types.TipSet, ts2 *types.TipSet) error {

	dbChain := local_chain.NewDatabaseChain(ctx, db)

	irreversibleFakeHeight := int64(0)
	reversibleFakeHeight := int64(0)

	var ts0 *types.TipSet
	var err error
	if ts1.Height() != 0 {
		ts0, err = node.ChainGetTipSet(ctx, ts1.Parents())
		if err != nil {
			return err
		}
		reversibleFakeHeight = int64(ts0.Height())
	} else {
		reversibleFakeHeight = int64(ts1.Height()) - 1
	}

	irreversibleFakeHeight = reversibleFakeHeight - 1

	var irreversibleFakeTsID uint64

	// 1. write fake prev tipset, as irreversible
	{
		modelIrreversibleFakeTs := model.TipSet{
			Height:        irreversibleFakeHeight,
			ParentWeight:  decimal.Zero,
			ParentBaseFee: decimal.Zero,
			Reward:        decimal.Zero,
			Penalty:       decimal.Zero,
			IsValid:       true,
			InHeadChain:   true,
		}
		if err := db.Create(&modelIrreversibleFakeTs).Error; err != nil {
			return err
		}

		modelIrreversible := model.Irreversible{
			TipSetID: modelIrreversibleFakeTs.ID,
			Height:   irreversibleFakeHeight,
		}

		if err := db.Create(&modelIrreversible).Error; err != nil {
			return err
		}

		irreversibleFakeTsID = modelIrreversibleFakeTs.ID
	}

	// 2. write fake tipset, as reversible
	reversibleFakeTsk := ts1.Parents()

	var reversibleFakeTsID uint64

	if err := db.Transaction(func(tx *gorm.DB) error {

		modelFakeTipSet := model.TipSet{
			Height:         reversibleFakeHeight,
			ParentTipSetID: irreversibleFakeTsID,
			ParentWeight:   decimal.Zero,
			ParentBaseFee:  decimal.Zero,
			Reward:         decimal.Zero,
			Penalty:        decimal.Zero,
			IsValid:        true,
			InHeadChain:    true,
		}
		if err := tx.Create(&modelFakeTipSet).Error; err != nil {
			return err
		}

		reversibleFakeTsID = modelFakeTipSet.ID

		modelFakeReversibleTipSet := model.ReversibleTipSet{
			TipSetID:       reversibleFakeTsID,
			Height:         reversibleFakeHeight,
			ParentHeight:   irreversibleFakeHeight,
			Tsk:            reversibleFakeTsk.String(),
			ParentTipSetID: irreversibleFakeTsID,
			Status:         model.TipSetStatusValid,
		}
		if err := tx.Create(&modelFakeReversibleTipSet).Error; err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	log.Infof("####write height %v <= %v <= %v <= %v", irreversibleFakeHeight, reversibleFakeHeight, ts1.Height(), ts2.Height())

	// 3. append real full tipset
	{
		var ts0ChangeAddressList []address.Address

		ts1ChangeAddressList, err := getAndWriteFullTipSet(ctx, dbChain, node, ts0ChangeAddressList, ts0, ts1)
		if err != nil {
			return err
		}

		if _, err := getAndWriteFullTipSet(ctx, dbChain, node, ts1ChangeAddressList, ts1, ts2); err != nil {
			return err
		}
	}

	// 4. pop fake tipset
	if err := dao.Pin(ctx, db, reversibleFakeTsk, reversibleFakeHeight); err != nil {
		return err
	}

	if err := dao.Pin(ctx, db, ts1.Key(), int64(ts1.Height())); err != nil {
		return err
	}

	// delete raw irreversible fake ts
	if err := db.Where("id = ?", irreversibleFakeTsID).Delete(&model.TipSet{}).Error; err != nil {
		return err
	}

	// delete raw reversible fake ts, now it's an irreversible tipset.
	if err := db.Where("id = ?", reversibleFakeTsID).Delete(&model.TipSet{}).Error; err != nil {
		return err
	}

	return nil
}

func getAndWriteFullTipSet(ctx context.Context, dbChain *local_chain.DatabaseChain, node api.FullNode, prevTsChangeAddressList []address.Address, prevTs *types.TipSet, curTs *types.TipSet) ([]address.Address, error) {

	// 1. get fts
	curHeightKnownBlockMessages := make(map[cid.Cid]*api.BlockMessages)
	fts, err := migrate.GetFullTipSet(ctx, node, prevTsChangeAddressList, true, prevTs, curTs, curHeightKnownBlockMessages)
	if err != nil {
		return nil, err
	}

	// 2. grow, alway is head
	var newHeadChain []*types.TipSet
	newHeadChain = append(newHeadChain, curTs)
	return fts.CurTsChangeAddressList, dbChain.Grow(fts, &common.HeadChangeInfo{NewHeadChain: newHeadChain}, true)
}

func checkDB(ctx context.Context, db *gorm.DB) error {
	log.Infof("########## load from database #########")

	headChainLimit, err := dao.GetDatabaseChainConfig(db)
	if err != nil {
		return err
	}
	genesisTs, latestIrreversibleTs, reversibleTsList, reversibleTsChangedAddressList, headChainList, err := dao.GetDatabaseChainState(db)
	if err != nil {
		return err
	}

	log.Infof("head chain limit=%v", headChainLimit)

	local_chain.NewMemoryChain(genesisTs, latestIrreversibleTs, reversibleTsList, reversibleTsChangedAddressList, headChainList)

	return nil
}

func initCache(ctx context.Context, db *gorm.DB, rds *redis.Client) error {

	var modelTipSetList []model.TipSet
	if err := db.Model(&model.TipSet{}).Order("parent_weight asc").Find(&modelTipSetList).Error; err != nil {
		return err
	}

	for _, modelTipSet := range modelTipSetList {
		var headChainList []*dao.HeadChainInfo

		info := dao.HeadChainInfo{
			Height:       modelTipSet.Height,
			ParentHeight: modelTipSet.ParentHeight,
			ID:           modelTipSet.ID,
			ParentID:     modelTipSet.ParentTipSetID,
		}

		headChainList = append(headChainList, &info)

		pipe := rds.TxPipeline()
		defer pipe.Close()

		var chainNofity string

		{
			set, addTo, _ := dao.GetBatchHeightChangedData(ctx, db, headChainList)
			for k, v := range set {
				pipe.Set(ctx, k, v, dao.CacheTimeout)
			}
			for _, info := range addTo {
				pipe.RPush(ctx, info.Key, info.Value)
			}

			chainNofity = set[dao.BuildTipSetDigestKey(info.Height)]
		}

		byteChainHead, _ := json.Marshal(info)
		pipe.Set(ctx, dao.BuildChainHeadKey(), string(byteChainHead), 0)
		pipe.Publish(ctx, dao.BuildChainNotifyKey(), chainNofity)

		_, err := pipe.Exec(ctx)
		if err != nil {
			pipe.Discard()
			panic("discard")
		}
	}

	return nil
}
