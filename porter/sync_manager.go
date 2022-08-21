package porter

import (
	"context"

	"github.com/filecoin-project/lotus/api"
	"github.com/go-redis/redis/v8"
	"github.com/rqzrqh/sync_filecoin/dao"
	"github.com/rqzrqh/sync_filecoin/local_chain"
	"gorm.io/gorm"
)

type SyncManager struct {
	db                *gorm.DB
	localChainManager *local_chain.LocalChainManager
	dataSource        *DataSource

	bmm  *BlockMessageManager
	ptsm *PendingTipSetManager

	differ             *Differ
	syncer             *Syncer
	bmSyncer           *BlockMessageSyncer
	dbChainStateNotify chan local_chain.DatabaseChainState
}

func NewSyncManager(ctx context.Context, db *gorm.DB, rds *redis.Client, nodes []api.FullNode) *SyncManager {
	nodeStateNotify := make([]chan UpdateNodeCmd, len(nodes))
	for i := 0; i < len(nodes); i++ {
		nodeStateNotify[i] = make(chan UpdateNodeCmd)
	}

	incomingBlockNotify := make(chan IncomingBlock)

	dbChainStateNotify := make(chan local_chain.DatabaseChainState)

	if err := dao.GetDatabaseLock(db); err != nil {
		panic("GetDatabaseLock failed")
	}

	if err := dao.CleanupDatabaseChain(ctx, db); err != nil {
		dao.ReleaseDatabaseLock(db)
		panic("Cleanup failed")
	}

	headChainLimit, err := dao.GetDatabaseChainConfig(db)
	if err != nil {
		dao.ReleaseDatabaseLock(db)
		panic("GetChainConfig failed")
	}
	log.Infof("head chain limit=%v", headChainLimit)

	genesisTs, latestIrreversibleTs, reversibleTsList, reversibleTsChangedAddressList, headChainList, err := dao.GetDatabaseChainState(db)
	if err != nil {
		dao.ReleaseDatabaseLock(db)
		panic("GetChainState failed")
	}

	memChain := local_chain.NewMemoryChain(genesisTs, latestIrreversibleTs, reversibleTsList, reversibleTsChangedAddressList, headChainList)

	dataSource := newDataSource(ctx, genesisTs, latestIrreversibleTs, memChain.GetChainHead(), headChainLimit, nodes, nodeStateNotify, incomingBlockNotify)

	localChainManager := local_chain.NewDbChainManager(ctx, db, rds, memChain, headChainLimit, dbChainStateNotify)

	blockMessageManager := newBlockMessageManager(ctx, incomingBlockNotify)
	pendingTipSetManager := newPendingTipSetManager()

	syncManager := &SyncManager{
		db:                 db,
		localChainManager:  localChainManager,
		dataSource:         dataSource,
		ptsm:               pendingTipSetManager,
		bmm:                blockMessageManager,
		differ:             newDiffer(dataSource, memChain, pendingTipSetManager),
		syncer:             newSyncer(ctx, dataSource.GetNodes(), nodeStateNotify, blockMessageManager, pendingTipSetManager, localChainManager),
		bmSyncer:           newBlockMessageSyncer(ctx, db, blockMessageManager, localChainManager),
		dbChainStateNotify: dbChainStateNotify,
	}

	return syncManager
}

func (sm *SyncManager) Start() {
	sm.localChainManager.Start()

	sm.bmm.start()
	sm.ptsm.Start()
	sm.differ.Start()
	sm.syncer.Start()
	sm.bmSyncer.start()
	sm.dataSource.Start()

	go func() {
		for {
			select {
			case state := <-sm.dbChainStateNotify:
				sm.dataSource.notifyChainState(state.Irreversible, state.ChainHead)
				sm.bmm.setIrreversibleHeight(state.Irreversible.Height())
			}
		}
	}()
}

func (sm *SyncManager) Stop() {
	sm.localChainManager.Stop()

	dao.ReleaseDatabaseLock(sm.db)
}
