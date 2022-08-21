package local_chain

import (
	"context"
	"sync"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/go-redis/redis/v8"
	logging "github.com/ipfs/go-log/v2"
	common "github.com/rqzrqh/sync_filecoin/common"
	"gorm.io/gorm"
)

var log = logging.Logger("local_chain")

type GrowRequest struct {
	fts                   *common.FullTipSet
	headChangeInfo        *common.HeadChangeInfo
	writePrevTsActorState bool
}

type LocalChainManager struct {
	dbChain              *DatabaseChain
	memChain             *MemoryChain
	cacheChain           *CacheChain
	growCh               chan *GrowRequest
	cacheChainHeadNotify chan CacheChainHead
	dbChainStateNotify   chan DatabaseChainState
	headChainLimit       int
	serialWriteMutex     sync.Mutex
}

func NewDbChainManager(ctx context.Context, db *gorm.DB, rds *redis.Client, memChain *MemoryChain, headChainLimit int, dbChainStateNotify chan DatabaseChainState) *LocalChainManager {

	dbChain := NewDatabaseChain(ctx, db)

	growCh := make(chan *GrowRequest, 10)
	cacheChainHeadNotify := make(chan CacheChainHead)

	cacheChain := newCacheChain(ctx, db, rds, cacheChainHeadNotify)

	return &LocalChainManager{
		dbChain:              dbChain,
		memChain:             memChain,
		cacheChain:           cacheChain,
		growCh:               growCh,
		cacheChainHeadNotify: cacheChainHeadNotify,
		dbChainStateNotify:   dbChainStateNotify,
		headChainLimit:       headChainLimit,
		serialWriteMutex:     sync.Mutex{},
	}
}

func (c *LocalChainManager) Start() {
	go c.run()
	c.cacheChain.start()
}

func (c *LocalChainManager) Stop() {
}

func (c *LocalChainManager) run() {
	for {
		select {
		case req := <-c.growCh:

			if err := c.dbChain.Grow(req.fts, req.headChangeInfo, req.writePrevTsActorState); err != nil {
				panic(err)
			}

		case cacheChainHead := <-c.cacheChainHeadNotify:

			var range1 int
			var range2 int

			cacheChainHeadHeight := cacheChainHead.height
			memChainHeadHeight := int64(c.memChain.GetChainHead().Height())
			irreversibleHeight := int64(c.memChain.GetLatestIrreversibleTipset().Height())

			// must keep lower branch as prune condition
			if cacheChainHeadHeight < memChainHeadHeight {
				range1 = int(cacheChainHeadHeight - irreversibleHeight)
				range2 = int(memChainHeadHeight - cacheChainHeadHeight)
			} else {
				range1 = int(memChainHeadHeight - irreversibleHeight)
				range2 = int(cacheChainHeadHeight - memChainHeadHeight)
			}

			if range1 > c.headChainLimit {

				range1 = c.headChainLimit

				newIrreversibleList := c.memChain.SetHeadChainLimit(range1 + range2)
				for _, heightInfo := range newIrreversibleList {
					headChainTs := heightInfo.Ts
					pi := heightInfo.Pi

					for {
						curTs, sameHeightHeadChainTs, ptsd := pi.Pop(headChainTs)
						if curTs == nil {
							break
						}

						deletePrevTsActorState := false

						// if parent in head chain, not delete state
						// if is nil, it's parent is irreversible
						if ptsd != nil && ptsd.RefCount() == 0 {
							deletePrevTsActorState = true
						}

						if err := c.dbChain.Prune(curTs, sameHeightHeadChainTs, deletePrevTsActorState); err != nil {
							panic(err)
						}
					}

					if err := c.dbChain.Pin(headChainTs); err != nil {
						panic(err)
					}
				}

				go func() {
					state := DatabaseChainState{
						Irreversible: c.memChain.GetLatestIrreversibleTipset(),
						ChainHead:    c.memChain.GetChainHead(),
					}
					c.dbChainStateNotify <- state
				}()
			}
		}
	}
}

// must wait until it has been written to memChain, thus memChain could keep consistency with outer.
func (c *LocalChainManager) Grow(fts *common.FullTipSet) error {

	// ensure write memchain and dbchain is consist for multi syncer groutine
	c.serialWriteMutex.Lock()
	defer func() {
		c.serialWriteMutex.Unlock()
	}()

	headChangeInfo, parentIsBranchHead, err := c.memChain.grow(fts.CurTs, fts.CurTsChangeAddressList)
	if err != nil {
		log.Warnw("memchain grow", "error", err, "height", fts.CurTs.Height(), "tsk", fts.CurTs.Key(), "ptsk", fts.CurTs.Parents())
		if err == common.ChainGrowErrTsExist {
			return nil
		}
		return err
	}

	req := GrowRequest{
		fts:                   fts,
		headChangeInfo:        headChangeInfo,
		writePrevTsActorState: parentIsBranchHead,
	}

	c.growCh <- &req

	return nil
}

func (c *LocalChainManager) GetReversibleTipSetData(tsk types.TipSetKey) (*types.TipSet, []address.Address, bool, error) {
	return c.memChain.getReversibleTipSetData(tsk)
}
