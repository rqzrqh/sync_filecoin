package porter

import (
	"context"
	"time"

	"github.com/filecoin-project/lotus/api"
	"github.com/ipfs/go-cid"
	"github.com/rqzrqh/sync_filecoin/local_chain"
	"github.com/rqzrqh/sync_filecoin/migrate"
	"go.uber.org/atomic"
)

type Syncer struct {
	ctx                  context.Context
	nodes                []*DataSourceNode
	nodeStateNotifyRx    []chan UpdateNodeCmd
	blockMessageManager  *BlockMessageManager
	pendingTipSetManager *PendingTipSetManager
	localChainManager    *local_chain.LocalChainManager
}

func newSyncer(ctx context.Context, nodes []*DataSourceNode, nodeStateNotify []chan UpdateNodeCmd, blockMessageManager *BlockMessageManager, pendingTipSetManager *PendingTipSetManager, localChainManager *local_chain.LocalChainManager) *Syncer {
	return &Syncer{
		ctx:                  ctx,
		nodes:                nodes,
		nodeStateNotifyRx:    nodeStateNotify,
		blockMessageManager:  blockMessageManager,
		pendingTipSetManager: pendingTipSetManager,
		localChainManager:    localChainManager,
	}
}

func (s *Syncer) Start() {
	go s.run()
}

func (s *Syncer) Stop() {

}

func (s *Syncer) run() {

	for i, n := range s.nodes {
		node := n
		idx := i

		go func() {

			nodeStateNotifyRx := s.nodeStateNotifyRx[idx]

			inProcess := atomic.NewBool(false)
			nodeValid := false

			timer := time.NewTicker(100 * time.Millisecond)
			defer timer.Stop()

			for {
				select {
				case <-timer.C:
					if nodeValid && !inProcess.Load() {
						inProcess.Store(true)

						go func() {
							defer func() {
								inProcess.Store(false)
							}()

							s.sync(node)
						}()
					}
				case cmd := <-nodeStateNotifyRx:
					if cmd.operate == Add {
						nodeValid = true
						log.Infof("syncer enable", node)
					} else if cmd.operate == Remove {
						nodeValid = false
						log.Infof("syncer disable", node)
					}
				}
			}
		}()
	}
}

func (s *Syncer) sync(node *DataSourceNode) {
	curTs, err := s.pendingTipSetManager.selectTipSetForSync(node)
	if err != nil {
		time.Sleep(1 * time.Second)
		return
	}

	prevTs, prevTsChangeAddressList, prevTsIsBranchHead, err := s.localChainManager.GetReversibleTipSetData(curTs.Parents())
	if err != nil {
		panic(err)
	}

	curHeightKnownBlockMessages := make(map[cid.Cid]*api.BlockMessages)
	for _, blk := range curTs.Blocks() {
		blockMessages := s.blockMessageManager.getBlockMessages(blk)
		if blockMessages != nil {
			curHeightKnownBlockMessages[blk.Cid()] = blockMessages
		}
	}

	fts, err := migrate.GetFullTipSet(s.ctx, node.api, prevTsChangeAddressList, prevTsIsBranchHead, prevTs, curTs, curHeightKnownBlockMessages)
	if err != nil {
		log.Warnf("syncer get fulltipset failed(%v), sleep", err)
		// remove it? and differ add it later?
		s.pendingTipSetManager.onSyncResult(node, curTs, err)
		time.Sleep(5 * time.Second)
		return
	}

	for idx, blk := range fts.CurTs.Blocks() {
		s.blockMessageManager.setBlockMessages(blk, fts.AllBlockMessages[idx], nil)
	}

	if err := s.localChainManager.Grow(fts); err != nil {
		log.Warnf("syncer grow failed(%v), sleep", err)
		s.pendingTipSetManager.onSyncResult(node, curTs, err)
		time.Sleep(5 * time.Second)
		return
	}

	s.pendingTipSetManager.onSyncResult(node, curTs, err)
}
