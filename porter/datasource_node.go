package porter

import (
	"context"
	"sync"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
)

const (
	Add    int8 = 0
	Remove int8 = 1
)

type UpdateNodeCmd struct {
	operate int8
	node    *DataSourceNode
}

type IncomingBlock struct {
	blk  *types.BlockHeader
	node *DataSourceNode
}

type DataSourceNode struct {
	ctx            context.Context
	genesisTs      *types.TipSet
	api            api.FullNode
	chain          *DataSourceChain
	headChainLimit int
	isSyncStart    *atomic.Bool
}

func newDataSourceNode(
	ctx context.Context,
	dbGenesisTs *types.TipSet,
	dbIrreversible *types.TipSet,
	dbChainHead *types.TipSet,
	headChainLimit int,
	node api.FullNode,
) *DataSourceNode {
	nodeGenesisTs, err := node.ChainGetGenesis(ctx)

	if err != nil {
		log.Errorf("ChainGetGenesis error: %+v", err)
		panic("Init error")
	}

	if nodeGenesisTs.Key() != dbGenesisTs.Key() {
		panic("Genesis not equal")
	}

	firstReversibleTs, err := node.ChainGetTipSetAfterHeight(ctx, dbIrreversible.Height()+1, types.EmptyTSK)

	if err != nil {
		log.Errorf("ChainGetTipSet error: %+v", err)
		panic("Init error")
	}

	if dbIrreversible.Key() != firstReversibleTs.Parents() {
		panic("first reversible not connect with second reversible")
	}

	secondReversibleTs, err := node.ChainGetTipSetAfterHeight(ctx, firstReversibleTs.Height()+1, types.EmptyTSK)

	if err != nil {
		log.Errorf("ChainGetTipSet error: %+v", err)
		panic("Init error")
	}

	if firstReversibleTs.Key() != secondReversibleTs.Parents() {
		panic("first reversible not connect with second reversible")
	}

	dsChain := newDataSourceChain(
		nodeGenesisTs,
		dbIrreversible,
		firstReversibleTs,
		secondReversibleTs,
		headChainLimit,
	)

	return &DataSourceNode{
		ctx:            ctx,
		genesisTs:      nodeGenesisTs,
		api:            node,
		chain:          dsChain,
		headChainLimit: headChainLimit,
		isSyncStart:    atomic.NewBool(false),
	}
}

func (node *DataSourceNode) align(dbIrreversible *types.TipSet, dbChainHead *types.TipSet) {
	node.chain.setBenchMark(dbIrreversible, dbChainHead)
}

func (node *DataSourceNode) getChain() *DataSourceChain {
	return node.chain
}

func (node *DataSourceNode) grow(rawBranchHeads []*types.TipSet) {
	var branchHeads []*types.TipSet

	// get heads of branches need to sync
	for _, ts := range rawBranchHeads {
		if node.chain.contain(ts.Key()) {
			continue
		}

		if err := node.chain.grow(ts); err == nil {
			continue
		}

		branchHeads = append(branchHeads, ts)
	}

	if len(branchHeads) == 0 {
		return
	}

	// only one worker
	if !node.isSyncStart.CAS(false, true) {
		return
	}

	go func() {
		defer node.isSyncStart.Store(false)

		dsChainBranchHeads := node.chain.chain.GetBranchHeads()

		// get ds highest and remote lowest, compare and decide how to get missing tipset
		var dsHeadHighestTs *types.TipSet
		for _, v := range dsChainBranchHeads {
			if dsHeadHighestTs == nil || v.Height() > dsHeadHighestTs.Height() {
				dsHeadHighestTs = v
			}
		}

		var remoteHeadLowestTs *types.TipSet
		for _, v := range branchHeads {
			if remoteHeadLowestTs == nil || v.Height() < remoteHeadLowestTs.Height() {
				remoteHeadLowestTs = v
			}
		}

		// from low to high
		// beyond dbHeadChainLimit range's tipset can sync from low to high
		for (remoteHeadLowestTs.Height() - dsHeadHighestTs.Height()) > abi.ChainEpoch(node.headChainLimit) {

			//fmt.Println("sync ", dsHeadHighestTs.Height()+1)

			// these tipset are considered as irreversible
			// from low to high by height, so Syncer can work immediately once program start.
			ts, err := node.api.ChainGetTipSetAfterHeight(node.ctx, dsHeadHighestTs.Height()+1, types.EmptyTSK)
			if err != nil {
				log.Errorf("DataSourceNode error: %+v", err)
				return
			}

			if dsHeadHighestTs.Key() != ts.Parents() {
				log.Warnf("key not continuous means headchange, maybe dbHeadChainLimit is too small")
				// break and try from head to tail
				branchHeads = []*types.TipSet{}
				branchHeads = append(branchHeads, ts)
				break
			}

			if err := node.chain.grow(ts); err != nil {
				log.Warnf("DataSourceChain grow error: %+v", err)
				return
			}

			dsHeadHighestTs = ts
		}

		// from branch's head to tail, from high to low by tipset key, ensure recently reversible branches can be handled.
		mtx := sync.Mutex{}
		tsSet := make(map[types.TipSetKey]*types.TipSet)
		var allBranches []*types.TipSet

		// from high to low
		grp, _ := errgroup.WithContext(node.ctx)
		for _, v := range branchHeads {

			head := v

			grp.Go(func() error {

				cts := head

				mtx.Lock()
				allBranches = append(allBranches, cts)
				mtx.Unlock()

				for {
					tsk := cts.Parents()
					if node.chain.contain(tsk) {
						break
					}

					mtx.Lock()
					if _, exist := tsSet[tsk]; exist {
						mtx.Unlock()
						break
					}
					mtx.Unlock()

					ts, err := node.api.ChainGetTipSet(node.ctx, tsk)
					if err != nil {
						break
					}

					if ts.Height() < node.chain.getLatestIrreversibleTipset().Height() {
						log.Warnf("branch path beyond the irreversible height, headchainlimit too small?. %v %v %v", ts.Height(), node.chain.getLatestIrreversibleTipset().Height(), head.Height())
						break
					}

					mtx.Lock()
					if _, exist := tsSet[tsk]; exist {
						mtx.Unlock()
						break
					}

					tsSet[tsk] = ts
					allBranches = append(allBranches, ts)
					mtx.Unlock()

					cts = ts
				}

				return nil
			})
		}

		grp.Wait()

		// try grow all synced branches
		count := len(allBranches)
		if count > 0 {
			for i := count - 1; i >= 0; i-- {
				node.chain.grow(allBranches[i])
			}
		}
	}()
}

func (node *DataSourceNode) listenChainNotify(nodeStateNotify chan<- UpdateNodeCmd) {
	var notify <-chan []*api.HeadChange
	var err error

	curHeight := abi.ChainEpoch(-1)

	id := 0

	for {
		if notify == nil {
			notify, err = node.api.ChainNotify(node.ctx)
			if err != nil {
				log.Errorf("ChainNotify error: %+v", err)
				time.Sleep(3 * time.Second)
				continue
			} else {
				log.Infof("remote ChainNotify success")
				nodeStateNotify <- UpdateNodeCmd{operate: Add, node: node}
			}
		}

		timer := time.NewTicker(60 * time.Second)

		for {
			select {
			case changes, ok := <-notify:
				id++
				if !ok {
					nodeStateNotify <- UpdateNodeCmd{operate: Remove, node: node}

					log.Warnf("ChainNotify channel closed")
					notify = nil
					curHeight = abi.ChainEpoch(-1)
					break
				}

				if len(changes) == 0 {
					continue
				}

				var branchHeads []*types.TipSet

				for _, event := range changes {
					//log.Infof("#### %v %v %v %v", id, event.Type, event.Val.Height(), event.Val.Key())

					if event.Type == store.HCCurrent {
						// first connect return head
						branchHeads = append(branchHeads, event.Val)
					} else if event.Type == store.HCApply {
						branchHeads = append(branchHeads, event.Val)
					}
				}

				// if want to get all tipset for fork test, set false
				if true {
					maxHeight := abi.ChainEpoch(-1)
					for _, ts := range branchHeads {
						if ts.Height() > maxHeight {
							maxHeight = ts.Height()
						}
					}

					if curHeight == -1 || maxHeight > curHeight {
						timer.Reset(60 * time.Second)
						// update curHeight, whether sync success or failure
						curHeight = maxHeight
						go func() {
							// delay and get a stable chain head, decrease sync fulltipset and database load
							time.Sleep(10 * time.Second)
							chainHead, err := node.api.ChainHead(node.ctx)
							if err == nil {
								var newBranchHeads []*types.TipSet
								newBranchHeads = append(newBranchHeads, chainHead)
								node.grow(newBranchHeads)
							}
						}()
					}
				} else {
					node.grow(branchHeads)
				}

			case <-timer.C:
				go func() {
					chainHead, err := node.api.ChainHead(node.ctx)
					if err == nil {
						var newBranchHeads []*types.TipSet
						newBranchHeads = append(newBranchHeads, chainHead)
						node.grow(newBranchHeads)
					}
				}()
			case <-node.ctx.Done():
				return
			}

			if notify == nil {
				break
			}
		}

		timer.Stop()
	}
}

func (node *DataSourceNode) listenSyncIncomingBlocks(incomingBlockNotify chan<- IncomingBlock) {
	var ch <-chan *types.BlockHeader
	var err error

	for {
		if ch == nil {
			ch, err = node.api.SyncIncomingBlocks(node.ctx)
			if err != nil {
				log.Errorf("SyncIncomingBlocks error: %+v", err)
				build.Clock.Sleep(3 * time.Second)
				continue
			}
		}

		select {
		case incomingBlocks, ok := <-ch:
			if !ok {
				log.Warnf("SyncIncomingBlocks channel closed")
				ch = nil
				continue
			}

			go func() {
				incomingBlockNotify <- IncomingBlock{blk: incomingBlocks, node: node}
			}()

		case <-node.ctx.Done():
			return
		}
	}
}

func (node *DataSourceNode) start(nodeStateNotify chan<- UpdateNodeCmd, incomingBlockNotify chan<- IncomingBlock) {
	go node.listenChainNotify(nodeStateNotify)
	go node.listenSyncIncomingBlocks(incomingBlockNotify)
}
