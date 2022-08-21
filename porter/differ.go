package porter

import (
	"time"

	"github.com/filecoin-project/lotus/chain/types"
	"github.com/rqzrqh/sync_filecoin/local_chain"
)

type Differ struct {
	dataSource *DataSource
	memChain   *local_chain.MemoryChain
	ptsm       *PendingTipSetManager
}

func newDiffer(dataSource *DataSource, memChain *local_chain.MemoryChain, ptsm *PendingTipSetManager) *Differ {
	return &Differ{
		dataSource: dataSource,
		memChain:   memChain,
		ptsm:       ptsm,
	}
}

func (d *Differ) Start() {

	go func() {
		timer := time.NewTicker(1 * time.Second)
		defer timer.Stop()

		for {
			select {
			case <-timer.C:
				d.check()
			}
		}
	}()
}

func (d *Differ) check() {

	dsNodes := d.dataSource.GetNodes()
	dbChain := d.memChain.CloneInner()

	for _, n := range dsNodes {

		node := n
		dsChain := node.getChain()

		go func() {

			newDsChain := dsChain.cloneInner()
			newDsChain.SetIrreversibleTs(dbChain.GetLatestIrreversibleTipset())

			// if multi lotus, one is fall behind, below error could occur.
			if newDsChain.GetLatestIrreversibleTipset().Key() != dbChain.GetLatestIrreversibleTipset().Key() {
				panic("irreversible ts not equal")
			}

			dsChainBranchHeadTsSet := newDsChain.GetBranchHeads()
			dsChainBranchTsSet := newDsChain.GetBranchSet()

			diffSet := make(map[types.TipSetKey]*types.TipSet)

			for _, branchHeadTs := range dsChainBranchHeadTsSet {

				curTs := branchHeadTs

				for {
					prevTs, exist := dsChainBranchTsSet[curTs.Parents()]
					if !exist {
						break
					}

					if dbChain.ContainInBranch(curTs.Key()) {
						break
					}

					diffSet[curTs.Key()] = curTs

					curTs = prevTs
				}
			}

			d.ptsm.appendDifferences(diffSet, node)
		}()
	}
}
