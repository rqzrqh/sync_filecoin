package porter

import (
	"sync"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/types"
	common "github.com/rqzrqh/sync_filecoin/common"
	"golang.org/x/xerrors"
)

type DataSourceChain struct {
	rwLock         sync.RWMutex
	chain          *common.Chain
	headChainLimit int
	dbChainHead    *types.TipSet
}

func newDataSourceChain(
	genesisTs *types.TipSet,
	irreversibleTs *types.TipSet,
	firstReversibleTs *types.TipSet,
	secondReversibleTs *types.TipSet,
	headChainLimit int,
) *DataSourceChain {

	reversibleTsList := make([]*types.TipSet, 0)
	reversibleTsList = append(reversibleTsList, firstReversibleTs)
	reversibleTsList = append(reversibleTsList, secondReversibleTs)

	dataSet := make(map[types.TipSetKey]interface{})
	dataSet[firstReversibleTs.Key()] = nil
	dataSet[secondReversibleTs.Key()] = nil

	headChainList := make([]*types.TipSet, 0)
	headChainList = append(headChainList, secondReversibleTs)

	return &DataSourceChain{
		rwLock:         sync.RWMutex{},
		chain:          common.NewChain(genesisTs, irreversibleTs, reversibleTsList, dataSet, headChainList),
		headChainLimit: headChainLimit,
		dbChainHead:    secondReversibleTs,
	}
}

func (dsc *DataSourceChain) grow(ts *types.TipSet) error {
	dsc.rwLock.Lock()
	defer dsc.rwLock.Unlock()

	// max height is dbChainHead.Height() + 2*headChainLimit, 2*headChainLimit was used for buffer unsynced tipset, it's enough.
	// TODO consider weight?
	if ts.Height() > (dsc.dbChainHead.Height() + abi.ChainEpoch(2*dsc.headChainLimit) + 10) {
		return xerrors.New("out of upper")
	}

	_, err := dsc.chain.Grow(ts, nil)
	if err != nil {
		//log.Infof("dsChain err=%v height=%v weight=%v key=%v", err, ts.Height(), ts.ParentWeight(), ts.Key())
	} else {
		//log.Infof("dsChain success height=%v weight=%v key=%v", ts.Height(), ts.ParentWeight(), ts.Key())
	}

	return err
}

func (dsc *DataSourceChain) cloneInner() *common.Chain {
	dsc.rwLock.RLock()
	defer dsc.rwLock.RUnlock()

	return dsc.chain.Clone()
}

func (dsc *DataSourceChain) getLatestIrreversibleTipset() *types.TipSet {
	dsc.rwLock.RLock()
	defer dsc.rwLock.RUnlock()

	return dsc.chain.GetLatestIrreversibleTipset()
}

func (dsc *DataSourceChain) contain(tsk types.TipSetKey) bool {
	dsc.rwLock.RLock()
	defer dsc.rwLock.RUnlock()

	return dsc.chain.ContainInBranch(tsk)
}

func (dsc *DataSourceChain) setBenchMark(dbIrreversible *types.TipSet, dbChainHead *types.TipSet) {
	dsc.rwLock.Lock()
	defer dsc.rwLock.Unlock()

	dsc.dbChainHead = dbChainHead
	// may set failed
	dsc.chain.SetIrreversibleTs(dbIrreversible)
}
