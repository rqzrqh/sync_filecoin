package local_chain

import (
	"sort"
	"sync"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/types"
	common "github.com/rqzrqh/sync_filecoin/common"
	"golang.org/x/xerrors"
)

type DatabaseChainState struct {
	Irreversible *types.TipSet
	ChainHead    *types.TipSet
}

type MemoryChain struct {
	rwLock sync.RWMutex
	chain  *common.Chain
}

func NewMemoryChain(genesisTs *types.TipSet, irreversibleTs *types.TipSet, reversibleTsList []*types.TipSet, reversibleTsChangedAddressList map[types.TipSetKey][]address.Address, headChainList []*types.TipSet) *MemoryChain {

	dataSet := make(map[types.TipSetKey]interface{})
	for k, v := range reversibleTsChangedAddressList {
		dataSet[k] = v
	}

	mc := &MemoryChain{
		rwLock: sync.RWMutex{},
		chain:  common.NewChain(genesisTs, irreversibleTs, reversibleTsList, dataSet, headChainList),
	}

	log.Infof("genesis height=%v %v", genesisTs.Height(), genesisTs.Key())
	log.Infof("irreversible height=%v %v", irreversibleTs.Height(), irreversibleTs.Key())
	log.Infof("reversible tipset count=%v", len(reversibleTsList))

	// get chain head
	chainHead := mc.chain.GetChainHead()
	log.Infof("chainhead height=%v tsk=%v", chainHead.Height(), chainHead.Key())

	// get branch heads
	type Head struct {
		tsk    types.TipSetKey
		height abi.ChainEpoch
	}

	branchHeads := mc.chain.GetBranchHeads()
	orderedBranchHeads := make([]Head, 0)

	for k, v := range branchHeads {
		head := Head{
			tsk:    k,
			height: v.Height(),
		}
		orderedBranchHeads = append(orderedBranchHeads, head)
	}

	sort.Slice(orderedBranchHeads, func(i, j int) bool {
		return orderedBranchHeads[i].height < orderedBranchHeads[j].height
	})

	for _, v := range orderedBranchHeads {
		log.Debugf("branchHead height=%v tsk=%v", v.height, v.tsk)
	}

	return mc
}

func (mc *MemoryChain) grow(ts *types.TipSet, changedAddressList []address.Address) (*common.HeadChangeInfo, bool, error) {

	mc.rwLock.Lock()
	defer mc.rwLock.Unlock()

	parentIsBranchHead := mc.chain.IsBranchHead(ts.Parents())

	headChangeInfo, err := mc.chain.Grow(ts, changedAddressList)

	return headChangeInfo, parentIsBranchHead, err
}

func (mc *MemoryChain) SetHeadChainLimit(limit int) []*common.IrreversibleHeightInfo {

	mc.rwLock.Lock()
	defer mc.rwLock.Unlock()

	return mc.chain.SetHeadChainLimit(limit)
}

func (mc *MemoryChain) GetLatestIrreversibleTipset() *types.TipSet {
	mc.rwLock.RLock()
	defer mc.rwLock.RUnlock()

	return mc.chain.GetLatestIrreversibleTipset()
}

func (mc *MemoryChain) GetChainHead() *types.TipSet {
	mc.rwLock.RLock()
	defer mc.rwLock.RUnlock()

	return mc.chain.GetChainHead()
}

func (mc *MemoryChain) CloneInner() *common.Chain {
	mc.rwLock.RLock()
	defer mc.rwLock.RUnlock()

	return mc.chain.Clone()
}

func (mc *MemoryChain) getReversibleTipSetData(tsk types.TipSetKey) (*types.TipSet, []address.Address, bool, error) {
	mc.rwLock.RLock()
	defer mc.rwLock.RUnlock()

	ts, data, err := mc.chain.GetReversibleTipSet(tsk)
	if err != nil {
		return nil, nil, false, xerrors.New("reversible tsk not exist")
	}

	isBranchHead := mc.chain.IsBranchHead(tsk)
	tsChangedAddressList := data.([]address.Address)

	return ts, tsChangedAddressList, isBranchHead, nil
}
