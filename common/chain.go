package common

import (
	"sort"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/types"
	"golang.org/x/xerrors"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("common")

var (
	ChainGrowErrTsExist        = xerrors.New("tipset exist")
	ChainGrowErrParentNotExist = xerrors.New("parent not exist")
)

type TipSetData struct {
	ts   *types.TipSet
	data interface{}
}

type TipSetPruneInfo struct {
	ts                    *types.TipSet
	sameHeightHeadChainTs *types.TipSet
	refCount              int
}

func (tspi *TipSetPruneInfo) RefCount() int {
	return tspi.refCount
}

type PruneInfo struct {
	set         map[types.TipSetKey]*TipSetPruneInfo
	branchHeads map[types.TipSetKey]struct{}
}

// from high to low
func (pi *PruneInfo) Pop(headChainTs *types.TipSet) (*types.TipSet, *types.TipSet, *TipSetPruneInfo) {

	for tsk := range pi.branchHeads {

		data := pi.set[tsk]
		ts := data.ts
		sameHeightHeadChainTs := data.sameHeightHeadChainTs

		if data.refCount != 0 {
			log.Errorf("prune refcount error. refcount=%v height=%v key=%v", data.refCount, ts.Height(), ts.Key())
			panic("prune info bug refcount error")
		}

		ptsk := ts.Parents()

		ptsd, exist := pi.set[ptsk]
		if exist {
			ptsd.refCount--
			if ptsd.refCount == 0 {
				pi.branchHeads[ptsk] = struct{}{}
			}
		}

		delete(pi.set, tsk)
		delete(pi.branchHeads, tsk)

		return ts, sameHeightHeadChainTs, ptsd
	}

	if len(pi.set) != 0 {
		log.Errorf("prune left count=%v", len(pi.set))
		for _, v := range pi.set {
			log.Errorf("left=%v  %v", v.ts.Height(), v.ts.Key())
		}
		panic("prune info bug")
	}

	return nil, nil, nil
}

type IrreversibleHeightInfo struct {
	Ts *types.TipSet
	Pi *PruneInfo
}

// the condition1 is essential.
// however, the condition2 is an optimization, it can decrease cache's revert effectively.
func compareWeightAndHeight(oldTsd *TipSetData, newTsd *TipSetData) bool {
	return newTsd.ts.ParentWeight().GreaterThan(oldTsd.ts.ParentWeight()) ||
		(newTsd.ts.Height() == oldTsd.ts.Height() && len(newTsd.ts.Blocks()) > len(oldTsd.ts.Blocks()))
}

type Chain struct {
	genesisTs      *types.TipSet
	irreversibleTs *types.TipSet
	// reversible
	allReversibleTipSet map[types.TipSetKey]*TipSetData
	branchHeads         map[types.TipSetKey]abi.ChainEpoch
	chainHead           types.TipSetKey
}

// need strict check
func NewChain(genesisTs *types.TipSet, irreversibleTs *types.TipSet, reversibleTsList []*types.TipSet, reversibleTsData map[types.TipSetKey]interface{}, headChainList []*types.TipSet) *Chain {

	count := len(reversibleTsList)
	orderedReversibleTsDataList := make([]*TipSetData, count)
	for i := 0; i < count; i++ {
		ts := reversibleTsList[i]
		data := reversibleTsData[ts.Key()]
		tsd := TipSetData{
			ts:   ts,
			data: data,
		}
		orderedReversibleTsDataList[i] = &tsd
	}

	// must sort by height in ascending order!!!
	sort.Slice(orderedReversibleTsDataList, func(i, j int) bool {
		return orderedReversibleTsDataList[i].ts.Height() < orderedReversibleTsDataList[j].ts.Height()
	})

	allReversibleTipSet := make(map[types.TipSetKey]*TipSetData)
	branchHeads := make(map[types.TipSetKey]abi.ChainEpoch)

	for i := 0; i < count; i++ {
		tsd := orderedReversibleTsDataList[i]
		// add all to the Set
		if i == 0 {
			allReversibleTipSet[tsd.ts.Key()] = tsd
		} else {
			if _, exist := allReversibleTipSet[tsd.ts.Key()]; exist {
				log.Errorw("newchain key exist", "height", tsd.ts.Height(), "key", tsd.ts.Key())
				panic("newchain key exist")
			}

			if _, exist := allReversibleTipSet[tsd.ts.Parents()]; !exist {
				log.Errorw("newchain can not found parent", "height", tsd.ts.Height(), "key", tsd.ts.Key(), "parent", tsd.ts.Parents())
				panic("newchain can not found parent")
			}
			allReversibleTipSet[tsd.ts.Key()] = tsd
		}

		// get branchHeads
		delete(branchHeads, tsd.ts.Parents())
		branchHeads[tsd.ts.Key()] = tsd.ts.Height()
	}

	// check irreversible
	if irreversibleTs.Key() != orderedReversibleTsDataList[0].ts.Parents() {
		log.Errorw("irreversible", "height", irreversibleTs.Height(), "key", irreversibleTs.Key())
		log.Errorw("reversible", "height", orderedReversibleTsDataList[0].ts.Height(), "key", orderedReversibleTsDataList[0].ts.Key(), "parent", orderedReversibleTsDataList[0].ts.Parents())
		panic("irreversible not connect")
	}

	sort.Slice(headChainList, func(i, j int) bool {
		return headChainList[i].Height() < headChainList[j].Height()
	})

	headChainCount := len(headChainList)

	return &Chain{
		genesisTs:           genesisTs,
		irreversibleTs:      irreversibleTs,
		allReversibleTipSet: allReversibleTipSet,
		branchHeads:         branchHeads,
		chainHead:           headChainList[headChainCount-1].Key(),
	}
}

func (c *Chain) Clone() *Chain {
	allReversibleTipSet := make(map[types.TipSetKey]*TipSetData)
	for k, v := range c.allReversibleTipSet {
		allReversibleTipSet[k] = v
	}

	branchHeads := make(map[types.TipSetKey]abi.ChainEpoch)
	for k, v := range c.branchHeads {
		branchHeads[k] = v
	}

	return &Chain{
		genesisTs:           c.genesisTs,
		irreversibleTs:      c.irreversibleTs,
		allReversibleTipSet: allReversibleTipSet,
		branchHeads:         branchHeads,
		chainHead:           c.chainHead,
	}
}

func (c *Chain) Grow(ts *types.TipSet, data interface{}) (*HeadChangeInfo, error) {
	return c.grow(ts, data)
}

func (c *Chain) SetHeadChainLimit(limit int) []*IrreversibleHeightInfo {
	irreversibleList := c.setHeadChainLimit(limit)
	if count := len(irreversibleList); count > 0 {
		latestIrreversibleTipSet := irreversibleList[count-1].Ts
		c.irreversibleTs = latestIrreversibleTipSet
	}

	return irreversibleList
}

func (c *Chain) SetIrreversibleTs(ts *types.TipSet) []*IrreversibleHeightInfo {

	var irreversibleList []*IrreversibleHeightInfo

	if ts.Height() <= c.irreversibleTs.Height() || c.irreversibleTs.Key() == ts.Key() {
		return irreversibleList
	}

	irreversibleList = c.setIrreversibleTs(ts)
	if count := len(irreversibleList); count > 0 {
		latestIrreversibleTipSet := irreversibleList[count-1].Ts
		c.irreversibleTs = latestIrreversibleTipSet
	}

	return irreversibleList
}

func (c *Chain) GetGenesisTipSet() *types.TipSet {
	return c.genesisTs
}

func (c *Chain) GetLatestIrreversibleTipset() *types.TipSet {
	return c.irreversibleTs
}

func (c *Chain) GetBranchHeads() map[types.TipSetKey]*types.TipSet {

	branchHeads := make(map[types.TipSetKey]*types.TipSet)

	for k := range c.branchHeads {
		tsd := c.allReversibleTipSet[k]
		branchHeads[k] = tsd.ts
	}

	return branchHeads
}

func (c *Chain) GetChainHead() *types.TipSet {
	tsd := c.allReversibleTipSet[c.chainHead]
	return tsd.ts
}

func (c *Chain) GetBranchSet() map[types.TipSetKey]*types.TipSet {

	all := make(map[types.TipSetKey]*types.TipSet)
	for k, v := range c.allReversibleTipSet {
		all[k] = v.ts
	}

	return all
}

func (c *Chain) ContainInBranch(tsk types.TipSetKey) bool {
	_, exist := c.allReversibleTipSet[tsk]
	return exist
}

func (c *Chain) GetReversibleTipSet(tsk types.TipSetKey) (*types.TipSet, interface{}, error) {

	tsd, exist := c.allReversibleTipSet[tsk]
	if !exist {
		return nil, nil, xerrors.New("reversible get tipset not exist")
	}

	return tsd.ts, tsd.data, nil
}

func (c *Chain) IsBranchHead(tsk types.TipSetKey) bool {
	_, isBranchHead := c.branchHeads[tsk]
	return isBranchHead
}

func (c *Chain) grow(ts *types.TipSet, data interface{}) (*HeadChangeInfo, error) {
	var exist bool
	var headChangeInfo HeadChangeInfo

	// 1. grow, append on it's parent
	_, exist = c.allReversibleTipSet[ts.Key()]
	if exist {
		return nil, ChainGrowErrTsExist
	}

	_, exist = c.allReversibleTipSet[ts.Parents()]
	if !exist {
		return nil, ChainGrowErrParentNotExist
	}

	tsData := &TipSetData{ts: ts, data: data}
	c.allReversibleTipSet[ts.Key()] = tsData
	delete(c.branchHeads, ts.Parents())
	c.branchHeads[ts.Key()] = ts.Height()

	oldHead := c.allReversibleTipSet[c.chainHead]

	// the only place where Porter check the weight
	if compareWeightAndHeight(oldHead, tsData) {

		c.chainHead = ts.Key()

		newHeadChainTs := ts
		oldHeadChainTs := oldHead.ts

		for {
			if newHeadChainTs.Key() == oldHeadChainTs.Key() {
				break
			}

			if newHeadChainTs.Height() > oldHeadChainTs.Height() {
				headChangeInfo.NewHeadChain = append(headChangeInfo.NewHeadChain, newHeadChainTs)
				newHeadChainTs = c.allReversibleTipSet[newHeadChainTs.Parents()].ts

			} else if newHeadChainTs.Height() < oldHeadChainTs.Height() {
				headChangeInfo.OldHeadChain = append(headChangeInfo.OldHeadChain, oldHeadChainTs)
				oldHeadChainTs = c.allReversibleTipSet[oldHeadChainTs.Parents()].ts

			} else {

				headChangeInfo.NewHeadChain = append(headChangeInfo.NewHeadChain, newHeadChainTs)
				newHeadChainTs = c.allReversibleTipSet[newHeadChainTs.Parents()].ts

				headChangeInfo.OldHeadChain = append(headChangeInfo.OldHeadChain, oldHeadChainTs)
				oldHeadChainTs = c.allReversibleTipSet[oldHeadChainTs.Parents()].ts
			}
		}
	}

	return &headChangeInfo, nil
}

func (c *Chain) getHeadChain() []*types.TipSet {
	var headChainPath []*types.TipSet
	{
		tsk := c.chainHead
		for {
			tsd, exist := c.allReversibleTipSet[tsk]
			if !exist {
				break
			}
			headChainPath = append(headChainPath, tsd.ts)
			tsk = tsd.ts.Parents()
		}
	}

	return headChainPath
}

func (c *Chain) setIrreversibleTs(ts *types.TipSet) []*IrreversibleHeightInfo {

	var toBeIrreversibleList []*IrreversibleHeightInfo

	// should not be any branch tail
	if _, exist := c.branchHeads[ts.Key()]; exist {
		log.Errorw("setIrreversibleTs failed. ts is branch tail", "key", ts.Key(), "height", ts.Height())
		return toBeIrreversibleList
	}

	// get tipset need to be irreversible
	var tsList []*types.TipSet
	tsk := ts.Key()
	for {
		tsd, exist := c.allReversibleTipSet[tsk]
		if exist {
			tsList = append(tsList, tsd.ts)
			tsk = tsd.ts.Parents()
		} else {
			break
		}
	}

	// get headchain path
	fullHeadChainPath := c.getHeadChain()
	removedHeadChainPathTsks := make(map[types.TipSetKey]*types.TipSet)
	for _, ts := range fullHeadChainPath {
		removedHeadChainPathTsks[ts.Key()] = ts
	}
	/*
		// for debug
		fmt.Printf("%p", r)
		fmt.Println("input", ts.Key(), ts.Height())

		var orderList []*types.TipSet
		for _, ts := range removedHeadChainPathTsks {
			orderList = append(orderList, ts)
		}

		sort.Slice(orderList, func(i, j int) bool {
			return orderList[i].Height() < orderList[j].Height()
		})

		for _, ts := range orderList {
			fmt.Println("headchain", ts.Key(), ts.Height())
		}

		for _, ts := range tsList {
			fmt.Println("path", ts.Key(), ts.Height())
		}
	*/
	// irreversible ts should in head chain
	for _, ts := range tsList {
		_, exist := removedHeadChainPathTsks[ts.Key()]
		if !exist {
			panic("irreversible not on the head chain path")
		}
	}

	count := len(tsList)
	if count <= 0 {
		return toBeIrreversibleList
	}

	// remove from branches
	for i := 0; i < count; i++ {
		ts := tsList[count-1-i]

		pi := PruneInfo{
			set:         make(map[types.TipSetKey]*TipSetPruneInfo),
			branchHeads: make(map[types.TipSetKey]struct{}),
		}
		heightInfo := IrreversibleHeightInfo{
			Ts: ts,
			Pi: &pi,
		}

		toBeIrreversibleList = append(toBeIrreversibleList, &heightInfo)

		// remove these tipset
		delete(c.allReversibleTipSet, ts.Key())
		delete(removedHeadChainPathTsks, ts.Key())
	}

	c.collectIrreversibleResult(fullHeadChainPath, toBeIrreversibleList, removedHeadChainPathTsks)

	return toBeIrreversibleList
}

func (c *Chain) setHeadChainLimit(limit int) []*IrreversibleHeightInfo {
	var irreversibleList []*IrreversibleHeightInfo

	// 1. get heights which would be irreversible
	// get headchain path
	fullHeadChainPath := c.getHeadChain()
	removedHeadChainPathTsks := make(map[types.TipSetKey]*types.TipSet)
	for _, ts := range fullHeadChainPath {
		removedHeadChainPathTsks[ts.Key()] = ts
	}

	// get heights which would be irreversible
	headChainLen := len(fullHeadChainPath)
	count := headChainLen - limit
	if count <= 0 {
		return irreversibleList
	}

	for i := 0; i < count; i++ {
		ts := fullHeadChainPath[headChainLen-1-i]

		pi := PruneInfo{
			set:         make(map[types.TipSetKey]*TipSetPruneInfo),
			branchHeads: make(map[types.TipSetKey]struct{}),
		}
		heightInfo := IrreversibleHeightInfo{
			Ts: ts,
			Pi: &pi,
		}

		irreversibleList = append(irreversibleList, &heightInfo)

		// remove these tipset
		// do not need remove from branches, it must not in branches
		delete(c.allReversibleTipSet, ts.Key())
		delete(removedHeadChainPathTsks, ts.Key())
	}

	// 2. get branches need to be pruned
	c.collectIrreversibleResult(fullHeadChainPath, irreversibleList, removedHeadChainPathTsks)

	return irreversibleList
}

func (c *Chain) collectIrreversibleResult(fullHeadChainPath []*types.TipSet, irreversibleList []*IrreversibleHeightInfo, removedHeadChainPathTsks map[types.TipSetKey]*types.TipSet) {

	// get all branch's full path
	var branchesPath [][]*types.TipSet
	for latestTsk, _ := range c.branchHeads {

		if latestTsk == c.chainHead {
			continue
		}

		tsk := latestTsk
		var path []*types.TipSet
		for {
			tsd, exist := c.allReversibleTipSet[tsk]
			if !exist {
				break
			}
			path = append(path, tsd.ts)
			tsk = tsd.ts.Parents()
		}

		branchesPath = append(branchesPath, path)
	}

	// filter out those branches who should be pruned
	var pruneBranchesPath [][]*types.TipSet
	for _, path := range branchesPath {

		firstTipSet := path[len(path)-1]

		if _, exist := removedHeadChainPathTsks[firstTipSet.Key()]; !exist {
			pruneBranchesPath = append(pruneBranchesPath, path)
		}
	}

	// group by the to-be-irreversible tipset key which grow based on
	groupedPruneBranches := make(map[types.TipSetKey][][]*types.TipSet)
	for _, path := range pruneBranchesPath {

		firstTipSet := path[len(path)-1]

		x, exist := groupedPruneBranches[firstTipSet.Parents()]
		if !exist {
			var groupPath [][]*types.TipSet
			groupPath = append(groupPath, path)
			groupedPruneBranches[firstTipSet.Parents()] = groupPath
		} else {
			x := append(x, path)
			groupedPruneBranches[firstTipSet.Parents()] = x
		}
	}

	fullHeadChainHeightSet := make(map[abi.ChainEpoch]*types.TipSet)
	for _, v := range fullHeadChainPath {
		fullHeadChainHeightSet[v.Height()] = v
	}

	// calc reference relationship
	for _, hi := range irreversibleList {
		pathList := groupedPruneBranches[hi.Ts.Key()]

		pi := hi.Pi

		for _, path := range pathList {
			for _, ts := range path {
				sameHeightHeadChainTs := fullHeadChainHeightSet[ts.Height()]

				info := TipSetPruneInfo{
					ts:                    ts,
					sameHeightHeadChainTs: sameHeightHeadChainTs,
					refCount:              0,
				}
				pi.set[ts.Key()] = &info
			}
			pi.branchHeads[path[0].Key()] = struct{}{}
		}

		for _, path := range pathList {
			for _, p := range path {
				a, exist := pi.set[p.Parents()]
				if exist {
					a.refCount++
				}
			}
		}
		/*
			if hi.Ts.Height() == 1597580 {
				for k := range pi.branchHeads {
					fmt.Println("#####")
					tsk := k
					for {
						v, ext := pi.set[tsk]
						if !ext {
							break
						}
						fmt.Println(v.ts.Height(), v.refCount, v.ts.Key())
						tsk = v.ts.Parents()
					}
					fmt.Println("#####")
				}
			}
		*/
	}
}
