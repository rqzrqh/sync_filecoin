package porter

import (
	"sort"
	"sync"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/types"
	"golang.org/x/xerrors"
)

const (
	PendingStatusWaitSync  uint8 = 0
	PendingStatusInSyncing uint8 = 1
)

// WaitSync and InSyncing
type SyncData struct {
	curTs *types.TipSet
	nodes map[*DataSourceNode]struct{}
}

type TipSetInfo struct {
	status uint8
	data   interface{}
}

type CompleteTipSetInfo struct {
	ts *types.TipSet
	tm time.Time
}
type PendingTipSetManager struct {
	mtx           sync.Mutex
	heightTskSet  map[abi.ChainEpoch]map[types.TipSetKey]struct{}
	tskSet        map[types.TipSetKey]*TipSetInfo
	completeTsSet map[types.TipSetKey]*CompleteTipSetInfo
}

func newPendingTipSetManager() *PendingTipSetManager {
	return &PendingTipSetManager{
		mtx:           sync.Mutex{},
		heightTskSet:  make(map[abi.ChainEpoch]map[types.TipSetKey]struct{}),
		tskSet:        make(map[types.TipSetKey]*TipSetInfo),
		completeTsSet: make(map[types.TipSetKey]*CompleteTipSetInfo),
	}
}

func (ptsm *PendingTipSetManager) Start() {
	go func() {
		timer := time.NewTicker(5 * time.Second)
		defer timer.Stop()

		for {
			select {
			case <-timer.C:
				now := time.Now()

				ptsm.mtx.Lock()
				for k, v := range ptsm.completeTsSet {
					if now.Sub(v.tm) > 60*time.Second {
						log.Debugw("ptsm clean", "tsk", v.ts.Key(), "height", v.ts.Height())
						delete(ptsm.completeTsSet, k)
					}
				}
				ptsm.mtx.Unlock()
			}
		}
	}()
}

func (ptsm *PendingTipSetManager) appendDifferences(diffSet map[types.TipSetKey]*types.TipSet, node *DataSourceNode) {

	ptsm.mtx.Lock()
	defer func() {
		ptsm.mtx.Unlock()
	}()

	for _, ts := range diffSet {

		//log.Infof("add=%v %v %v", ts.Height(), ts.ParentWeight(), ts.Key())

		tsk := ts.Key()
		height := ts.Height()

		{
			_, exist := ptsm.completeTsSet[tsk]
			if exist {
				continue
			}
		}

		if _, exist := ptsm.tskSet[tsk]; exist {
			// TODO
			// check status
			// compare node
		} else {
			nodes := make(map[*DataSourceNode]struct{})
			nodes[node] = struct{}{}

			data := &SyncData{
				curTs: ts,
				nodes: nodes,
			}
			info := &TipSetInfo{
				status: PendingStatusWaitSync,
				data:   data,
			}
			ptsm.tskSet[tsk] = info

			if tsKeyMap, heightExist := ptsm.heightTskSet[height]; heightExist {
				tsKeyMap[tsk] = struct{}{}
			} else {
				set := make(map[types.TipSetKey]struct{})
				set[tsk] = struct{}{}
				ptsm.heightTskSet[height] = set
			}
		}
	}
}

func (ptsm *PendingTipSetManager) selectTipSetForSync(node *DataSourceNode) (*types.TipSet, error) {

	ptsm.mtx.Lock()
	defer func() {
		ptsm.mtx.Unlock()
	}()

	var heights []abi.ChainEpoch
	for h := range ptsm.heightTskSet {
		heights = append(heights, h)
	}

	sort.Slice(heights, func(i, j int) bool {
		return heights[i] < heights[j]
	})

	count := 0

	for _, h := range heights {
		tsKeyMap := ptsm.heightTskSet[h]
		for tsk := range tsKeyMap {

			count++

			if count > 20 {
				return nil, xerrors.New("too much is not match")
			}

			info := ptsm.tskSet[tsk]

			if info.status != PendingStatusWaitSync {
				continue
			}

			data := info.data.(*SyncData)

			// parent should not exist
			if _, exist := ptsm.tskSet[data.curTs.Parents()]; exist {
				continue
			}

			_, exist := data.nodes[node]
			if !exist {
				continue
			}

			info.status = PendingStatusInSyncing

			return data.curTs, nil
		}
	}

	return nil, xerrors.New("not found any valid wait sync ts")
}

// always think write is success
func (ptsm *PendingTipSetManager) onSyncResult(node *DataSourceNode, ts *types.TipSet, err error) {
	ptsm.mtx.Lock()
	defer func() {
		ptsm.mtx.Unlock()
	}()

	tsk := ts.Key()
	height := ts.Height()

	var info *TipSetInfo
	exist := false

	info, exist = ptsm.tskSet[tsk]

	if !exist {
		// warn height not exist
		return
	}

	if info.status != PendingStatusInSyncing {
		// warn tsInfo status error
		return
	}

	if err != nil {
		info.status = PendingStatusWaitSync
		return
	}

	delete(ptsm.tskSet, tsk)
	tskMap, _ := ptsm.heightTskSet[height]
	delete(tskMap, tsk)
	if len(tskMap) == 0 {
		delete(ptsm.heightTskSet, height)
	}

	completeInfo := CompleteTipSetInfo{
		ts: ts,
		tm: time.Now(),
	}

	ptsm.completeTsSet[ts.Key()] = &completeInfo
}
