package porter

import (
	"context"
	"sort"
	"sync"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
)

const (
	BlockMessageStatusWaitSync  uint8 = 0
	BlockMessageStatusInSyncing uint8 = 1
	BlockMessageStatusComplete  uint8 = 2
)

type BlockData struct {
	bh            *types.BlockHeader
	blockMessages *api.BlockMessages
	nodes         map[*DataSourceNode]struct{}
	status        uint8
}

type BlockMessageManager struct {
	ctx                 context.Context
	incomingBlockNotify chan IncomingBlock
	allHeightBlocks     map[abi.ChainEpoch]map[cid.Cid]*BlockData
	mtx                 sync.RWMutex
}

func newBlockMessageManager(ctx context.Context, incomingBlockNotify chan IncomingBlock) *BlockMessageManager {
	return &BlockMessageManager{
		ctx:                 ctx,
		incomingBlockNotify: incomingBlockNotify,
		allHeightBlocks:     make(map[abi.ChainEpoch]map[cid.Cid]*BlockData),
		mtx:                 sync.RWMutex{},
	}
}

func (b *BlockMessageManager) start() {

	go func() {
		for {
			select {
			case incoming := <-b.incomingBlockNotify:

				blk := incoming.blk
				node := incoming.node

				height := blk.Height
				blkCid := blk.Cid()

				b.mtx.Lock()
				if heightBlocks, exist := b.allHeightBlocks[height]; exist {
					if blkData, exist := heightBlocks[blkCid]; exist {
						blkData.nodes[node] = struct{}{}
					} else {
						nodes := make(map[*DataSourceNode]struct{})
						nodes[node] = struct{}{}

						blkData := BlockData{
							bh:     blk,
							nodes:  nodes,
							status: BlockMessageStatusWaitSync,
						}
						heightBlocks[blkCid] = &blkData
					}
				} else {
					nodes := make(map[*DataSourceNode]struct{})
					nodes[node] = struct{}{}
					blkData := BlockData{
						bh:     blk,
						nodes:  nodes,
						status: BlockMessageStatusWaitSync,
					}
					heightBlocks := make(map[cid.Cid]*BlockData)
					heightBlocks[blkCid] = &blkData

					b.allHeightBlocks[height] = heightBlocks
				}
				b.mtx.Unlock()
				// notify to sync
			}
		}
	}()
}

func (b *BlockMessageManager) getPendingBlock() (*types.BlockHeader, *DataSourceNode, error) {
	b.mtx.Lock()
	defer func() {
		b.mtx.Unlock()
	}()

	var heights []abi.ChainEpoch
	for h := range b.allHeightBlocks {
		heights = append(heights, h)
	}

	sort.Slice(heights, func(i, j int) bool {
		return heights[i] < heights[j]
	})

	for _, h := range heights {
		heightBlocks := b.allHeightBlocks[h]
		for _, blockData := range heightBlocks {
			if blockData.status == BlockMessageStatusWaitSync {
				blockData.status = BlockMessageStatusInSyncing

				for node := range blockData.nodes {
					return blockData.bh, node, nil
				}
			}
		}
	}

	return nil, nil, xerrors.New("get pending block failed")
}

func (b *BlockMessageManager) getBlockMessages(blk *types.BlockHeader) *api.BlockMessages {
	height := blk.Height
	blockCid := blk.Cid()

	b.mtx.RLock()
	defer func() {
		b.mtx.RUnlock()
	}()

	if heightBlocks, exist := b.allHeightBlocks[height]; exist {
		if blockData, exist := heightBlocks[blockCid]; exist {
			if blockData.status == BlockMessageStatusComplete {
				return blockData.blockMessages
			}
		}
	}

	return nil
}

func (b *BlockMessageManager) setBlockMessages(blk *types.BlockHeader, blockMessages *api.BlockMessages, err error) {

	height := blk.Height
	blockCid := blk.Cid()

	b.mtx.Lock()
	defer func() {
		b.mtx.Unlock()
	}()

	if heightBlocks, exist := b.allHeightBlocks[height]; exist {
		if blockData, exist := heightBlocks[blockCid]; exist {

			if err != nil {
				if blockData.status == BlockMessageStatusInSyncing {
					blockData.status = BlockMessageStatusWaitSync
				}
			} else {
				blockData.status = BlockMessageStatusComplete
				blockData.blockMessages = blockMessages
			}
		}
	}
}

func (b *BlockMessageManager) setIrreversibleHeight(height abi.ChainEpoch) {
	b.mtx.Lock()
	defer func() {
		b.mtx.Unlock()
	}()

	for h := range b.allHeightBlocks {
		if h <= height {
			delete(b.allHeightBlocks, height)
		}
	}
}
