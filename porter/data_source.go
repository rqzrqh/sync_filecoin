package porter

import (
	"context"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
)

type DataSource struct {
	ctx                 context.Context
	nodes               []*DataSourceNode
	nodeStateNotify     []chan UpdateNodeCmd
	incomingBlockNotify chan IncomingBlock
}

func newDataSource(
	ctx context.Context,
	dbGenesisTs *types.TipSet,
	dbChainIrreversible *types.TipSet,
	dbChainHead *types.TipSet,
	headChainLimit int,
	nodes []api.FullNode,
	nodeStateNotify []chan UpdateNodeCmd,
	incomingBlockNotify chan IncomingBlock,
) *DataSource {
	dsNodes := make([]*DataSourceNode, 0, len(nodes))

	for _, node := range nodes {
		dsNodes = append(dsNodes, newDataSourceNode(ctx, dbGenesisTs, dbChainIrreversible, dbChainHead, headChainLimit, node))
	}

	return &DataSource{
		ctx:                 ctx,
		nodes:               dsNodes,
		nodeStateNotify:     nodeStateNotify,
		incomingBlockNotify: incomingBlockNotify,
	}
}

func (ds *DataSource) Start() {
	for i, node := range ds.nodes {
		go node.start(ds.nodeStateNotify[i], ds.incomingBlockNotify)
	}
}

func (ds *DataSource) GetNodes() []*DataSourceNode {
	return ds.nodes
}

func (ds *DataSource) notifyChainState(irreversible *types.TipSet, chainHead *types.TipSet) {
	for _, node := range ds.nodes {
		node.align(irreversible, chainHead)
	}
}
