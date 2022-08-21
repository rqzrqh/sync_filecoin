package local_chain

import (
	"context"

	"github.com/filecoin-project/lotus/chain/types"
	"github.com/rqzrqh/sync_filecoin/common"
	"github.com/rqzrqh/sync_filecoin/dao"
	"gorm.io/gorm"
)

type DatabaseChain struct {
	ctx context.Context
	db  *gorm.DB
}

func NewDatabaseChain(ctx context.Context, db *gorm.DB) *DatabaseChain {

	return &DatabaseChain{
		ctx: ctx,
		db:  db,
	}
}

func (dbc *DatabaseChain) Grow(fts *common.FullTipSet, headChangeInfo *common.HeadChangeInfo, writePrevTsActorState bool) error {
	return dao.Grow(dbc.ctx, dbc.db, fts, headChangeInfo, writePrevTsActorState)
}

func (dbc *DatabaseChain) Prune(ts *types.TipSet, sameHeightHeadChainTs *types.TipSet, deletePrevTsActorState bool) error {
	return dao.Prune(dbc.ctx, dbc.db, ts, sameHeightHeadChainTs, deletePrevTsActorState)
}

func (dbc *DatabaseChain) Pin(ts *types.TipSet) error {
	return dao.Pin(dbc.ctx, dbc.db, ts.Key(), int64(ts.Height()))
}
