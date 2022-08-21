package porter

import (
	"context"

	"github.com/filecoin-project/lotus/api"
	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("porter")

type Porter struct {
	ctx         context.Context
	syncManager *SyncManager
}

func NewPorter(ctx context.Context, db *gorm.DB, rds *redis.Client, node []api.FullNode) *Porter {

	syncManager := NewSyncManager(ctx, db, rds, node)

	return &Porter{
		ctx:         ctx,
		syncManager: syncManager,
	}
}

func (p *Porter) Start() {
	p.syncManager.Start()
}

func (p *Porter) Stop() {
	p.syncManager.Stop()
}
