package porter

import (
	"context"
	"time"

	"github.com/rqzrqh/sync_filecoin/dao"
	"github.com/rqzrqh/sync_filecoin/local_chain"
	"gorm.io/gorm"
)

type BlockMessageSyncer struct {
	ctx               context.Context
	db                *gorm.DB
	bmm               *BlockMessageManager
	localChainManager *local_chain.LocalChainManager
}

func newBlockMessageSyncer(ctx context.Context, db *gorm.DB, bmm *BlockMessageManager, localChainManager *local_chain.LocalChainManager) *BlockMessageSyncer {
	return &BlockMessageSyncer{
		ctx:               ctx,
		db:                db,
		bmm:               bmm,
		localChainManager: localChainManager,
	}
}

func (s *BlockMessageSyncer) start() {

	go func() {
		timer := time.NewTicker(50 * time.Millisecond)
		defer timer.Stop()

		for {
			select {
			case <-timer.C:

				bh, node, err := s.bmm.getPendingBlock()
				if err != nil {
					continue
				}

				blockMessages, err := node.api.ChainGetBlockMessages(s.ctx, bh.Cid())

				if err != nil {
					s.bmm.setBlockMessages(bh, blockMessages, err)
					log.Errorw("ChainGetBlockMessages", err)
					return
				}

				s.bmm.setBlockMessages(bh, blockMessages, nil)

				userMsgCidToMessageID, err := dao.WriteBlockMessages(s.db, bh, blockMessages.BlsMessages, blockMessages.SecpkMessages)
				if err != nil {
					log.Errorw("WriteUserMessages", err)
					continue
				}

				blockID, err := dao.WriteBlock(s.db, bh, blockMessages.Cids)
				if err != nil {
					log.Errorw("WriteBlock", err)
					continue
				}

				if err := dao.WriteBlockMessageRelations(s.ctx, s.db, bh, blockMessages, blockID, userMsgCidToMessageID); err != nil {
					log.Errorw("WriteBlockMessageRelations", err)
					continue
				}
			}
		}
	}()
}
