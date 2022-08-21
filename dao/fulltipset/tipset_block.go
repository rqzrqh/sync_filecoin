package fulltipset

import (
	"github.com/ipfs/go-cid"
	"github.com/shopspring/decimal"
	"golang.org/x/xerrors"
	"gorm.io/gorm"

	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/rqzrqh/sync_filecoin/model"
)

func WriteTipSetBlocks(db *gorm.DB, tsID uint64, ts *types.TipSet, allBlockMessages []*api.BlockMessages, unProcessedMsgCidSet map[cid.Cid]struct{}, blockRewards []big.Int, penalty big.Int, allBlockMessageSet map[cid.Cid]int, blockHeaderIDs map[string]uint64) error {

	height := int64(ts.Height())

	var modelTipSetBlockList []*model.TipSetBlock

	for idx, blk := range ts.Blocks() {

		uniqueMessageCount := 0

		for _, msgCid := range allBlockMessages[idx].Cids {
			count, exist := allBlockMessageSet[msgCid]
			if !exist {
				log.Errorf("WriteTipSetBlocks msgCid not found %v", msgCid)
				return xerrors.New("WriteTipSetBlocks msgCid not found")
			}

			if count == 1 {
				uniqueMessageCount++
			}
		}

		var decimalPenalty decimal.Decimal
		if penalty.Int == nil {
			decimalPenalty = decimal.Zero
		} else {
			decimalPenalty = decimal.NewFromBigInt(penalty.Int, 0)
		}

		unProcessedMsgCount := 0
		for _, msgCid := range allBlockMessages[idx].Cids {
			if _, exist := unProcessedMsgCidSet[msgCid]; exist {
				unProcessedMsgCount++
			}
		}

		blkID, exist := blockHeaderIDs[blk.Cid().String()]
		if !exist {
			log.Errorf("WriteTipSetBlocks find block id failed, cid=%v", blk.Cid())
			return xerrors.New("WriteTipSetBlocks failed")
		}

		modelTipSetBlock := model.TipSetBlock{
			Height:                     height,
			TipSetID:                   tsID,
			BlockID:                    blkID,
			IndexInTipSet:              uint16(idx),
			Reward:                     decimal.NewFromBigInt(blockRewards[idx].Int, 0),
			Penalty:                    decimalPenalty,
			UniqueMessageCountInTipSet: uniqueMessageCount,
			UnProcessedMessageCount:    unProcessedMsgCount,
		}

		modelTipSetBlockList = append(modelTipSetBlockList, &modelTipSetBlock)
	}

	if err := db.CreateInBatches(&modelTipSetBlockList, 10).Error; err != nil {
		log.Errorw("WriteTipSetBlocks", "err", err, "height", height)
		return err
	}

	return nil
}
