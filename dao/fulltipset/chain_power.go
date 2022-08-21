package fulltipset

import (
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/rqzrqh/sync_filecoin/common"
	"github.com/rqzrqh/sync_filecoin/model"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

func WriteChainPower(tx *gorm.DB, tsID uint64, ts *types.TipSet, chainPower *common.ChainPower) error {

	height := int64(ts.Height())

	if chainPower != nil {
		modelChainPower := model.ChainPower{
			TipSetID:                           tsID,
			Height:                             height,
			TotalRawBytes:                      decimal.NewFromBigInt(chainPower.TotalRawBytes.Int, 0),
			TotalRawBytesCommitted:             decimal.NewFromBigInt(chainPower.TotalRawBytesCommitted.Int, 0),
			TotalQualityAdjustedBytes:          decimal.NewFromBigInt(chainPower.TotalQualityAdjustedBytes.Int, 0),
			TotalQualityAdjustedBytesCommitted: decimal.NewFromBigInt(chainPower.TotalQualityAdjustedBytesCommitted.Int, 0),
			TotalPledgeCollateral:              decimal.NewFromBigInt(chainPower.TotalPledgeCollateral.Int, 0),
			QaSmoothedPositionEstimate:         chainPower.QaPowerSmoothed.PositionEstimate.Int.String(),
			QaSmoothedVelocityEstimate:         chainPower.QaPowerSmoothed.VelocityEstimate.Int.String(),
			MinerCount:                         chainPower.MinerCount,
			MinerCountAboveMinimumPower:        chainPower.MinerCountAboveMinimumPower,
		}

		if err := tx.Create(&modelChainPower).Error; err != nil {
			log.Errorw("WriteChainPower", "err", err, "height", height)
			return err
		}
	}

	return nil
}
