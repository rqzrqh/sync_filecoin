package fulltipset

import (
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/rqzrqh/sync_filecoin/common"
	"github.com/rqzrqh/sync_filecoin/model"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

func WriteChainReward(tx *gorm.DB, tsID uint64, ts *types.TipSet, chainReward *common.ChainReward) error {

	height := int64(ts.Height())

	if chainReward != nil {
		modelChainReward := model.ChainReward{
			TipSetID:                     tsID,
			Height:                       height,
			CumSumBaselinePower:          decimal.NewFromBigInt(chainReward.CumSumBaselinePower.Int, 0),
			CumSumRealizedPower:          decimal.NewFromBigInt(chainReward.CumSumRealizedPower.Int, 0),
			EffectiveNetworkTime:         int64(chainReward.EffectiveNetworkTime),
			EffectiveBaselinePower:       decimal.NewFromBigInt(chainReward.EffectiveBaselinePower.Int, 0),
			NewBaselinePower:             decimal.NewFromBigInt(chainReward.NewBaselinePower.Int, 0),
			NewBaseReward:                decimal.NewFromBigInt(chainReward.NewBaseReward.Int, 0),
			NewSmoothingPositionEstimate: chainReward.NewSmoothingEstimate.PositionEstimate.Int.String(),
			NewSmoothingVelocityEstimate: chainReward.NewSmoothingEstimate.VelocityEstimate.Int.String(),
			TotalMinedReward:             decimal.NewFromBigInt(chainReward.TotalMinedReward.Int, 0),
		}

		if err := tx.Create(&modelChainReward).Error; err != nil {
			log.Errorw("WriteChainReward", "err", err, "height", height)
			return err
		}
	}

	return nil
}
