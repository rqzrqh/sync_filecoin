package model

import "github.com/shopspring/decimal"

type ChainReward struct {
	ID                           uint64          `gorm:"primaryKey;autoIncrement:true"`
	TipSetID                     uint64          `gorm:"column:tipset_id"` // ref TipSet's id
	Height                       int64           `gorm:"index"`
	CumSumBaselinePower          decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	CumSumRealizedPower          decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	EffectiveNetworkTime         int64
	EffectiveBaselinePower       decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	NewBaselinePower             decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	NewBaseReward                decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	NewSmoothingPositionEstimate string          `gorm:"type:text"`
	NewSmoothingVelocityEstimate string          `gorm:"type:text"`
	TotalMinedReward             decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
}

func (ChainReward) TableName() string {
	return "chain_reward_history"
}
