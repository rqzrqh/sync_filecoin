package model

import "github.com/shopspring/decimal"

type ChainPower struct {
	ID                                 uint64          `gorm:"primaryKey;autoIncrement:true"`
	TipSetID                           uint64          `gorm:"column:tipset_id"` // ref TipSet's id
	Height                             int64           `gorm:"index"`
	TotalRawBytes                      decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	TotalRawBytesCommitted             decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	TotalQualityAdjustedBytes          decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	TotalQualityAdjustedBytesCommitted decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	TotalPledgeCollateral              decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	QaSmoothedPositionEstimate         string          `gorm:"type:text"`
	QaSmoothedVelocityEstimate         string          `gorm:"type:text"`
	MinerCount                         int64
	MinerCountAboveMinimumPower        int64
}

func (ChainPower) TableName() string {
	return "chain_power_history"
}
