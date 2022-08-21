package model

import "github.com/shopspring/decimal"

type MarketDealProposal struct {
	ID                   uint64 `gorm:"primaryKey;autoIncrement:true"`
	TipSetID             uint64 `gorm:"column:tipset_id"` // ref TipSet's id
	Height               int64  `gorm:"index"`
	DealID               uint64 `gorm:"index"`
	PieceCID             string
	PieceSize            uint64
	PieceSizeUnpadded    uint64
	VerifiedDeal         bool
	Client               string
	Provider             string
	StartEpoch           int64
	EndEpoch             int64
	SlashedEpoch         int64
	StoragePricePerEpoch decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	ProviderCollateral   decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	ClientCollateral     decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
}

func (MarketDealProposal) TableName() string {
	return "market_deal_proposal"
}

type MarketDealState struct {
	ID               uint64 `gorm:"primaryKey;autoIncrement:true"`
	TipSetID         uint64 `gorm:"column:tipset_id"` // ref TipSet's id
	Height           int64  `gorm:"index"`
	DealID           uint64 `gorm:"index"` // no ref!!!
	SectorStartEpoch int64
	LastUpdatedEpoch int64
	SlashEpoch       int64
}

func (MarketDealState) TableName() string {
	return "market_deal_state"
}
