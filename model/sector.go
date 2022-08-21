package model

import "github.com/shopspring/decimal"

type SectorInfo struct {
	ID       uint64 `gorm:"primaryKey;autoIncrement:true"`
	TipSetID uint64 `gorm:"column:tipset_id"` // ref TipSet's id
	Height   int64  `gorm:"index"`

	MinerID         string `gorm:"index;type:varchar(255)"`
	SectorID        uint64 `gorm:"index"`
	SealedCid       string `gorm:"index;type:varchar(255)"`
	ActivationEpoch int64
	ExpirationEpoch int64

	DealWeight         decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	VerifiedDealWeight decimal.Decimal `gorm:"type:DECIMAL(38,0)"`

	InitialPledge         decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	ExpectedDayReward     decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	ExpectedStoragePledge decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
}

type SectorPrecommitInfo struct {
	ID       uint64 `gorm:"primaryKey;autoIncrement:true"`
	TipSetID uint64 `gorm:"column:tipset_id"` // ref TipSet's id
	Height   int64  `gorm:"index"`

	MinerID   string `gorm:"index;type:varchar(255)"`
	SectorID  uint64 `gorm:"index"`
	SealedCid string `gorm:"index;type:varchar(255)"`

	SealRandEpoch    int64
	ExpirationEpoch  int64
	PrecommitDeposit decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	PrecommitEpoch   int64

	DealWeight         decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	VerifiedDealWeight decimal.Decimal `gorm:"type:DECIMAL(38,0)"`

	IsReplaceCapacity bool

	ReplaceSectorDeadline  uint64
	ReplaceSectorPartition uint64
	ReplaceSectorNumber    uint64
}

func (SectorPrecommitInfo) TableName() string {
	return "sector_precommit_info"
}

type SectorLifecycleEvent string

type MinerSectorEvent struct {
	ID       uint64 `gorm:"primaryKey;autoIncrement:true"`
	TipSetID uint64 `gorm:"column:tipset_id"` // ref TipSet's id
	Height   int64  `gorm:"index"`

	MinerID  string `gorm:"index;type:varchar(255)"`
	SectorID uint64 `gorm:"index"`
	Event    SectorLifecycleEvent
}

func (MinerSectorEvent) TableName() string {
	return "miner_sector_event"
}

type SectorDealEvent struct {
	ID       uint64 `gorm:"primaryKey;autoIncrement:true"`
	TipSetID uint64 `gorm:"column:tipset_id"` // ref TipSet's id
	Height   int64  `gorm:"index"`

	DealID   uint64 `gorm:"index"` // which is deal table?
	MinerID  string `gorm:"index;type:varchar(255)"`
	SectorID uint64 `gorm:"index"`
}

func (SectorDealEvent) TableName() string {
	return "sector_deal_event"
}
