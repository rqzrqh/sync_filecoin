package model

import "github.com/shopspring/decimal"

type Miner struct {
	ID       uint64 `gorm:"primaryKey;autoIncrement:true"`
	TipSetID uint64 `gorm:"column:tipset_id"` // ref TipSet's id
	Height   int64  `gorm:"index"`

	MinerID    string `gorm:"index;type:varchar(255);column:miner_id"`
	OwnerAddr  string `gorm:"type:text"`
	WorkerAddr string `gorm:"type:text"`
	PeerID     string `gorm:"type:text;column:peer_id"`
	SectorSize uint64
}

func (Miner) TableName() string {
	return "miner_history"
}

type MinerPower struct {
	ID       uint64          `gorm:"primaryKey;autoIncrement:true"`
	TipSetID uint64          `gorm:"column:tipset_id"` // ref TipSet's id
	Miner    string          `gorm:"index;type:varchar(255)"`
	Height   int64           `gorm:"index"`
	RawPower decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	QalPower decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
}

func (MinerPower) TableName() string {
	return "miner_power_history"
}
