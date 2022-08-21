package model

import "github.com/shopspring/decimal"

type CommonActor struct {
	ID          uint64 `gorm:"primaryKey;autoIncrement:true"`
	TipSetID    uint64 `gorm:"column:tipset_id"` // ref TipSet's id
	Height      int64  `gorm:"index"`
	ActorName   string `gorm:"type:text"`
	ActorFamily string `gorm:"type:text"`
	Address     string `gorm:"index;type:varchar(255)"`
	Head        string // may need to repair data, it's easy to read data by stateroot
	Nonce       uint64
	Balance     decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
}

func (CommonActor) TableName() string {
	return "common_actor_history"
}
