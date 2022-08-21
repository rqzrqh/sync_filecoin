package model

import "github.com/shopspring/decimal"

type SystemMessage struct {
	ID       uint64 `gorm:"primaryKey;autoIncrement:true"`
	TipSetID uint64 `gorm:"column:tipset_id"` // ref TipSet's id
	Height   int64  `gorm:"index"`

	Size    int    `gorm:"column:size"`
	Cid     string `gorm:"index;type:varchar(255)"`
	Version uint64
	Nonce   uint64
	From    string          `gorm:"index;type:varchar(255)"`
	To      string          `gorm:"index;type:varchar(255)"`
	Value   decimal.Decimal `gorm:"type:DECIMAL(38,0)"`

	GasLimit   int64
	GasFeeCap  decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	GasPremium decimal.Decimal `gorm:"type:DECIMAL(38,0)"`

	Method uint64
	Params []byte
}

func (SystemMessage) TableName() string {
	return "system_messages"
}
