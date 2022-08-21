package model

import (
	"github.com/shopspring/decimal"
)

type MultiSig struct {
	ID       uint64 `gorm:"primaryKey;autoIncrement:true"`
	TipSetID uint64 `gorm:"column:tipset_id"` // ref TipSet's id
	Height   int64  `gorm:"index"`

	MultisigID    string `gorm:"type:text"`
	TransactionID int64

	// Transaction State
	To       string          `gorm:"type:varchar(255)"`
	Value    decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	Method   uint64
	Params   []byte
	Approved string `gorm:"type:longtext"`
}
