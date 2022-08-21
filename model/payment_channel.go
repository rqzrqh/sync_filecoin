package model

import "github.com/shopspring/decimal"

// TODO paychannel cid?
type PaymentChannel struct {
	ID       uint64 `gorm:"primaryKey;autoIncrement:true"`
	TipSetID uint64 `gorm:"column:tipset_id"` // ref TipSet's id
	Height   int64  `gorm:"index"`

	From       string `gorm:"index;type:varchar(255)"`
	To         string `gorm:"index;type:varchar(255)"`
	SettlingAt int64
	ToSend     decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	LaneCount  uint64
	LaneStates string `gorm:"type:longtext"` // map[uint64]common.LaneState
}

func (PaymentChannel) TableName() string {
	return "payment_channel"
}
