package model

import "github.com/shopspring/decimal"

type MessageInvokeResult struct {
	ID        uint64 `gorm:"primaryKey;autoIncrement:true"`
	Height    int64  `gorm:"index"`
	TipSetID  uint64 `gorm:"column:tipset_id"`        // ref TipSet's id
	MessageID uint64 `gorm:"index;column:message_id"` // ref Message's id

	MsgType uint8
	// from Invoke Result
	// gas cost
	GasUsed            decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	BaseFeeBurn        decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	OverEstimationBurn decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	MinerPenalty       decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	MinerTip           decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	Refund             decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	TotalCost          decimal.Decimal `gorm:"type:DECIMAL(38,0)"`

	GasRefund decimal.Decimal `gorm:"type:DECIMAL(38,0)"`

	// other result
	Error    string `gorm:"type:text"`
	Duration int64

	// from Receipt
	ExitCode int64
	Return   []byte
}

func (MessageInvokeResult) TableName() string {
	return "message_invoke_result"
}
