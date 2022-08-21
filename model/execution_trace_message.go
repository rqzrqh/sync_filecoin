package model

import "github.com/shopspring/decimal"

type ExecutionTraceMessage struct {
	ID       uint64 `gorm:"primaryKey;autoIncrement:true"`
	TipSetID uint64 `gorm:"column:tipset_id"` // ref TipSet's id
	Height   int64  `gorm:"index"`
	// the user or system message which belong to
	MessageID uint64 `gorm:"index;column:message_id"` // ref Messages's id

	MsgType uint8

	// when common.ExecutionTrace breaks down into model.ExecutionTrace rows
	// these elements can describe the tree and rebuild it.
	Level       int
	Index       int
	ParentIndex int

	// message
	Size    int    `gorm:"column:size"`
	Cid     string `gorm:"type:varchar(255)"`
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

	// from ExecutionTrace
	Error    string `gorm:"type:text"`
	Duration int64
	//GasCharge *types.GasTrace // level 0 is nil, level 0 is describe in invoke_result

	ExitCode int64
	Return   []byte
	GasUsed  int64
}

func (ExecutionTraceMessage) TableName() string {
	return "execution_trace_messages"
}
