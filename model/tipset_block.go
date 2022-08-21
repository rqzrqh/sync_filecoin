package model

import "github.com/shopspring/decimal"

type TipSetBlock struct {
	ID                         uint64          `gorm:"primaryKey;autoIncrement:true"`
	Height                     int64           `gorm:"index"`
	TipSetID                   uint64          `gorm:"column:tipset_id"`       // ref TipSet's id
	BlockID                    uint64          `gorm:"index;column:block_id"`  // ref Block's id
	IndexInTipSet              uint16          `gorm:"column:index_in_tipset"` // array index of BlockHeader in the tipset which belongs to, just like tipset.Blocks[IndexInTipSet]
	Reward                     decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	Penalty                    decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	UniqueMessageCountInTipSet int             `gorm:"column:unique_message_count_in_tipset"`
	UnProcessedMessageCount    int             `gorm:"column:unprocessed_msg_count"`
}

func (TipSetBlock) TableName() string {
	return "tipset_block"
}
