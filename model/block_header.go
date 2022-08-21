package model

import "github.com/shopspring/decimal"

type BlockHeader struct {
	ID     uint64 `gorm:"primaryKey;autoIncrement:true"`
	Height int64  `gorm:"index"`
	// no parent info, because block may incoming but it's parent not exist in local
	Cid          string `gorm:"uniqueIndex;type:varchar(255)"`
	Miner        string `gorm:"index;type:varchar(255)"`
	Size         int
	Reward       decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	Penalty      decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	WinCount     int64
	MessageCount int
	IsOrphan     bool `gorm:"type:bool;default:false;column:is_orphan"`

	Raw []byte
}

func (BlockHeader) TableName() string {
	return "block_header_list"
}
