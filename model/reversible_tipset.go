package model

import "github.com/shopspring/decimal"

// for inner use only!!
type ReversibleTipSet struct {
	ID                 uint64 `gorm:"primaryKey;autoIncrement:true"`
	TipSetID           uint64 `gorm:"type:bigint;column:tipset_id"` // ref TipSet's id
	Height             int64  `gorm:"type:bigint;column:height"`
	ParentHeight       int64  `gorm:"type:bigint;column:parent_height"`
	Status             uint8
	Tsk                string          `gorm:"type:longtext"`                       // it can not transform to TipSetKey directly. For read and find only.
	ParentTsk          string          `gorm:"type:longtext;column:parent_tsk"`     // it can not transform to TipSetKey directly. For read and find only.
	ParentTipSetID     uint64          `gorm:"type:bigint;column:parent_tipset_id"` // ref TipSet's id, but must not use forigen key constraint, maybe parent not exist if height=0
	ParentWeight       decimal.Decimal `gorm:"type:DECIMAL(38,0);column:parent_weight"`
	ChangedAddressList string          `gorm:"type:longtext;column:changed_addr_list"`
}

func (ReversibleTipSet) TableName() string {
	return "reversible_tipset"
}

const (
	TipSetStatusWriting  uint8 = 0
	TipSetStatusDeleting uint8 = 1
	TipSetStatusValid    uint8 = 2
)
