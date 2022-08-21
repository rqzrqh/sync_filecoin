package model

import "github.com/shopspring/decimal"

type TipSet struct {
	ID             uint64 `gorm:"primaryKey;autoIncrement:true"`
	Height         int64  `gorm:"index;"`
	ParentTipSetID uint64 `gorm:"column:parent_tipset_id"` // ref TipSet's id
	ParentHeight   int64  `gorm:"column:parent_height"`
	Timestamp      int64

	BlockCount              int
	TotalMessageCount       int
	DedupedMessageCount     int
	UnProcessedMessageCount int
	/*
		BaseFeeBurn         string
		OverEstimationBurn  string
		MinerPenalty        string
		MinerTip            string
		Refund              string
		GasRefund           int64
		GasBurned           int64
	*/

	ParentStateRoot string          `gorm:"type:text"`
	ParentWeight    decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	ParentBaseFee   decimal.Decimal `gorm:"type:DECIMAL(38,0)"`

	Reward  decimal.Decimal `gorm:"type:DECIMAL(38,0)"`
	Penalty decimal.Decimal `gorm:"type:DECIMAL(38,0)"`

	IsValid       bool `gorm:"type:bool;default:false;column:is_valid"`
	InHeadChain   bool `gorm:"type:bool;default:false;column:in_head_chain"`
	HasActorState bool `gorm:"type:bool;default:false;column:has_actor_state"` // for client access
}

func (TipSet) TableName() string {
	return "tipset_list"
}
