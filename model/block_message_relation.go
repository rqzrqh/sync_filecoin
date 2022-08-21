package model

type BlockMessageRelation struct {
	ID           uint64 `gorm:"primaryKey;autoIncrement:true"`
	Height       int64  `gorm:"index"`
	BlockID      uint64 `gorm:"index;column:block_id"`   // ref Block's id
	MessageID    uint64 `gorm:"index;column:message_id"` // ref Message's id
	IndexInBlock int
}

func (BlockMessageRelation) TableName() string {
	return "block_message_relation"
}
