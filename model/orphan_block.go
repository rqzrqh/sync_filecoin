package model

type OrphanBlock struct {
	ID     uint64 `gorm:"primaryKey;autoIncrement:true"`
	Height int64  `gorm:"index"`
	Cid    string `gorm:"uniqueIndex;type:varchar(255)"`
	Miner  string `gorm:"index;type:varchar(255)"`
}
