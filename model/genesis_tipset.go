package model

type GenesisTipSet struct {
	ID  uint64 `gorm:"primaryKey;autoIncrement:true"`
	Raw []byte
}

func (GenesisTipSet) TableName() string {
	return "genesis_tipset"
}
