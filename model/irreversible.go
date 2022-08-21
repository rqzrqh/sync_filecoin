package model

type Irreversible struct {
	ID       uint64 `gorm:"primaryKey;autoIncrement:true"`
	TipSetID uint64 `gorm:"column:tipset_id"` // ref TipSet's id
	Height   int64
}

func (Irreversible) TableName() string {
	return "irreversible"
}
