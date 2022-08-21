package model

type EmptyHeight struct {
	ID        uint64 `gorm:"primaryKey;autoIncrement:true"`
	Height    int64  `gorm:"uniqueIndex"`
	Timestamp int64
}

func (EmptyHeight) TableName() string {
	return "empty_height_list"
}
