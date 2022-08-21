package model

type PidFile struct {
	ID   uint64 `gorm:"primaryKey;autoIncrement:true"`
	Info string
}

func (PidFile) TableName() string {
	return "pid_file"
}
