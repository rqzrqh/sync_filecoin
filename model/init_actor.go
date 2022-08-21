package model

type InitActor struct {
	ID       uint64 `gorm:"primaryKey;autoIncrement:true"`
	TipSetID uint64 `gorm:"column:tipset_id"` // ref TipSet's id
	Height   int64  `gorm:"index"`
	Address  string `gorm:"index;type:varchar(255)"`
	ActorID  string `gorm:"index;type:varchar(255);column:actor_id"`
	// TODO how to describe id->pubkey
}

func (InitActor) TableName() string {
	return "init_actor"
}
