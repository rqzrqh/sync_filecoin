package model

type ChainConfig struct {
	ID             uint64 `gorm:"primaryKey;autoIncrement:true"`
	HeadChainLimit int
}

func (ChainConfig) TableName() string {
	return "chain_config"
}
