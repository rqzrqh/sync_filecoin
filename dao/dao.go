package dao

import (
	"context"
	"gorm.io/gorm"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("dao")

type Dao struct {
	ctx context.Context
	db  *gorm.DB
}

func NewDao(ctx context.Context, db *gorm.DB) *Dao {
	return &Dao{
		ctx: ctx,
		db:  db,
	}
}
