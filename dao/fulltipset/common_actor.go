package fulltipset

import (
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/rqzrqh/sync_filecoin/common"
	"github.com/rqzrqh/sync_filecoin/model"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

func WriteCommonActors(db *gorm.DB, tsID uint64, ts *types.TipSet, changedActors []common.ActorInfo) error {

	height := int64(ts.Height())

	var modelActors []*model.CommonActor
	for _, v := range changedActors {
		modelActor := model.CommonActor{
			TipSetID:    tsID,
			Height:      height,
			ActorName:   v.ActorName,
			ActorFamily: v.ActorFamily,
			Address:     v.Addr.String(),
			Head:        v.Act.Head.String(),
			Nonce:       v.Act.Nonce,
			Balance:     decimal.NewFromBigInt(v.Act.Balance.Int, 0),
		}
		modelActors = append(modelActors, &modelActor)
	}
	if len(modelActors) > 0 {
		if err := db.CreateInBatches(&modelActors, 200).Error; err != nil {
			log.Errorw("WriteCommonActors", "err", err, "height", height)
			return err
		}
	}

	return nil
}
