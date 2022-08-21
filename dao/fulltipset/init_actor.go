package fulltipset

import (
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/rqzrqh/sync_filecoin/common"
	"github.com/rqzrqh/sync_filecoin/model"
	"gorm.io/gorm"
)

func WriteInitActor(db *gorm.DB, tsID uint64, ts *types.TipSet, initActor *common.ChainInitActor) error {

	height := int64(ts.Height())

	if initActor != nil {

		var modelInitActors []*model.InitActor
		for addr, actorID := range initActor.AddressIDMap {
			initActor := model.InitActor{
				TipSetID: tsID,
				Height:   height,
				Address:  addr.String(),
				ActorID:  actorID.String(),
			}
			modelInitActors = append(modelInitActors, &initActor)
		}

		if len(modelInitActors) > 0 {
			if err := db.CreateInBatches(&modelInitActors, 100).Error; err != nil {
				log.Errorw("WriteInitActor", "err", err, "height", height)
				return err
			}
		}
	}

	return nil
}
