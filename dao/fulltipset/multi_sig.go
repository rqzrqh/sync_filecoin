package fulltipset

import (
	"encoding/json"

	"github.com/filecoin-project/lotus/chain/types"
	"github.com/rqzrqh/sync_filecoin/common"
	"github.com/rqzrqh/sync_filecoin/model"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

func WriteMultiSig(tx *gorm.DB, tsID uint64, ts *types.TipSet, multiSigTxs []*common.MultisigTransaction) error {

	height := int64(ts.Height())

	var modelMultiSigTxs []*model.MultiSig
	for _, tx := range multiSigTxs {
		strApproved, _ := json.Marshal(tx.Approved)

		modelMultiSigTx := model.MultiSig{
			TipSetID: tsID,
			Height:   height,

			MultisigID:    tx.MultisigID.String(),
			TransactionID: tx.TransactionID,
			To:            tx.To.String(),
			Value:         decimal.NewFromBigInt(tx.Value.Int, 0),
			Method:        tx.Method,
			Params:        tx.Params,
			Approved:      string(strApproved),
		}
		modelMultiSigTxs = append(modelMultiSigTxs, &modelMultiSigTx)
	}
	if len(modelMultiSigTxs) > 0 {
		if err := tx.Create(&modelMultiSigTxs).Error; err != nil {
			log.Errorw("WriteMultiSig", "err", err, "height", height)
			return err
		}
	}

	return nil
}
