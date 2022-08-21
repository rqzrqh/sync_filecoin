package fulltipset

import (
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/rqzrqh/sync_filecoin/common"
	"github.com/rqzrqh/sync_filecoin/model"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

func WriteSystemMessages(db *gorm.DB, tsID uint64, ts *types.TipSet, systemMessages []*common.SystemMessage) (map[string]uint64, error) {

	height := int64(ts.Height())

	var messages []*model.SystemMessage
	for _, v := range systemMessages {

		msg := v.Msg

		modelMessage := model.SystemMessage{
			Height:     height,
			TipSetID:   tsID,
			Size:       msg.ChainLength(),
			Cid:        msg.Cid().String(),
			Version:    msg.Version,
			Nonce:      msg.Nonce,
			From:       msg.From.String(),
			To:         msg.To.String(),
			Value:      decimal.NewFromBigInt(msg.Value.Int, 0),
			GasLimit:   msg.GasLimit,
			GasFeeCap:  decimal.NewFromBigInt(msg.GasFeeCap.Int, 0),
			GasPremium: decimal.NewFromBigInt(msg.GasPremium.Int, 0),
			Method:     uint64(msg.Method),
			Params:     msg.Params,
		}

		messages = append(messages, &modelMessage)
	}

	if len(messages) > 0 {
		if err := db.CreateInBatches(&messages, 100).Error; err != nil {
			log.Errorw("WriteSystemMessages", "err", err, "height", height)
			return nil, err
		}
	}

	msgCidToMessageID := make(map[string]uint64)
	for _, m := range messages {
		msgCidToMessageID[m.Cid] = m.ID
	}

	return msgCidToMessageID, nil
}
