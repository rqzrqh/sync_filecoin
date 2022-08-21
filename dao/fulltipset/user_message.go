package fulltipset

import (
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/rqzrqh/sync_filecoin/common"
	"github.com/rqzrqh/sync_filecoin/model"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func BuildMsg(msg *types.Message) *model.UserMessage {

	message := model.UserMessage{

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

	return &message
}

func BuildSignedMsg(msg *types.SignedMessage) *model.UserMessage {
	var signature []byte
	var err error

	signature, err = msg.Signature.MarshalBinary()
	if err != nil {
		return nil
	}

	message := model.UserMessage{

		Size:       msg.ChainLength(),
		Cid:        msg.Cid().String(),
		Version:    msg.Message.Version,
		Nonce:      msg.Message.Nonce,
		From:       msg.Message.From.String(),
		To:         msg.Message.To.String(),
		Value:      decimal.NewFromBigInt(msg.Message.Value.Int, 0),
		GasLimit:   msg.Message.GasLimit,
		GasFeeCap:  decimal.NewFromBigInt(msg.Message.GasFeeCap.Int, 0),
		GasPremium: decimal.NewFromBigInt(msg.Message.GasPremium.Int, 0),
		Method:     uint64(msg.Message.Method),
		Params:     msg.Message.Params,
		Signature:  signature,
	}

	return &message
}

func WriteUserMessages(db *gorm.DB, tsID uint64, ts *types.TipSet, userBlsMessages []*common.UserBlsMessage, userSecpMessages []*common.UserSecpMessage) (map[string]uint64, error) {

	height := int64(ts.Height())

	var msgCidList []string

	// no need to consider order
	var modelMessages []*model.UserMessage
	for _, v := range userBlsMessages {
		modelMessage := BuildMsg(v.Msg)
		modelMessages = append(modelMessages, modelMessage)
		msgCidList = append(msgCidList, v.Msg.Cid().String())
	}

	for _, v := range userSecpMessages {
		modelMessage := BuildSignedMsg(v.Msg)
		modelMessages = append(modelMessages, modelMessage)
		msgCidList = append(msgCidList, v.Msg.Cid().String())
	}

	if len(modelMessages) > 0 {

		if err := db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "cid"}},
			DoNothing: true}).CreateInBatches(&modelMessages, 10).Error; err != nil {
			log.Errorw("WriteUserMessages", "err", err, "height", height)
			return nil, err
		}
	}

	// must query
	var modelMessages2 []*model.UserMessage
	if err := db.Select("id, cid").Where("cid in (?)", msgCidList).Find(&modelMessages2).Error; err != nil {
		log.Errorw("WriteUserMessages", "err", err, "height", height)
		return nil, err
	}

	msgCidToMessageID := make(map[string]uint64)
	for _, m := range modelMessages2 {
		msgCidToMessageID[m.Cid] = m.ID
	}

	return msgCidToMessageID, nil
}
