package fulltipset

import (
	"encoding/json"

	"github.com/filecoin-project/lotus/chain/types"
	"github.com/rqzrqh/sync_filecoin/common"
	"github.com/rqzrqh/sync_filecoin/model"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

func WritePaymentChannels(tx *gorm.DB, tsID uint64, ts *types.TipSet, paychannels []*common.PaymentChannel) error {

	height := int64(ts.Height())

	var paymentChannels []*model.PaymentChannel
	for _, payment := range paychannels {

		var toSend decimal.Decimal
		if payment.ToSend.Int == nil {
			toSend = decimal.Zero
		} else {
			toSend = decimal.NewFromBigInt(payment.ToSend.Int, 0)
		}

		strState, _ := json.Marshal(payment.LaneStates)

		paymentChannel := model.PaymentChannel{

			TipSetID:   tsID,
			Height:     height,
			From:       payment.From.String(),
			To:         payment.To.String(),
			SettlingAt: int64(payment.SettlingAt),
			ToSend:     toSend,
			LaneCount:  payment.LaneCount,
			LaneStates: string(strState),
		}
		paymentChannels = append(paymentChannels, &paymentChannel)
	}
	if len(paymentChannels) > 0 {

		if err := tx.Create(&paymentChannels).Error; err != nil {
			log.Errorw("WritePaymentChannels", "err", err, "height", height)
			return err
		}
	}

	return nil
}
