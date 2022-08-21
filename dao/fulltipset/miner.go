package fulltipset

import (
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/rqzrqh/sync_filecoin/common"
	"github.com/rqzrqh/sync_filecoin/model"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

func WriteMinerPowers(db *gorm.DB, tsID uint64, ts *types.TipSet, fts *common.FullTipSet) error {

	height := int64(ts.Height())

	var minerPowers []model.MinerPower
	for _, power := range fts.MinerPowers {

		var decimalRawPower decimal.Decimal
		if power.RawPower.Int == nil {
			decimalRawPower = decimal.Zero
		} else {
			decimalRawPower = decimal.NewFromBigInt(power.RawPower.Int, 0)
		}

		var decimalQalPower decimal.Decimal
		if power.QalPower.Int == nil {
			decimalQalPower = decimal.Zero
		} else {
			decimalQalPower = decimal.NewFromBigInt(power.QalPower.Int, 0)
		}

		minerPower := model.MinerPower{
			TipSetID: tsID,
			Miner:    power.Act.Addr.String(),
			Height:   height,
			RawPower: decimalRawPower,
			QalPower: decimalQalPower,
		}
		minerPowers = append(minerPowers, minerPower)
	}
	if len(minerPowers) > 0 {

		if err := db.CreateInBatches(&minerPowers, 500).Error; err != nil {
			log.Errorw("WriteMinerPowers", "err", err, "height", height)
			return err
		}
	}

	return nil
}

func WriteMiners(db *gorm.DB, tsID uint64, ts *types.TipSet, fts *common.FullTipSet) error {

	height := int64(ts.Height())

	var miners []model.Miner
	for _, m := range fts.Miners {
		var peerID string
		if m.PeerID != nil {
			peerID = m.PeerID.String()
		}

		miner := model.Miner{
			TipSetID:   tsID,
			Height:     height,
			MinerID:    m.MinerID.String(),
			OwnerAddr:  m.OwnerAddr.String(),
			WorkerAddr: m.WorkerAddr.String(),
			PeerID:     peerID,
			SectorSize: uint64(m.SectorSize), // use short string?
		}
		miners = append(miners, miner)
	}
	if len(miners) > 0 {

		if err := db.CreateInBatches(&miners, 500).Error; err != nil {
			log.Errorw("WriteMiners", "err", err, "height", height)
			return err
		}
	}

	return nil
}
