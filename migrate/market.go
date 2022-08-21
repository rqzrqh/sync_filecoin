package migrate

import (
	"context"
	"strconv"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/rqzrqh/sync_filecoin/common"
	"golang.org/x/xerrors"
)

func HandleStorageMarketChanges(ctx context.Context, node api.FullNode, ts *types.TipSet, actList []common.ActorInfo) ([]*common.MarketDealProposal, []*common.MarketDealState, error) {

	tsKey := ts.Key()
	dealProposals := make([]*common.MarketDealProposal, 0)
	dealStates := make([]*common.MarketDealState, 0)

	if len(actList) > 1 {
		return nil, nil, xerrors.New("storage market actor count error")
	}

	if len(actList) == 0 {
		return dealProposals, dealStates, nil
	}

	// TODO insert in sorted order (lowest height -> highest height) since dealid is pk of table.

	deals, err := node.StateMarketDeals(ctx, tsKey)
	if err != nil {
		return nil, nil, err
	}

	for dealID, ds := range deals {

		id, err := strconv.ParseUint(dealID, 10, 64)
		if err != nil {
			return nil, nil, err
		}

		dealProposal := common.MarketDealProposal{
			DealID:            abi.DealID(id),
			PieceCID:          ds.Proposal.PieceCID,
			PieceSize:         ds.Proposal.PieceSize,
			PieceSizeUnpadded: ds.Proposal.PieceSize.Unpadded(),
			VerifiedDeal:      ds.Proposal.VerifiedDeal,
			Client:            ds.Proposal.Client,
			Provider:          ds.Proposal.Provider,
			StartEpoch:        ds.Proposal.StartEpoch,
			EndEpoch:          ds.Proposal.EndEpoch,
			//SlashedEpoch
			StoragePricePerEpoch: ds.Proposal.StoragePricePerEpoch,
			ProviderCollateral:   ds.Proposal.ProviderCollateral,
			ClientCollateral:     ds.Proposal.ClientCollateral,
		}

		dealState := common.MarketDealState{
			DealID:           abi.DealID(id),
			SectorStartEpoch: ds.State.SectorStartEpoch,
			LastUpdatedEpoch: ds.State.LastUpdatedEpoch,
			SlashEpoch:       ds.State.SlashEpoch,
		}

		dealProposals = append(dealProposals, &dealProposal)
		dealStates = append(dealStates, &dealState)
	}

	return dealProposals, dealStates, nil
}
