package common

import (
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type HeadChangeInfo struct {
	OldHeadChain []*types.TipSet
	NewHeadChain []*types.TipSet
}

type ActorInfo struct {
	Act         types.Actor
	Addr        address.Address
	ActorName   string
	ActorFamily string
	// unused now
	State string
}

type InvokeResult struct {
	MsgID cid.Cid

	GasUsed            abi.TokenAmount
	BaseFeeBurn        abi.TokenAmount
	OverEstimationBurn abi.TokenAmount
	MinerPenalty       abi.TokenAmount
	MinerTip           abi.TokenAmount
	Refund             abi.TokenAmount
	TotalCost          abi.TokenAmount

	GasRefund abi.TokenAmount // what's this?

	Error    string
	Duration time.Duration

	Receipt *types.MessageReceipt // may be nil?
}

// has been flatten
type ExecutionTrace struct {
	Error     string
	Duration  time.Duration
	GasCharge *types.GasTrace // level 0 is nil, level 0 is describe in invoke_result, unused now

	Receipt *types.MessageReceipt // may be nil?
}

const (
	MsgTypeUser   uint8 = 0 // user message
	MsgTypeSystem uint8 = 1 // internal message, generated by tipset?
)

type ExecutionTraceMessage struct {
	BelongToMsgCid  cid.Cid
	BelongToMsgType uint8
	// when types.ExecutionTrace breaks down into common.ExecutionTrace rows, index and parentIndex can used for rebuild it.
	// index is increasing in a types.ExecutionTrace
	Level       int // for read-only
	Index       int
	ParentIndex int

	Message *types.Message
	Trace   *ExecutionTrace
}

type MarketDealProposal struct {
	DealID               abi.DealID
	PieceCID             cid.Cid
	PieceSize            abi.PaddedPieceSize
	PieceSizeUnpadded    abi.UnpaddedPieceSize
	VerifiedDeal         bool
	Client               address.Address
	Provider             address.Address
	StartEpoch           abi.ChainEpoch
	EndEpoch             abi.ChainEpoch
	SlashedEpoch         abi.ChainEpoch
	StoragePricePerEpoch big.Int
	ProviderCollateral   big.Int
	ClientCollateral     big.Int
}

type MarketDealState struct {
	DealID           abi.DealID
	SectorStartEpoch abi.ChainEpoch
	LastUpdatedEpoch abi.ChainEpoch
	SlashEpoch       abi.ChainEpoch
}

type LaneState struct {
	Nonce    uint64
	Redeemed big.Int
}

type PaymentChannel struct {
	From       address.Address
	To         address.Address
	SettlingAt abi.ChainEpoch
	ToSend     abi.TokenAmount
	LaneCount  uint64
	LaneStates map[uint64]LaneState
}

type ChainInitActor struct {
	AddressIDMap map[address.Address]address.Address
}

type ChainReward struct {
	CumSumBaselinePower big.Int
	CumSumRealizedPower big.Int

	EffectiveNetworkTime   abi.ChainEpoch
	EffectiveBaselinePower big.Int

	// NOTE: These variables are wrong. Talk to @ZX about fixing. These _do
	// not_ represent "new" anything.
	NewBaselinePower     big.Int
	NewBaseReward        big.Int
	NewSmoothingEstimate builtin.FilterEstimate

	TotalMinedReward big.Int
}

type ChainPower struct {
	TotalRawBytes                      big.Int
	TotalRawBytesCommitted             big.Int
	TotalQualityAdjustedBytes          big.Int
	TotalQualityAdjustedBytesCommitted big.Int
	TotalPledgeCollateral              big.Int

	QaPowerSmoothed builtin.FilterEstimate

	MinerCount                  int64
	MinerCountAboveMinimumPower int64
}

type Miner struct {
	MinerID    address.Address
	OwnerAddr  address.Address
	WorkerAddr address.Address
	PeerID     *peer.ID
	SectorSize abi.SectorSize
}

type MinerPower struct {
	Act      ActorInfo
	RawPower big.Int
	QalPower big.Int
}

type SectorInfo struct {
	MinerID         address.Address
	SectorID        abi.SectorNumber
	SealedCid       cid.Cid
	ActivationEpoch abi.ChainEpoch
	ExpirationEpoch abi.ChainEpoch

	DealWeight         big.Int
	VerifiedDealWeight big.Int

	InitialPledge         big.Int
	ExpectedDayReward     big.Int
	ExpectedStoragePledge big.Int
}

type SectorPrecommitInfo struct {
	MinerID   address.Address
	SectorID  abi.SectorNumber
	SealedCid cid.Cid

	SealRandEpoch    abi.ChainEpoch
	ExpirationEpoch  abi.ChainEpoch
	PrecommitDeposit big.Int
	PrecommitEpoch   abi.ChainEpoch

	DealWeight         big.Int
	VerifiedDealWeight big.Int
	IsReplaceCapacity  bool

	ReplaceSectorDeadline  uint64
	ReplaceSectorPartition uint64
	ReplaceSectorNumber    abi.SectorNumber
}

type SectorLifecycleEvent string

type MinerSectorEvent struct {
	MinerID  address.Address
	SectorID uint64
	Event    SectorLifecycleEvent
}

type SectorDealEvent struct {
	DealID   abi.DealID
	MinerID  address.Address
	SectorID uint64
}

type PartitionStatus struct {
	Terminated bitfield.BitField
	Expired    bitfield.BitField
	Faulted    bitfield.BitField
	InRecovery bitfield.BitField
	Recovered  bitfield.BitField
}

type MultisigTransaction struct {
	MultisigID    address.Address
	TransactionID int64

	// Transaction State
	To       address.Address
	Value    big.Int
	Method   uint64
	Params   []byte
	Approved []string
}

type UserBlsMessage struct {
	Msg    *types.Message
	Result *InvokeResult
}

type UserSecpMessage struct {
	Msg    *types.SignedMessage
	Result *InvokeResult
}

type SystemMessage struct {
	Msg    *types.Message
	Result *InvokeResult
}
type FullTipSet struct {
	// here is raw
	PrevTs *types.TipSet
	CurTs  *types.TipSet

	// PrevTs ActorState
	// Note!!! it is store at CurTs.Height!!!
	ChangedActors       []ActorInfo
	ChainPower          *ChainPower
	ChainReward         *ChainReward
	MinerPowers         []*MinerPower
	Miners              []*Miner
	ChainInitActor      *ChainInitActor
	MarketDealProposals []*MarketDealProposal
	MarketDealStates    []*MarketDealState
	PaymentChannels     []*PaymentChannel

	SectorInfos          []*SectorInfo
	SectorPrecommitInfos []*SectorPrecommitInfo
	MinerSectorEvents    []*MinerSectorEvent
	SectorDealEvents     []*SectorDealEvent

	MultiSigTxs []*MultisigTransaction

	// CurTs Message And Blocks
	AllBlockMessages     []*api.BlockMessages
	AllBlockMessageSet   map[cid.Cid]int
	UnProcessedMsgCidSet map[cid.Cid]struct{}

	UserBlsMessages  []*UserBlsMessage  // deduplicated
	UserSecpMessages []*UserSecpMessage // deduplicated
	SystemMessages   []*SystemMessage

	ExecutionTraceMesages []*ExecutionTraceMessage

	TotalReward  abi.TokenAmount
	BlockRewards []abi.TokenAmount
	Penalty      abi.TokenAmount

	//GasCosts GasOutputs

	CurTsChangeAddressList []address.Address
}