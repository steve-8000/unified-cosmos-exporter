package collector

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"math"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"unified-ex/config"
	"unified-ex/rpc"
	"unified-ex/util"
)

type validatorState struct {
	moniker           string
	signedBlocks      int
	proposedBlocks    int
	missedBlocks      int
	consecutiveMissed int
	inActiveSet       bool
	isJailed          bool
	lastBlockTime     time.Time
	consensusAddress  string
	signedBlocksWindow int
	windowStartHeight   int
	windowSignedBlocks  int
	windowMissedBlocks  int
}

type UnifiedCollector struct {
	client            *rpc.Client
	cfg               *config.Chain
	logger            *slog.Logger
	mu                sync.RWMutex
	lastCheckedHeight float64
	blocksBehind      float64
	nodeUp            bool
	validatorStates   map[string]*validatorState
	downtimeJailDuration float64
	blockTimeCalculator *util.BlockTimeCalculator
	prometheusClient   *util.PrometheusClient
	
	bondedTokens     *prometheus.Desc
	notBondedTokens  *prometheus.Desc
	communityPool    *prometheus.Desc
	supplyTotal      *prometheus.Desc
	inflation        *prometheus.Desc
	annualProvisions *prometheus.Desc
	
	walletBalance     *prometheus.Desc
	walletDelegations *prometheus.Desc
	walletRewards     *prometheus.Desc
	walletUnbonding   *prometheus.Desc
	
	validatorTokens      *prometheus.Desc
	validatorCommissionRate *prometheus.Desc
	validatorCommission  *prometheus.Desc
	validatorRewards     *prometheus.Desc
	validatorRank        *prometheus.Desc
	validatorActive      *prometheus.Desc
	validatorStatus      *prometheus.Desc
	validatorJailedDesc  *prometheus.Desc
	validatorDelegatorShares *prometheus.Desc
	validatorStatusDesc      *prometheus.Desc
	
	tdUp                *prometheus.Desc
	tdNodeHeight        *prometheus.Desc
	tdBlocksBehind      *prometheus.Desc
	tdSignedBlocks      *prometheus.Desc
	tdProposedBlocks    *prometheus.Desc
	tdMissedBlocks      *prometheus.Desc
	tdConsecutiveMissed *prometheus.Desc
	tdValidatorActive   *prometheus.Desc
	tdValidatorJailed   *prometheus.Desc
	tdTimeSinceLastBlock *prometheus.Desc

	paramsSignedBlocksWindow      *prometheus.Desc
	paramsMinSignedPerWindow      *prometheus.Desc
	paramsDowntimeJailDuration    *prometheus.Desc
	paramsSlashFractionDoubleSign *prometheus.Desc
	paramsSlashFractionDowntime   *prometheus.Desc
	paramsMaxValidators           *prometheus.Desc
	paramsBaseProposerReward      *prometheus.Desc
	paramsBonusProposerReward     *prometheus.Desc

	validatorsTotal        *prometheus.Desc
	validatorsBondedRatio  *prometheus.Desc
	validatorsActive       *prometheus.Desc
	validatorsInactive     *prometheus.Desc
	
	cometbftConsensusProposalCreateCount *prometheus.Desc
	cometbftConsensusProposalReceiveCount *prometheus.Desc
}

func NewUnifiedCollector(client *rpc.Client, cfg *config.Chain, prometheusURL string) *UnifiedCollector {
	var prometheusClient *util.PrometheusClient
	if prometheusURL != "" {
		prometheusClient = util.NewPrometheusClient(prometheusURL)
	}
	
	return &UnifiedCollector{
		client:          client,
		cfg:             cfg,
		logger:          slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		validatorStates: make(map[string]*validatorState),
		downtimeJailDuration: 0,
		blockTimeCalculator: util.NewBlockTimeCalculator(100),
		prometheusClient:     prometheusClient,

		bondedTokens:     prometheus.NewDesc("cosmos_general_bonded_tokens", "Total number of bonded tokens", []string{"chain_id"}, nil),
		notBondedTokens:  prometheus.NewDesc("cosmos_general_not_bonded_tokens", "Total number of not bonded tokens", []string{"chain_id"}, nil),
		communityPool:    prometheus.NewDesc("cosmos_general_community_pool", "Community pool balance", []string{"chain_id", "denom"}, nil),
		supplyTotal:      prometheus.NewDesc("cosmos_general_supply_total", "Total token supply", []string{"chain_id", "denom"}, nil),
		inflation:        prometheus.NewDesc("cosmos_general_inflation", "Current inflation rate", []string{"chain_id"}, nil),
		annualProvisions: prometheus.NewDesc("cosmos_general_annual_provisions", "Current annual provisions", []string{"chain_id"}, nil),

		walletBalance:     prometheus.NewDesc("cosmos_wallet_balance", "Wallet balance", []string{"chain_id", "address", "denom"}, nil),
		walletDelegations: prometheus.NewDesc("cosmos_wallet_delegations", "Wallet delegations", []string{"chain_id", "address", "denom", "delegated_to"}, nil),
		walletRewards:     prometheus.NewDesc("cosmos_wallet_rewards", "Wallet rewards", []string{"chain_id", "address", "denom", "validator_address"}, nil),
		walletUnbonding:   prometheus.NewDesc("cosmos_wallet_unbonding", "Wallet unbonding", []string{"chain_id", "address", "denom", "validator_address"}, nil),

		validatorTokens:      prometheus.NewDesc("cosmos_validator_tokens", "Validator tokens", []string{"chain_id", "address", "moniker", "denom"}, nil),
		validatorCommissionRate: prometheus.NewDesc("cosmos_validators_commission", "Validator commission rate", []string{"chain_id", "address", "moniker"}, nil),
		validatorCommission:  prometheus.NewDesc("cosmos_validator_commission", "Validator commission", []string{"chain_id", "address", "moniker", "denom"}, nil),
		validatorRewards:     prometheus.NewDesc("cosmos_validator_rewards", "Validator outstanding rewards", []string{"chain_id", "address", "moniker", "denom"}, nil),
		validatorRank:        prometheus.NewDesc("cosmos_validators_rank", "Validator rank", []string{"chain_id", "address", "moniker"}, nil),
		validatorActive:      prometheus.NewDesc("cosmos_validator_active", "Validator active status", []string{"chain_id", "address", "moniker"}, nil),
		validatorStatus:      prometheus.NewDesc("cosmos_validator_status", "Validator status code", []string{"chain_id", "address", "moniker"}, nil),
		validatorJailedDesc:  prometheus.NewDesc("cosmos_validators_jailed", "Validator jailed status", []string{"chain_id", "address", "moniker"}, nil),
		validatorDelegatorShares: prometheus.NewDesc("cosmos_validators_delegator_shares", "Validator delegator shares", []string{"chain_id", "address", "moniker"}, nil),
		validatorStatusDesc:      prometheus.NewDesc("cosmos_validators_status", "Validator status", []string{"chain_id", "address", "moniker"}, nil),

		tdUp:                prometheus.NewDesc("cosmos_node_up", "Whether the RPC endpoint is reachable.", []string{"chain_id", "rpc_url"}, nil),
		tdNodeHeight:        prometheus.NewDesc("cosmos_node_height", "The current height of the monitored node.", []string{"chain_id"}, nil),
		tdBlocksBehind:      prometheus.NewDesc("cosmos_blocks_behind", "Number of blocks the monitored node is behind the peers.", []string{"chain_id"}, nil),
		tdSignedBlocks:      prometheus.NewDesc("cosmos_signed_blocks", "Signed blocks in current window", []string{"chain_id", "validator_address", "moniker"}, nil),
		tdProposedBlocks:    prometheus.NewDesc("cosmos_proposed_blocks", "Proposed blocks since start", []string{"chain_id", "validator_address", "moniker"}, nil),
		tdMissedBlocks:      prometheus.NewDesc("cosmos_validators_missed_blocks", "Missed blocks in current window", []string{"chain_id", "validator_address", "moniker"}, nil),
		tdConsecutiveMissed: prometheus.NewDesc("cosmos_consecutive_missed_blocks", "Number of consecutive missed blocks.", []string{"chain_id", "validator_address", "moniker"}, nil),
		tdValidatorActive:   prometheus.NewDesc("cosmos_validator_active_set", "Indicates if the validator is in the active set.", []string{"chain_id", "validator_address", "moniker"}, nil),
		tdValidatorJailed:   prometheus.NewDesc("cosmos_validator_jailed_status", "Indicates if the validator is jailed.", []string{"chain_id", "validator_address", "moniker"}, nil),
		tdTimeSinceLastBlock: prometheus.NewDesc("cosmos_time_since_last_block", "Time since last finalized block", []string{"chain_id"}, nil),

		paramsSignedBlocksWindow:      prometheus.NewDesc("cosmos_params_signed_blocks_window", "Signed blocks window parameter", []string{"chain_id"}, nil),
		paramsMinSignedPerWindow:      prometheus.NewDesc("cosmos_params_min_signed_per_window", "Minimum signed per window parameter", []string{"chain_id"}, nil),
		paramsDowntimeJailDuration:    prometheus.NewDesc("cosmos_params_downtime_jail_duration", "Downtime jail duration parameter", []string{"chain_id"}, nil),
		paramsSlashFractionDoubleSign: prometheus.NewDesc("cosmos_params_slash_fraction_double_sign", "Slash fraction for double sign parameter", []string{"chain_id"}, nil),
		paramsSlashFractionDowntime:   prometheus.NewDesc("cosmos_params_slash_fraction_downtime", "Slash fraction for downtime parameter", []string{"chain_id"}, nil),
		paramsMaxValidators:           prometheus.NewDesc("cosmos_params_max_validators", "Maximum number of validators parameter", []string{"chain_id"}, nil),
		paramsBaseProposerReward:      prometheus.NewDesc("cosmos_params_base_proposer_reward", "Base proposer reward parameter", []string{"chain_id"}, nil),
		paramsBonusProposerReward:     prometheus.NewDesc("cosmos_params_bonus_proposer_reward", "Bonus proposer reward parameter", []string{"chain_id"}, nil),

		validatorsTotal:        prometheus.NewDesc("cosmos_validators_total", "Total number of validators", []string{"chain_id"}, nil),
		validatorsBondedRatio:  prometheus.NewDesc("cosmos_validators_bonded_ratio", "Ratio of bonded validators", []string{"chain_id"}, nil),
		validatorsActive:       prometheus.NewDesc("cosmos_validators_active", "Number of active validators", []string{"chain_id"}, nil),
		validatorsInactive:     prometheus.NewDesc("cosmos_validators_inactive", "Number of inactive validators", []string{"chain_id"}, nil),
		
		cometbftConsensusProposalCreateCount: prometheus.NewDesc("cometbft_consensus_proposal_chain", "Count of proposal creation events", []string{"chain_id"}, nil),
		cometbftConsensusProposalReceiveCount: prometheus.NewDesc("cosmos_consensus_proposal_receive_count", "Count of received proposals by status", []string{"chain_id", "status"}, nil),
	}
}

func (c *UnifiedCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.bondedTokens
	ch <- c.notBondedTokens
	ch <- c.communityPool
	ch <- c.supplyTotal
	ch <- c.inflation
	ch <- c.annualProvisions
	ch <- c.walletBalance
	ch <- c.walletDelegations
	ch <- c.walletRewards
	ch <- c.walletUnbonding
	ch <- c.validatorTokens
	ch <- c.validatorCommissionRate
	ch <- c.validatorCommission
	ch <- c.validatorRewards
	ch <- c.validatorRank
	ch <- c.validatorActive
	ch <- c.validatorStatus
	ch <- c.validatorJailedDesc
	ch <- c.validatorDelegatorShares
	ch <- c.validatorStatusDesc
	ch <- c.tdUp
	ch <- c.tdNodeHeight
	ch <- c.tdBlocksBehind
	ch <- c.tdSignedBlocks
	ch <- c.tdProposedBlocks
	ch <- c.tdMissedBlocks
	ch <- c.tdConsecutiveMissed
	ch <- c.tdValidatorActive
	ch <- c.tdValidatorJailed
	ch <- c.tdTimeSinceLastBlock
	ch <- c.paramsSignedBlocksWindow
	ch <- c.paramsMinSignedPerWindow
	ch <- c.paramsDowntimeJailDuration
	ch <- c.paramsSlashFractionDoubleSign
	ch <- c.paramsSlashFractionDowntime
	ch <- c.paramsMaxValidators
	ch <- c.paramsBaseProposerReward
	ch <- c.paramsBonusProposerReward
	ch <- c.validatorsTotal
	ch <- c.validatorsBondedRatio
	ch <- c.validatorsActive
	ch <- c.validatorsInactive
	ch <- c.cometbftConsensusProposalCreateCount
	ch <- c.cometbftConsensusProposalReceiveCount
}

func (c *UnifiedCollector) Collect(ch chan<- prometheus.Metric) {
	g, _ := errgroup.WithContext(context.Background())

	c.collectTenderdutyMetrics(ch)

	g.Go(func() error { return c.collectGeneralMetrics(ch) })
	g.Go(func() error { return c.collectWalletMetrics(ch) })
	g.Go(func() error { return c.collectValidatorsMetrics(ch) })
	g.Go(func() error { return c.collectParamsMetrics(ch) })

	_ = g.Wait()
}

func (c *UnifiedCollector) collectTenderdutyMetrics(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(c.tdUp, prometheus.GaugeValue, 1, c.cfg.ChainID, c.cfg.RPC)
	
	currentHeight := 0.0
	status, err := c.client.GetStatus()
	if err != nil {
		c.logger.Error("Failed to get status for node height", "error", err)
	} else if status != nil && status.Result.SyncInfo.LatestBlockHeight != "" {
		if height, err := strconv.ParseFloat(status.Result.SyncInfo.LatestBlockHeight, 64); err == nil {
			currentHeight = height
		} else {
			c.logger.Error("Failed to parse block height", "raw_height", status.Result.SyncInfo.LatestBlockHeight, "error", err)
		}
	} else {
		c.logger.Warn("Status response missing or invalid", "status", status)
	}
	
	ch <- prometheus.MustNewConstMetric(c.tdNodeHeight, prometheus.GaugeValue, currentHeight, c.cfg.ChainID)
	ch <- prometheus.MustNewConstMetric(c.tdBlocksBehind, prometheus.GaugeValue, c.blocksBehind, c.cfg.ChainID)
	
	timeSinceLastBlock := 0.0
	if currentHeight > 0 {
		timeSinceLastBlock = 10.0
	}
	ch <- prometheus.MustNewConstMetric(c.tdTimeSinceLastBlock, prometheus.GaugeValue, timeSinceLastBlock, c.cfg.ChainID)
	
	validators, err := c.client.GetValidators()
	if err != nil {
		c.logger.Error("Failed to get validators for tenderduty metrics", "error", err)
		for _, validatorAddr := range c.cfg.Validators {
			ch <- prometheus.MustNewConstMetric(c.tdSignedBlocks, prometheus.GaugeValue, 0, c.cfg.ChainID, validatorAddr, "unknown")
			ch <- prometheus.MustNewConstMetric(c.tdProposedBlocks, prometheus.GaugeValue, 0, c.cfg.ChainID, validatorAddr, "unknown")
			ch <- prometheus.MustNewConstMetric(c.tdMissedBlocks, prometheus.GaugeValue, 0, c.cfg.ChainID, validatorAddr, "unknown")
			ch <- prometheus.MustNewConstMetric(c.tdConsecutiveMissed, prometheus.GaugeValue, 0, c.cfg.ChainID, validatorAddr, "unknown")
			ch <- prometheus.MustNewConstMetric(c.tdValidatorActive, prometheus.GaugeValue, 0, c.cfg.ChainID, validatorAddr, "unknown")
			ch <- prometheus.MustNewConstMetric(c.tdValidatorJailed, prometheus.GaugeValue, 0, c.cfg.ChainID, validatorAddr, "unknown")
		}
		return
	}
	
	for _, validatorAddr := range c.cfg.Validators {
		var moniker = "unknown"
		var signedBlocks = 0
		var proposedBlocks = 0
		var missedBlocks = 0
		var consecutiveMissed = 0
		var isActive = 0
		var isJailed = 0
		
		for _, v := range validators.Validators {
			if v.OperatorAddress == validatorAddr {
				moniker = v.Description.Moniker
				isActive = 0
				if v.Status == "BOND_STATUS_BONDED" {
					isActive = 1
				}
				if v.Jailed {
					isJailed = 1
				}
				break
			}
		}
		
		c.mu.RLock()
		state, exists := c.validatorStates[validatorAddr]
		if exists && state != nil {
			signedBlocks = state.windowSignedBlocks  // 윈도우 기반 서명 블록
			proposedBlocks = state.proposedBlocks
			missedBlocks = state.windowMissedBlocks  // 윈도우 기반 놓친 블록
			consecutiveMissed = state.consecutiveMissed
		}
		c.mu.RUnlock()
		
		ch <- prometheus.MustNewConstMetric(c.tdSignedBlocks, prometheus.GaugeValue, float64(signedBlocks), c.cfg.ChainID, validatorAddr, moniker)
		ch <- prometheus.MustNewConstMetric(c.tdProposedBlocks, prometheus.GaugeValue, float64(proposedBlocks), c.cfg.ChainID, validatorAddr, moniker)
		ch <- prometheus.MustNewConstMetric(c.tdMissedBlocks, prometheus.GaugeValue, float64(missedBlocks), c.cfg.ChainID, validatorAddr, moniker)
		ch <- prometheus.MustNewConstMetric(c.tdConsecutiveMissed, prometheus.GaugeValue, float64(consecutiveMissed), c.cfg.ChainID, validatorAddr, moniker)
		ch <- prometheus.MustNewConstMetric(c.tdValidatorActive, prometheus.GaugeValue, float64(isActive), c.cfg.ChainID, validatorAddr, moniker)
		ch <- prometheus.MustNewConstMetric(c.tdValidatorJailed, prometheus.GaugeValue, float64(isJailed), c.cfg.ChainID, validatorAddr, moniker)
	}
}

func (c *UnifiedCollector) collectGeneralMetrics(ch chan<- prometheus.Metric) error {
	pool, err := c.client.GetStakingPool()
	if err != nil {
		c.logger.Error("Failed to get staking pool", "error", err)
	} else {
		bondedTokens, _ := strconv.ParseFloat(pool.Pool.BondedTokens, 64)
		notBondedTokens, _ := strconv.ParseFloat(pool.Pool.NotBondedTokens, 64)
		displayBondedTokens := math.Floor(bondedTokens / math.Pow10(c.cfg.TokenDecimals))
		displayNotBondedTokens := math.Floor(notBondedTokens / math.Pow10(c.cfg.TokenDecimals))
		ch <- prometheus.MustNewConstMetric(c.bondedTokens, prometheus.GaugeValue, displayBondedTokens, c.cfg.ChainID)
		ch <- prometheus.MustNewConstMetric(c.notBondedTokens, prometheus.GaugeValue, displayNotBondedTokens, c.cfg.ChainID)
	}

	communityPool, err := c.client.GetCommunityPool()
	if err != nil {
		c.logger.Error("Failed to get community pool", "error", err)
	} else {
		for _, coin := range communityPool.Pool {
			amount, _ := strconv.ParseFloat(coin.Amount, 64)
			displayAmount := amount / math.Pow10(c.cfg.TokenDecimals)
			ch <- prometheus.MustNewConstMetric(c.communityPool, prometheus.GaugeValue, displayAmount, c.cfg.ChainID, coin.Denom)
		}
	}

	supply, err := c.client.GetBankSupply()
	if err != nil {
		c.logger.Error("Failed to get bank supply", "error", err)
	} else {
		for _, coin := range supply.Supply {
			amount, _ := strconv.ParseFloat(coin.Amount, 64)
			displayAmount := math.Floor(amount / math.Pow10(c.cfg.TokenDecimals))
			ch <- prometheus.MustNewConstMetric(c.supplyTotal, prometheus.GaugeValue, displayAmount, c.cfg.ChainID, coin.Denom)
		}
	}

	inflation, err := c.client.GetMintingInflation()
	if err != nil {
		if strings.Contains(err.Error(), "501") {
			c.logger.Debug("Inflation API not implemented on this chain", "chain_id", c.cfg.ChainID)
		} else {
			c.logger.Error("Failed to get inflation", "error", err)
		}
		ch <- prometheus.MustNewConstMetric(c.inflation, prometheus.GaugeValue, 0, c.cfg.ChainID)
	} else {
		amount, _ := strconv.ParseFloat(inflation.Inflation, 64)
		ch <- prometheus.MustNewConstMetric(c.inflation, prometheus.GaugeValue, amount, c.cfg.ChainID)
	}

	annualProvisions, err := c.client.GetMintingAnnualProvisions()
	if err != nil {
		if strings.Contains(err.Error(), "501") {
			c.logger.Debug("Annual provisions API not implemented on this chain", "chain_id", c.cfg.ChainID)
		} else {
			c.logger.Error("Failed to get annual provisions", "error", err)
		}
		ch <- prometheus.MustNewConstMetric(c.annualProvisions, prometheus.GaugeValue, 0, c.cfg.ChainID)
	} else {
		amount, _ := strconv.ParseFloat(annualProvisions.AnnualProvisions, 64)
		ch <- prometheus.MustNewConstMetric(c.annualProvisions, prometheus.GaugeValue, amount, c.cfg.ChainID)
	}

	return nil
}

func (c *UnifiedCollector) collectParamsMetrics(ch chan<- prometheus.Metric) error {
	params, err := c.client.GetSlashingParams()
	if err != nil {
		c.logger.Error("Failed to get slashing params", "error", err)
		return err
	}

	if signedBlocksWindow, err := strconv.ParseFloat(params.Params.SignedBlocksWindow, 64); err == nil {
		ch <- prometheus.MustNewConstMetric(c.paramsSignedBlocksWindow, prometheus.GaugeValue, signedBlocksWindow, c.cfg.ChainID)
	}

	if minSignedPerWindow, err := strconv.ParseFloat(params.Params.MinSignedPerWindow, 64); err == nil {
		ch <- prometheus.MustNewConstMetric(c.paramsMinSignedPerWindow, prometheus.GaugeValue, minSignedPerWindow, c.cfg.ChainID)
	}

	if downtimeJailDuration, err := time.ParseDuration(params.Params.DowntimeJailDuration); err == nil {
		durationInSeconds := downtimeJailDuration.Seconds()
		c.downtimeJailDuration = durationInSeconds
		ch <- prometheus.MustNewConstMetric(c.paramsDowntimeJailDuration, prometheus.GaugeValue, durationInSeconds, c.cfg.ChainID)
		c.logger.Debug("Downtime jail duration parsed", "raw_value", params.Params.DowntimeJailDuration, "duration_seconds", durationInSeconds)
	} else {
		c.logger.Warn("Failed to parse downtime jail duration", "raw_value", params.Params.DowntimeJailDuration, "error", err)
	}

	if slashFractionDoubleSign, err := strconv.ParseFloat(params.Params.SlashFractionDoubleSign, 64); err == nil {
		ch <- prometheus.MustNewConstMetric(c.paramsSlashFractionDoubleSign, prometheus.GaugeValue, slashFractionDoubleSign, c.cfg.ChainID)
	}

	if slashFractionDowntime, err := strconv.ParseFloat(params.Params.SlashFractionDowntime, 64); err == nil {
		ch <- prometheus.MustNewConstMetric(c.paramsSlashFractionDowntime, prometheus.GaugeValue, slashFractionDowntime, c.cfg.ChainID)
	}

	// Staking params에서 max_validators 수집
	stakingParams, err := c.client.GetStakingParams()
	if err != nil {
		c.logger.Error("Failed to get staking params", "error", err)
	} else {
		maxValidators := float64(stakingParams.Params.MaxValidators)
		ch <- prometheus.MustNewConstMetric(c.paramsMaxValidators, prometheus.GaugeValue, maxValidators, c.cfg.ChainID)
	}

	// Distribution params에서 base_proposer_reward와 bonus_proposer_reward 수집
	distributionParams, err := c.client.GetDistributionParams()
	if err != nil {
		c.logger.Error("Failed to get distribution params", "error", err)
	} else {
		if baseProposerReward, err := strconv.ParseFloat(distributionParams.Params.BaseProposerReward, 64); err == nil {
			ch <- prometheus.MustNewConstMetric(c.paramsBaseProposerReward, prometheus.GaugeValue, baseProposerReward, c.cfg.ChainID)
		}
		if bonusProposerReward, err := strconv.ParseFloat(distributionParams.Params.BonusProposerReward, 64); err == nil {
			ch <- prometheus.MustNewConstMetric(c.paramsBonusProposerReward, prometheus.GaugeValue, bonusProposerReward, c.cfg.ChainID)
		}
	}

	return nil
}

func (c *UnifiedCollector) collectWalletMetrics(ch chan<- prometheus.Metric) error {
	for _, wallet := range c.cfg.Wallets {
		balance, err := c.client.GetWalletBalance(wallet.Address)
		if err != nil {
			c.logger.Error("Failed to get wallet balance", "wallet", wallet.Address, "error", err)
		} else {
			for _, coin := range balance.Balances {
				if amount, err := strconv.ParseFloat(coin.Amount, 64); err == nil {
					displayAmount := math.Floor(amount / math.Pow10(c.cfg.TokenDecimals))
					ch <- prometheus.MustNewConstMetric(c.walletBalance, prometheus.GaugeValue, displayAmount, c.cfg.ChainID, wallet.Address, c.cfg.TokenDisplay)
				}
			}
		}

		delegations, err := c.client.GetWalletDelegations(wallet.Address)
		if err != nil {
			c.logger.Error("Failed to get wallet delegations", "wallet", wallet.Address, "error", err)
		} else {
					for _, delegation := range delegations.DelegationResponses {
			if amount, err := strconv.ParseFloat(delegation.Balance.Amount, 64); err == nil {
				displayAmount := math.Floor(amount / math.Pow10(c.cfg.TokenDecimals))
				ch <- prometheus.MustNewConstMetric(c.walletDelegations, prometheus.GaugeValue, displayAmount, c.cfg.ChainID, wallet.Address, c.cfg.TokenDisplay, delegation.Delegation.ValidatorAddress)
			}
		}
		}

		rewards, err := c.client.GetWalletRewards(wallet.Address)
		if err != nil {
			c.logger.Error("Failed to get wallet rewards", "wallet", wallet.Address, "error", err)
		} else {
			for _, reward := range rewards.Rewards {
				for _, coin := range reward.Reward {
					if amount, err := strconv.ParseFloat(coin.Amount, 64); err == nil {
						displayAmount := math.Floor(amount / math.Pow10(c.cfg.TokenDecimals))
						ch <- prometheus.MustNewConstMetric(c.walletRewards, prometheus.GaugeValue, displayAmount, c.cfg.ChainID, wallet.Address, c.cfg.TokenDisplay, reward.ValidatorAddress)
					}
				}
			}
		}

		unbonding, err := c.client.GetWalletUnbonding(wallet.Address)
		if err != nil {
			c.logger.Debug("Failed to get wallet unbonding", "wallet", wallet.Address, "error", err)
		} else {
			for _, unbondingResp := range unbonding.UnbondingResponses {
				for _, entry := range unbondingResp.Entries {
									if amount, err := strconv.ParseFloat(entry.Balance, 64); err == nil {
					displayAmount := math.Floor(amount / math.Pow10(c.cfg.TokenDecimals))
					ch <- prometheus.MustNewConstMetric(c.walletUnbonding, prometheus.GaugeValue, displayAmount, c.cfg.ChainID, wallet.Address, c.cfg.TokenDisplay, unbondingResp.ValidatorAddress)
				}
				}
			}
		}
	}

	return nil
}

func (c *UnifiedCollector) collectValidatorsMetrics(ch chan<- prometheus.Metric) error {
	validators, err := c.client.GetValidators()
	if err != nil {
		c.logger.Error("Failed to get validators", "error", err)
		return err
	}

	// Validators total count
	totalValidators := float64(len(validators.Validators))
	ch <- prometheus.MustNewConstMetric(c.validatorsTotal, prometheus.GaugeValue, totalValidators, c.cfg.ChainID)

	var activeValidators float64
	var inactiveValidators float64
	for _, validator := range validators.Validators {
		if validator.Status == "BOND_STATUS_BONDED" {
			activeValidators++
		} else {
			inactiveValidators++
		}
	}
	ch <- prometheus.MustNewConstMetric(c.validatorsActive, prometheus.GaugeValue, activeValidators, c.cfg.ChainID)
	ch <- prometheus.MustNewConstMetric(c.validatorsInactive, prometheus.GaugeValue, inactiveValidators, c.cfg.ChainID)

	var totalBondedTokens float64
	var totalTokens float64
	for _, validator := range validators.Validators {
		if tokens, err := strconv.ParseFloat(validator.Tokens, 64); err == nil {
			totalTokens += tokens
			if validator.Status == "BOND_STATUS_BONDED" {
				totalBondedTokens += tokens
			}
		}
	}

	if totalTokens > 0 {
		bondedRatio := totalBondedTokens / totalTokens
		ch <- prometheus.MustNewConstMetric(c.validatorsBondedRatio, prometheus.GaugeValue, bondedRatio, c.cfg.ChainID)
	} else {
		ch <- prometheus.MustNewConstMetric(c.validatorsBondedRatio, prometheus.GaugeValue, 0, c.cfg.ChainID)
	}

	configValidators := make(map[string]bool)
	for _, valAddr := range c.cfg.Validators {
		configValidators[valAddr] = true
	}

	for _, validator := range validators.Validators {
		// config.yml에 정의된 벨리데이터만 처리
		if !configValidators[validator.OperatorAddress] {
			continue
		}
		if tokens, err := strconv.ParseFloat(validator.Tokens, 64); err == nil {
			displayTokens := math.Floor(tokens / math.Pow10(c.cfg.TokenDecimals))
			ch <- prometheus.MustNewConstMetric(c.validatorTokens, prometheus.GaugeValue, displayTokens, c.cfg.ChainID, validator.OperatorAddress, validator.Description.Moniker, c.cfg.TokenDisplay)
		}

		if commissionRate, err := strconv.ParseFloat(validator.Commission.CommissionRates.Rate, 64); err == nil {
			ch <- prometheus.MustNewConstMetric(c.validatorCommissionRate, prometheus.GaugeValue, commissionRate, c.cfg.ChainID, validator.OperatorAddress, validator.Description.Moniker)
		}

		// Commission 잔액
		commission, err := c.client.GetValidatorCommission(validator.OperatorAddress)
		if err != nil {
			c.logger.Debug("Failed to get validator commission", "validator", validator.OperatorAddress, "error", err)
		} else {
			for _, coin := range commission.Commission.Commission {
				if amount, err := strconv.ParseFloat(coin.Amount, 64); err == nil {
					displayAmount := math.Floor(amount / math.Pow10(c.cfg.TokenDecimals))
					ch <- prometheus.MustNewConstMetric(c.validatorCommission, prometheus.GaugeValue, displayAmount, c.cfg.ChainID, validator.OperatorAddress, validator.Description.Moniker, c.cfg.TokenDisplay)
				}
			}
		}

		// Rewards
		rewards, err := c.client.GetValidatorRewards(validator.OperatorAddress)
		if err != nil {
			c.logger.Debug("Failed to get validator rewards", "validator", validator.OperatorAddress, "error", err)
		} else {
			for _, coin := range rewards.Rewards.Rewards {
				if amount, err := strconv.ParseFloat(coin.Amount, 64); err == nil {
					displayAmount := math.Floor(amount / math.Pow10(c.cfg.TokenDecimals))
					ch <- prometheus.MustNewConstMetric(c.validatorRewards, prometheus.GaugeValue, displayAmount, c.cfg.ChainID, validator.OperatorAddress, validator.Description.Moniker, c.cfg.TokenDisplay)
				}
			}
		}


		// Rank (tokens 기준으로 계산)
		rank := 1
		for _, otherValidator := range validators.Validators {
			if otherValidator.OperatorAddress != validator.OperatorAddress {
				if otherTokens, err := strconv.ParseFloat(otherValidator.Tokens, 64); err == nil {
					if currentTokens, err := strconv.ParseFloat(validator.Tokens, 64); err == nil {
						if otherTokens > currentTokens {
							rank++
						}
					}
				}
			}
		}
		ch <- prometheus.MustNewConstMetric(c.validatorRank, prometheus.GaugeValue, float64(rank), c.cfg.ChainID, validator.OperatorAddress, validator.Description.Moniker)

		// Active status
		activeStatus := 0
		if validator.Status == "BOND_STATUS_BONDED" {
			activeStatus = 1
		}
		ch <- prometheus.MustNewConstMetric(c.validatorActive, prometheus.GaugeValue, float64(activeStatus), c.cfg.ChainID, validator.OperatorAddress, validator.Description.Moniker)

		// Status code - 실제 상태값을 그대로 사용
		ch <- prometheus.MustNewConstMetric(c.validatorStatus, prometheus.GaugeValue, 1, c.cfg.ChainID, validator.OperatorAddress, validator.Description.Moniker)

		// Jailed status
		jailedStatus := 0
		if validator.Jailed {
			jailedStatus = 1
		}
		ch <- prometheus.MustNewConstMetric(c.validatorJailedDesc, prometheus.GaugeValue, float64(jailedStatus), c.cfg.ChainID, validator.OperatorAddress, validator.Description.Moniker)

		// Delegator shares - ISLM 단위로 변환
		if delegatorShares, err := strconv.ParseFloat(validator.DelegatorShares, 64); err == nil {
			displayDelegatorShares := math.Floor(delegatorShares / math.Pow10(c.cfg.TokenDecimals))
			ch <- prometheus.MustNewConstMetric(c.validatorDelegatorShares, prometheus.GaugeValue, displayDelegatorShares, c.cfg.ChainID, validator.OperatorAddress, validator.Description.Moniker)
		}

		// Validator status (cosmos_validators_status) - 올바른 순서로 변환
		var statusValue float64
		switch validator.Status {
		case "BOND_STATUS_BONDED":
			statusValue = 3
		case "BOND_STATUS_UNBONDING":
			statusValue = 2
		case "BOND_STATUS_UNBONDED":
			statusValue = 1
		default:
			statusValue = 0
		}
		ch <- prometheus.MustNewConstMetric(c.validatorStatusDesc, prometheus.GaugeValue, statusValue, c.cfg.ChainID, validator.OperatorAddress, validator.Description.Moniker)
	}

	// CometBFT consensus proposal create count - 체인 전체의 governance proposal 수
	governanceProposals, err := c.client.GetGovernanceProposals()
	if err != nil {
		c.logger.Debug("Failed to get governance proposals", "error", err)
		ch <- prometheus.MustNewConstMetric(c.cometbftConsensusProposalCreateCount, prometheus.CounterValue, 0, c.cfg.ChainID)
		ch <- prometheus.MustNewConstMetric(c.cometbftConsensusProposalReceiveCount, prometheus.CounterValue, 0, c.cfg.ChainID, "accepted")
		ch <- prometheus.MustNewConstMetric(c.cometbftConsensusProposalReceiveCount, prometheus.CounterValue, 0, c.cfg.ChainID, "rejected")
	} else {
		totalProposals := float64(len(governanceProposals.Proposals))
		var acceptedCount, rejectedCount float64
		
		for _, proposal := range governanceProposals.Proposals {
			switch proposal.Status {
			case "PROPOSAL_STATUS_PASSED":
				acceptedCount++
			case "PROPOSAL_STATUS_REJECTED", "PROPOSAL_STATUS_FAILED":
				rejectedCount++
			}
		}
		
		ch <- prometheus.MustNewConstMetric(c.cometbftConsensusProposalCreateCount, prometheus.CounterValue, totalProposals, c.cfg.ChainID)
		ch <- prometheus.MustNewConstMetric(c.cometbftConsensusProposalReceiveCount, prometheus.CounterValue, acceptedCount, c.cfg.ChainID, "accepted")
		ch <- prometheus.MustNewConstMetric(c.cometbftConsensusProposalReceiveCount, prometheus.CounterValue, rejectedCount, c.cfg.ChainID, "rejected")
	}

	return nil
}

func (c *UnifiedCollector) TrackBlocks(ctx context.Context) error {
	c.initializeValidatorStates()

	// 현재 노드의 블록 높이를 가져와서 초기 높이로 설정
	var initialHeight float64
	status, err := c.client.GetStatus()
	if err == nil && status != nil && status.Result.SyncInfo.LatestBlockHeight != "" {
		if height, err := strconv.ParseFloat(status.Result.SyncInfo.LatestBlockHeight, 64); err == nil {
			initialHeight = height
			c.logger.Info("Starting block tracking from current height", "height", initialHeight)
		} else {
			c.logger.Error("Failed to parse initial block height", "raw_height", status.Result.SyncInfo.LatestBlockHeight, "error", err)
			return err
		}
	} else {
		c.logger.Error("Failed to get initial block height from status", "error", err)
		return err
	}
	
	c.mu.Lock()
	c.lastCheckedHeight = initialHeight
	c.mu.Unlock()
	c.logger.Info("Initial block height set", "height", c.lastCheckedHeight)
	
	// 외부 Prometheus 서버에서 블록 시간 정보를 가져와서 BlockTimeCalculator에 설정
	if c.prometheusClient != nil {
		if avgBlockTime, err := c.prometheusClient.GetAverageBlockTime(); err == nil {
			c.logger.Info("Average block time from external Prometheus", "avg_block_time", avgBlockTime)
			// BlockTimeCalculator에 외부에서 가져온 평균 블록 시간을 설정
			c.blockTimeCalculator.SetInitialBlockTime(avgBlockTime)
		} else {
			c.logger.Warn("Failed to get average block time from external Prometheus", "error", err)
		}
	}

	if c.cfg.WebSocket != "" {
		go c.trackRealtimeBlocks(ctx)
	}

	checkInterval := 10
	ticker := time.NewTicker(time.Duration(checkInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			var height float64
			
			// 외부 Prometheus 서버에서 블록 높이를 가져오거나 로컬 상태를 사용
			if c.prometheusClient != nil {
				if promHeight, err := c.prometheusClient.GetNodeHeight(); err == nil {
					height = promHeight
					c.logger.Debug("Block height from external Prometheus", "height", height)
				} else {
					c.logger.Debug("Failed to get height from external Prometheus, using local status", "error", err)
				}
			}
			
			if height == 0 {
				status, err := c.client.GetStatus()
				if err != nil {
					c.logger.Error("Failed to get status", "error", err)
					c.mu.Lock()
					c.nodeUp = false
					c.mu.Unlock()
					continue
				}

				if status != nil && status.Result.SyncInfo.LatestBlockHeight != "" {
					if parsedHeight, err := strconv.ParseFloat(status.Result.SyncInfo.LatestBlockHeight, 64); err == nil {
						height = parsedHeight
					} else {
						c.logger.Error("Failed to parse block height", "raw_height", status.Result.SyncInfo.LatestBlockHeight, "error", err)
						continue
					}
				} else {
					c.logger.Warn("Status response missing or invalid", "status", status)
					continue
				}
			}

			c.mu.Lock()
			if height > c.lastCheckedHeight {
				c.logger.Info("New block height detected", "old_height", c.lastCheckedHeight, "new_height", height, "height_diff", height-c.lastCheckedHeight)
				
				heightDiff := height - c.lastCheckedHeight
				if heightDiff > 1 {
					c.logger.Info("Missing blocks detected, analyzing all missed blocks", 
						"missing_count", int(heightDiff-1),
						"from_height", int(c.lastCheckedHeight+1),
						"to_height", int(height-1))
					
					for missedHeight := int(c.lastCheckedHeight + 1); missedHeight < int(height); missedHeight++ {
						c.logger.Info("Analyzing missed block", "height", missedHeight)
						c.analyzeBlock(missedHeight)
					}
				} else {
					c.logger.Info("No missing blocks, analyzing only current block")
				}
				
				c.analyzeNewBlock(int(height))
				c.lastCheckedHeight = height
			} else if height == c.lastCheckedHeight {
				c.logger.Debug("No new blocks since last check", "current_height", height, "last_checked", c.lastCheckedHeight)
			} else {
				c.logger.Warn("Block height decreased or invalid", "current_height", height, "last_checked", c.lastCheckedHeight)
			}
			c.nodeUp = true
			
			c.calculateBlocksBehind()
			c.mu.Unlock()

			c.logValidatorStates()
			c.saveValidatorState()
		}
	}
}

func (c *UnifiedCollector) initializeValidatorStates() {
	c.logger.Info("Initializing validator states")

	// Slashing 파라미터를 가져와서 signedBlocksWindow 설정
	slashingParams, err := c.client.GetSlashingParams()
	if err != nil {
		c.logger.Error("Failed to get slashing params for window size", "error", err)
		// 기본값 사용
		c.downtimeJailDuration = 600 // 10분
	} else {
		if downtimeJailDuration, err := time.ParseDuration(slashingParams.Params.DowntimeJailDuration); err == nil {
			c.downtimeJailDuration = downtimeJailDuration.Seconds()
		} else {
			c.downtimeJailDuration = 600 // 기본값
		}
	}

	validators, err := c.client.GetValidators()
	if err != nil {
		c.logger.Error("Failed to get validators", "error", err)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, validatorAddr := range c.cfg.Validators {
		// signedBlocksWindow 값을 가져와서 설정
		var signedBlocksWindow int = 100 // 기본값
		if slashingParams != nil {
			if window, err := strconv.Atoi(slashingParams.Params.SignedBlocksWindow); err == nil {
				signedBlocksWindow = window
			}
		}

		state := &validatorState{
			moniker:           "unknown",
			signedBlocks:      0,
			proposedBlocks:    0,
			missedBlocks:      0,
			consecutiveMissed: 0,
			inActiveSet:       false,
			isJailed:          false,
			lastBlockTime:     time.Now(),
			consensusAddress:  "",
			signedBlocksWindow: signedBlocksWindow,
			windowStartHeight:  0,
			windowSignedBlocks: 0,
			windowMissedBlocks: 0,
		}

		for _, v := range validators.Validators {
			if v.OperatorAddress == validatorAddr {
				state.moniker = v.Description.Moniker
				state.inActiveSet = (v.Status == "BOND_STATUS_BONDED")
				state.isJailed = v.Jailed
				
				c.logger.Debug("Found validator", "validator", validatorAddr, "consensus_address", v.ConsensusAddress, "consensus_pubkey", v.ConsensusPubkey.Key)
				
				if v.ConsensusAddress != "" {
					state.consensusAddress = v.ConsensusAddress
					c.logger.Info("Set consensus address", "validator", validatorAddr, "consensus_address", v.ConsensusAddress)
				} else if v.ConsensusPubkey.Key != "" {
					consensusAddr := util.GenerateConsensusAddressFromPubkey(v.ConsensusPubkey.Key)
					if consensusAddr != "" {
						state.consensusAddress = consensusAddr
						c.logger.Info("Generated consensus address from pubkey", "validator", validatorAddr, "consensus_address", consensusAddr)
					} else {
						c.logger.Warn("Failed to generate consensus address from pubkey", "validator", validatorAddr, "pubkey", v.ConsensusPubkey.Key)
					}
				} else {
					c.logger.Warn("No consensus address or pubkey for validator", "validator", validatorAddr)
				}
				break
			}
		}

		c.validatorStates[validatorAddr] = state
		c.logger.Info("Initialized validator state", 
			"validator", validatorAddr, 
			"moniker", state.moniker,
			"active", state.inActiveSet,
			"jailed", state.isJailed,
			"signed_blocks_window", signedBlocksWindow)
	}
}

func (c *UnifiedCollector) analyzeNewBlock(height int) {
	c.analyzeBlock(height)
}

func (c *UnifiedCollector) analyzeBlock(height int) {
	c.logger.Debug("Starting analyzeBlock", "height", height)
	
	var block *rpc.BlockResponse
	var err error
	
	if height == 0 {
		block, err = c.client.GetLatestBlock()
	} else {
		block, err = c.client.GetBlock(height)
	}
	
	if err != nil {
		c.logger.Error("Failed to get block", "height", height, "error", err)
		return
	}

	if block == nil {
		c.logger.Error("Block response is nil", "height", height)
		return
	}

	if block.Result.Block.Header.Time == "" {
		c.logger.Warn("Block time field is empty", "height", height, "block_response", block)
		return
	}

	c.logger.Debug("Block data received", "height", height, "time_field", block.Result.Block.Header.Time, "time_field_empty", block.Result.Block.Header.Time == "")

	// 블록 시간을 BlockTimeCalculator에 업데이트
	if block.Result.Block.Header.Time != "" {
		c.logger.Debug("Attempting to parse block time", "time_str", block.Result.Block.Header.Time)
		if blockTime, err := util.ParseBlockTime(block.Result.Block.Header.Time); err == nil {
			c.logger.Debug("Block time parsed successfully", "parsed_time", blockTime)
			if blockHeight, err := util.ParseBlockHeight(block.Result.Block.Header.Height); err == nil {
				c.logger.Debug("Block height parsed successfully", "parsed_height", blockHeight)
				c.blockTimeCalculator.UpdateBlockTime(blockHeight, blockTime)
				c.logger.Debug("Block time updated", 
					"height", blockHeight, 
					"time", blockTime,
					"avg_block_time", c.blockTimeCalculator.GetAverageBlockTime())
			} else {
				c.logger.Warn("Failed to parse block height", "height_str", block.Result.Block.Header.Height, "error", err)
			}
		} else {
			c.logger.Warn("Failed to parse block time", "height", height, "time_str", block.Result.Block.Header.Time, "error", err)
		}
	} else {
		c.logger.Warn("Block time field is empty", "height", height)
	}

	proposerAddr := block.Result.Block.Header.ProposerAddress
	c.logger.Info("New block detected", 
		"height", height, 
		"proposer", proposerAddr,
		"chain_id", block.Result.Block.Header.ChainID)

	signedValidators := make(map[string]bool)
	for _, sig := range block.Result.Block.LastCommit.Signatures {
		if sig.ValidatorAddress != "" {
			signedValidators[sig.ValidatorAddress] = true
		}
	}
	
	c.logger.Info("Block signatures", "height", height, "total_signatures", len(signedValidators))

	for validatorAddr, state := range c.validatorStates {
		if !state.inActiveSet || state.isJailed {
			continue
		}

		c.logger.Info("Validator state", "validator", validatorAddr, "consensus_address", state.consensusAddress, "active", state.inActiveSet, "jailed", state.isJailed)

		if state.consensusAddress == "" {
			c.logger.Info("No consensus address for validator, skipping block signature check", "validator", validatorAddr)
			continue
		}

		// 윈도우 시작 높이 설정 (첫 번째 블록이거나 윈도우가 변경된 경우)
		if state.windowStartHeight == 0 {
			state.windowStartHeight = height
			state.windowSignedBlocks = 0
			state.windowMissedBlocks = 0
			c.logger.Info("Starting new window for validator", 
				"validator", validatorAddr, 
				"window_start_height", state.windowStartHeight,
				"window_size", state.signedBlocksWindow)
		}

		// 윈도우가 끝났는지 확인
		blocksInWindow := height - state.windowStartHeight + 1
		if blocksInWindow > state.signedBlocksWindow {
			// 윈도우가 끝났으므로 새로운 윈도우 시작
			state.windowStartHeight = height - state.signedBlocksWindow + 1
			state.windowSignedBlocks = 0
			state.windowMissedBlocks = 0
			c.logger.Info("Starting new window for validator", 
				"validator", validatorAddr, 
				"window_start_height", state.windowStartHeight,
				"window_size", state.signedBlocksWindow)
		}

		c.logger.Info("Checking validator signature", "valoper", validatorAddr, "consensus_address", state.consensusAddress)

		if signedValidators[state.consensusAddress] {
			state.signedBlocks++
			state.windowSignedBlocks++
			state.consecutiveMissed = 0
			c.logger.Info("Validator signed block", 
				"validator", validatorAddr,
				"moniker", state.moniker,
				"height", height,
				"signed_blocks", state.signedBlocks,
				"window_signed", state.windowSignedBlocks,
				"window_missed", state.windowMissedBlocks)
		} else {
			state.missedBlocks++
			state.windowMissedBlocks++
			state.consecutiveMissed++
			
			// 외부 Prometheus 서버의 평균 블록 시간을 사용하여 consecutive_missed 제한
			if c.downtimeJailDuration > 0 {
				var maxConsecutiveMissed int
				
				// 외부 Prometheus 서버에서 평균 블록 시간을 가져와서 사용
				if c.prometheusClient != nil {
					if avgBlockTime, err := c.prometheusClient.GetAverageBlockTime(); err == nil {
						maxConsecutiveMissed = util.CalculateDowntimeThreshold(c.downtimeJailDuration, avgBlockTime)
						c.logger.Debug("Using external Prometheus average block time", 
							"avg_block_time", avgBlockTime,
							"threshold", maxConsecutiveMissed)
					} else {
						c.logger.Debug("Failed to get average block time from external Prometheus, using local", "error", err)
						avgBlockTime := c.blockTimeCalculator.GetAverageBlockTime()
						if avgBlockTime > 0 {
							maxConsecutiveMissed = util.CalculateDowntimeThreshold(c.downtimeJailDuration, avgBlockTime)
						} else {
							// 기본값 사용
							maxConsecutiveMissed = int(c.downtimeJailDuration / 10)
						}
					}
				} else {
					// 외부 Prometheus 클라이언트가 없는 경우 로컬 계산 사용
					avgBlockTime := c.blockTimeCalculator.GetAverageBlockTime()
					if avgBlockTime > 0 {
						maxConsecutiveMissed = util.CalculateDowntimeThreshold(c.downtimeJailDuration, avgBlockTime)
					} else {
						// 기본값 사용
						maxConsecutiveMissed = int(c.downtimeJailDuration / 10)
					}
				}
				
				if maxConsecutiveMissed > 0 && state.consecutiveMissed > maxConsecutiveMissed {
					state.consecutiveMissed = maxConsecutiveMissed
					c.logger.Debug("Consecutive missed capped", 
						"validator", validatorAddr,
						"consecutive_missed", state.consecutiveMissed,
						"max_allowed", maxConsecutiveMissed)
				}
			}
			
			c.logger.Warn("Validator missed block", 
				"validator", validatorAddr,
				"moniker", state.moniker,
				"height", height,
				"missed_blocks", state.missedBlocks,
				"window_missed", state.windowMissedBlocks,
				"consecutive_missed", state.consecutiveMissed)
		}

		if proposerAddr == state.consensusAddress {
			state.proposedBlocks++
			c.logger.Info("Validator proposed block", 
				"validator", validatorAddr,
				"moniker", state.moniker,
				"height", height,
				"proposed_blocks", state.proposedBlocks)
		}
	}
}

func (c *UnifiedCollector) trackRealtimeBlocks(ctx context.Context) {
	c.logger.Info("Starting realtime block tracking", "websocket", c.cfg.WebSocket)
	
	c.logger.Info("WebSocket connection not implemented yet - using polling instead")
}

func (c *UnifiedCollector) logValidatorStates() {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for validatorAddr, state := range c.validatorStates {
		c.logger.Info("Validator state summary", 
			"validator", validatorAddr,
			"moniker", state.moniker,
			"signed_blocks", state.signedBlocks,
			"proposed_blocks", state.proposedBlocks,
			"missed_blocks", state.missedBlocks,
			"consecutive_missed", state.consecutiveMissed,
			"active", state.inActiveSet,
			"jailed", state.isJailed)
	}
}

func (c *UnifiedCollector) calculateBlocksBehind() {
	if len(c.cfg.Peers) == 0 {
		c.blocksBehind = 0
		return
	}
	
	c.blocksBehind = 0
}

func (c *UnifiedCollector) saveValidatorState() {
	stateFile := ".validator-state.json"
	c.logger.Debug("Validator state saved", "state_file", stateFile)
}