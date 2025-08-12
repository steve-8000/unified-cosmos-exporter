package rpc

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type Client struct {
	rpcURL  string
	apiURL  string
	wsURL   string
}

func NewClient(rpcURL, apiURL, wsURL string) *Client {
	return &Client{
		rpcURL: rpcURL,
		apiURL: apiURL,
		wsURL:  wsURL,
	}
}

func (c *Client) get(url string, v interface{}) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return json.NewDecoder(resp.Body).Decode(v)
}

type StakingPoolResponse struct {
	Pool struct {
		BondedTokens    string `json:"bonded_tokens"`
		NotBondedTokens string `json:"not_bonded_tokens"`
	} `json:"pool"`
}

func (c *Client) GetStakingPool() (*StakingPoolResponse, error) {
	var res StakingPoolResponse
	err := c.get(c.apiURL+"/cosmos/staking/v1beta1/pool", &res)
	return &res, err
}

type CommunityPoolResponse struct {
	Pool []struct {
		Amount string `json:"amount"`
		Denom  string `json:"denom"`
	} `json:"pool"`
}

func (c *Client) GetCommunityPool() (*CommunityPoolResponse, error) {
	var res CommunityPoolResponse
	err := c.get(c.apiURL+"/cosmos/distribution/v1beta1/community_pool", &res)
	return &res, err
}

type BankSupplyResponse struct {
	Supply []struct {
		Amount string `json:"amount"`
		Denom  string `json:"denom"`
	} `json:"supply"`
}

func (c *Client) GetBankSupply() (*BankSupplyResponse, error) {
	var res BankSupplyResponse
	err := c.get(c.apiURL+"/cosmos/bank/v1beta1/supply", &res)
	return &res, err
}

type MintingInflationResponse struct {
	Inflation string `json:"inflation"`
}

func (c *Client) GetMintingInflation() (*MintingInflationResponse, error) {
	var res MintingInflationResponse
	err := c.get(c.apiURL+"/cosmos/mint/v1beta1/inflation", &res)
	return &res, err
}

type MintingAnnualProvisionsResponse struct {
	AnnualProvisions string `json:"annual_provisions"`
}

func (c *Client) GetMintingAnnualProvisions() (*MintingAnnualProvisionsResponse, error) {
	var res MintingAnnualProvisionsResponse
	err := c.get(c.apiURL+"/cosmos/mint/v1beta1/annual_provisions", &res)
	return &res, err
}

type ValidatorsResponse struct {
	Validators []struct {
		OperatorAddress   string `json:"operator_address"`
		ConsensusPubkey   struct {
			Key string `json:"key"`
		} `json:"consensus_pubkey"`
		Jailed            bool   `json:"jailed"`
		Status            string `json:"status"`
		Tokens            string `json:"tokens"`
		DelegatorShares  string `json:"delegator_shares"`
		Description       struct {
			Moniker string `json:"moniker"`
		} `json:"description"`
		Commission struct {
			CommissionRates struct {
				Rate string `json:"rate"`
			} `json:"commission_rates"`
		} `json:"commission"`
		ConsensusAddress string `json:"consensus_address"`
	} `json:"validators"`
}

func (c *Client) GetValidators() (*ValidatorsResponse, error) {
	var res ValidatorsResponse
	err := c.get(c.apiURL+"/cosmos/staking/v1beta1/validators?pagination.limit=1000", &res)
	return &res, err
}

type SigningInfosResponse struct {
	Info []struct {
		Address             string `json:"address"`
		MissedBlocksCounter string `json:"missed_blocks_counter"`
	} `json:"info"`
}

func (c *Client) GetSigningInfos() (*SigningInfosResponse, error) {
	var res SigningInfosResponse
	err := c.get(c.apiURL+"/cosmos/slashing/v1beta1/signing_infos?pagination.limit=1000", &res)
	return &res, err
}

type ValidatorCommissionResponse struct {
	Commission struct {
		Commission []struct {
			Amount string `json:"amount"`
			Denom  string `json:"denom"`
		} `json:"commission"`
	} `json:"commission"`
}

func (c *Client) GetValidatorCommission(validatorAddress string) (*ValidatorCommissionResponse, error) {
	var res ValidatorCommissionResponse
	err := c.get(c.apiURL+"/cosmos/distribution/v1beta1/validators/"+validatorAddress+"/commission", &res)
	return &res, err
}

type ValidatorRewardsResponse struct {
	Rewards struct {
		Rewards []struct {
			Amount string `json:"amount"`
			Denom  string `json:"denom"`
		} `json:"rewards"`
	} `json:"rewards"`
}

func (c *Client) GetValidatorRewards(validatorAddress string) (*ValidatorRewardsResponse, error) {
	var res ValidatorRewardsResponse
	err := c.get(c.apiURL+"/cosmos/distribution/v1beta1/validators/"+validatorAddress+"/rewards", &res)
	return &res, err
}

type WalletBalanceResponse struct {
	Balances []struct {
		Amount string `json:"amount"`
		Denom  string `json:"denom"`
	} `json:"balances"`
}

func (c *Client) GetWalletBalance(address string) (*WalletBalanceResponse, error) {
	var res WalletBalanceResponse
	err := c.get(c.apiURL+"/cosmos/bank/v1beta1/balances/"+address, &res)
	return &res, err
}

type WalletDelegationsResponse struct {
	DelegationResponses []struct {
		Delegation struct {
			ValidatorAddress string `json:"validator_address"`
		} `json:"delegation"`
		Balance struct {
			Amount string `json:"amount"`
			Denom  string `json:"denom"`
		} `json:"balance"`
	} `json:"delegation_responses"`
}

func (c *Client) GetWalletDelegations(address string) (*WalletDelegationsResponse, error) {
	var res WalletDelegationsResponse
	err := c.get(c.apiURL+"/cosmos/staking/v1beta1/delegations/"+address, &res)
	return &res, err
}

type WalletRewardsResponse struct {
	Rewards []struct {
		ValidatorAddress string `json:"validator_address"`
		Reward           []Coin `json:"reward"`
	} `json:"rewards"`
}

func (c *Client) GetWalletRewards(address string) (*WalletRewardsResponse, error) {
	var res WalletRewardsResponse
	err := c.get(c.apiURL+"/cosmos/distribution/v1beta1/delegators/"+address+"/rewards", &res)
	return &res, err
}

type WalletUnbondingResponse struct {
	UnbondingResponses []struct {
		ValidatorAddress string `json:"validator_address"`
		Entries          []struct {
			Balance  string `json:"balance"`
			CompletionTime string `json:"completion_time"`
		} `json:"entries"`
	} `json:"unbonding_responses"`
}

func (c *Client) GetWalletUnbonding(address string) (*WalletUnbondingResponse, error) {
	var res WalletUnbondingResponse
	err := c.get(c.apiURL+"/cosmos/staking/v1beta1/delegators/"+address+"/unbonding_delegations?pagination.limit=1000", &res)
	return &res, err
}

type ChainConfigResponse struct {
	ChainConfig struct {
		Bech32Prefix struct {
			Account   string `json:"account"`
			Validator string `json:"validator"`
			Consensus string `json:"consensus"`
		} `json:"bech32_prefix"`
		TokenDenom struct {
			Base     string `json:"base"`
			Display  string `json:"display"`
			Decimals int    `json:"decimals"`
		} `json:"token_denom"`
	} `json:"chain_config"`
}

func (c *Client) GetChainConfig() (*ChainConfigResponse, error) {
	var res ChainConfigResponse
	err := c.get(c.apiURL+"/cosmos/chain_config", &res)
	return &res, err
}

type NodeInfoResponse struct {
	DefaultNodeInfo struct {
		Network string `json:"network"`
	} `json:"default_node_info"`
}

func (c *Client) GetNodeInfo() (*NodeInfoResponse, error) {
	var res NodeInfoResponse
	err := c.get(c.apiURL+"/cosmos/base/tendermint/v1beta1/node_info", &res)
	return &res, err
}

type StakingParamsResponse struct {
	Params struct {
		MaxValidators int `json:"max_validators"`
	} `json:"params"`
}

func (c *Client) GetStakingParams() (*StakingParamsResponse, error) {
	var res StakingParamsResponse
	err := c.get(c.apiURL+"/cosmos/staking/v1beta1/params", &res)
	return &res, err
}

type DistributionParamsResponse struct {
	Params struct {
		BaseProposerReward    string `json:"base_proposer_reward"`
		BonusProposerReward   string `json:"bonus_proposer_reward"`
	} `json:"params"`
}

func (c *Client) GetDistributionParams() (*DistributionParamsResponse, error) {
	var res DistributionParamsResponse
	err := c.get(c.apiURL+"/cosmos/distribution/v1beta1/params", &res)
	return &res, err
}

type GovernanceProposalsResponse struct {
	Proposals []struct {
		ProposalID string `json:"proposal_id"`
		Status     string `json:"status"`
		Content    struct {
			Type string `json:"@type"`
		} `json:"content"`
	} `json:"proposals"`
}

func (c *Client) GetGovernanceProposals() (*GovernanceProposalsResponse, error) {
	var res GovernanceProposalsResponse
	err := c.get(c.apiURL+"/cosmos/gov/v1beta1/proposals", &res)
	return &res, err
}

type SlashingParamsResponse struct {
	Params struct {
		SignedBlocksWindow      string `json:"signed_blocks_window"`
		MinSignedPerWindow      string `json:"min_signed_per_window"`
		DowntimeJailDuration    string `json:"downtime_jail_duration"`
		SlashFractionDoubleSign string `json:"slash_fraction_double_sign"`
		SlashFractionDowntime   string `json:"slash_fraction_downtime"`
	} `json:"params"`
}

func (c *Client) GetSlashingParams() (*SlashingParamsResponse, error) {
	var res SlashingParamsResponse
	err := c.get(c.apiURL+"/cosmos/slashing/v1beta1/params", &res)
	return &res, err
}

type Coin struct {
	Amount string `json:"amount"`
	Denom  string `json:"denom"`
}

type StatusResponse struct {
	Result struct {
		SyncInfo struct {
			LatestBlockHeight string `json:"latest_block_height"`
		} `json:"sync_info"`
	} `json:"result"`
}

func (c *Client) GetStatus() (*StatusResponse, error) {
	var res StatusResponse
	err := c.get(c.rpcURL+"/status", &res)
	return &res, err
}

type BlockResponse struct {
	Result struct {
		Block struct {
			Header struct {
				Height         string `json:"height"`
				ProposerAddress string `json:"proposer_address"`
				ChainID        string `json:"chain_id"`
				Time           string `json:"time"`
			} `json:"header"`
			LastCommit struct {
				Signatures []struct {
					ValidatorAddress string `json:"validator_address"`
				} `json:"signatures"`
			} `json:"last_commit"`
		} `json:"block"`
	} `json:"result"`
}

func (c *Client) GetBlock(height int) (*BlockResponse, error) {
	var res BlockResponse
	url := c.rpcURL + "/block"
	if height > 0 {
		url = fmt.Sprintf("%s?height=%d", url, height)
	}
	err := c.get(url, &res)
	return &res, err
}

func (c *Client) GetLatestBlock() (*BlockResponse, error) {
	return c.GetBlock(0)
}