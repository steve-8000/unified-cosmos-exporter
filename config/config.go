package config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Config struct {
	ListenAddress   string         `yaml:"listen_address"`
	MetricsInterval int            `yaml:"metrics_interval"`
	BlockTracking   BlockTracking  `yaml:"block_tracking"`
	Chains          []Chain        `yaml:"chains"`
	Logging         Logging        `yaml:"logging"`
	Prometheus      Prometheus     `yaml:"prometheus"`
}

type BlockTracking struct {
	Enabled                 bool `yaml:"enabled"`
	Interval               int  `yaml:"interval"`
	MaxConsecutiveMissed  int  `yaml:"max_consecutive_missed"`
}

type Chain struct {
	ChainID          string   `yaml:"chain_id"`
	Name             string   `yaml:"name"`
	RPC              string   `yaml:"rpc"`
	API              string   `yaml:"api"`
	WebSocket        string   `yaml:"websocket"`
	AccountPrefix    string   `yaml:"account_prefix"`
	ValidatorPrefix  string   `yaml:"validator_prefix"`
	ConsensusPrefix  string   `yaml:"consensus_prefix"`
	TokenBase        string   `yaml:"token_base"`
	TokenDisplay     string   `yaml:"token_display"`
	TokenDecimals    int      `yaml:"token_decimals"`
	AutoDetect       bool     `yaml:"auto_detect"`
	Validators       []string `yaml:"validators"`
	Wallets          []Wallet `yaml:"wallets"`
	Peers            []string `yaml:"peers"`
}

type Wallet struct {
	Address string `yaml:"address"`
	Name    string `yaml:"name"`
}

type Logging struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type Prometheus struct {
	Server  string   `yaml:"server"`
	Metrics []string `yaml:"metrics"`
}

func LoadConfig(filename string) (*Config, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}