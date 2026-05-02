package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Bot          BotConfig          `yaml:"bot"`
	Indexing     IndexingConfig     `yaml:"indexing"`
	LLM          LLMConfig          `yaml:"llm"`
	Search       SearchConfig       `yaml:"search"`
	ImageProc    ImageProcConfig    `yaml:"image_processing"`
	Retry        RetryConfig        `yaml:"retry"`
	StoragePath  string             `yaml:"storage_path"`
}

type BotConfig struct {
	Homeserver  string `yaml:"homeserver"`
	UserID      string `yaml:"user_id"`
	AccessToken string `yaml:"access_token"`
}

type IndexingConfig struct {
	BatchTimeout       time.Duration `yaml:"batch_timeout"`
	MaxBatchDelay      time.Duration `yaml:"max_batch_delay"`
	StartupGracePeriod time.Duration `yaml:"startup_grace_period"`
	Rooms              []string      `yaml:"rooms"`
	DelayedEmbedHour   int           `yaml:"delayed_embed_hour"`
	DelayedEmbedMinute int           `yaml:"delayed_embed_minute"`
}

type LLMConfig struct {
	BaseURL        string `yaml:"base_url"`
	APIKey         string `yaml:"api_key"`
	EmbeddingModel string `yaml:"embedding_model"`
	ImageModel     string `yaml:"image_model"`
	ImagePrompt    string `yaml:"image_prompt"`
	Timeout        time.Duration `yaml:"timeout"`
}

type SearchConfig struct {
	VectorDimensions int `yaml:"vector_dimensions"`
	ResultLimit      int `yaml:"result_limit"`
}

type ImageProcConfig struct {
	ConvertBinary string        `yaml:"convert_binary"`
	OutputFormat  string        `yaml:"output_format"`
	OutputQuality int           `yaml:"output_quality"`
	MaxLongSide   int           `yaml:"max_long_side"`
	CacheDir      string        `yaml:"cache_dir"`
	CacheTTL      time.Duration `yaml:"cache_ttl"`
}

type RetryConfig struct {
	InitialDelay time.Duration `yaml:"initial_delay"`
	MaxDelay     time.Duration `yaml:"max_delay"`
	Multiplier   float64       `yaml:"multiplier"`
	MaxRetries   int           `yaml:"max_retries"`
	Timeout      time.Duration `yaml:"timeout"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) Validate() error {
	if c.Bot.Homeserver == "" {
		return fmt.Errorf("bot.homeserver is required")
	}
	if c.Indexing.StartupGracePeriod == 0 {
		c.Indexing.StartupGracePeriod = 10 * time.Second
	}
	if c.Bot.UserID == "" {
		return fmt.Errorf("bot.user_id is required")
	}
	if c.Bot.AccessToken == "" {
		return fmt.Errorf("bot.access_token is required")
	}
	if c.LLM.BaseURL == "" {
		return fmt.Errorf("llm.base_url is required")
	}
	if c.LLM.EmbeddingModel == "" {
		return fmt.Errorf("llm.embedding_model is required")
	}
	if c.LLM.ImageModel == "" {
		return fmt.Errorf("llm.image_model is required")
	}
	if c.LLM.ImagePrompt == "" {
		return fmt.Errorf("llm.image_prompt is required")
	}
	if c.Indexing.BatchTimeout == 0 {
		c.Indexing.BatchTimeout = 60 * time.Second
	}
	if c.Indexing.MaxBatchDelay == 0 {
		c.Indexing.MaxBatchDelay = 10 * time.Minute
	}
	if c.Search.VectorDimensions == 0 {
		c.Search.VectorDimensions = 1536
	}
	if c.Search.ResultLimit == 0 {
		c.Search.ResultLimit = 5
	}
	if c.ImageProc.ConvertBinary == "" {
		c.ImageProc.ConvertBinary = "convert"
	}
	if c.ImageProc.OutputFormat == "" {
		c.ImageProc.OutputFormat = "jpg"
	}
	if c.ImageProc.OutputQuality == 0 {
		c.ImageProc.OutputQuality = 85
	}
	if c.ImageProc.CacheDir == "" {
		c.ImageProc.CacheDir = "./cache/images"
	}
	if c.ImageProc.CacheTTL == 0 {
		c.ImageProc.CacheTTL = 24 * time.Hour
	}
	if c.Retry.InitialDelay == 0 {
		c.Retry.InitialDelay = 1 * time.Second
	}
	if c.Retry.MaxDelay == 0 {
		c.Retry.MaxDelay = 5 * time.Minute
	}
	if c.Retry.Multiplier == 0 {
		c.Retry.Multiplier = 2.0
	}
	if c.Retry.Timeout == 0 {
		c.Retry.Timeout = 60 * time.Second
	}
	if c.StoragePath == "" {
		c.StoragePath = "./bot-data"
	}
	if c.Indexing.DelayedEmbedHour == 0 {
		c.Indexing.DelayedEmbedHour = 5
	}
	if c.Indexing.DelayedEmbedMinute == 0 {
		c.Indexing.DelayedEmbedMinute = 0
	}
	return nil
}
