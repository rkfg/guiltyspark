package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_valid_yaml(t *testing.T) {
	yaml := "bot:\n  homeserver: https://matrix.example.org\n  user_id: '@bot:example.org'\n  access_token: secret\nllm:\n  base_url: http://localhost:8080\n  embedding_model: text-embedding\n  image_model: vision-model\n  image_prompt: describe\nsearch:\n  vector_dimensions: 4096\n"
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	err := os.WriteFile(cfgPath, []byte(yaml), 0644)
	require.NoError(t, err)

	cfg, err := Load(cfgPath)
	require.NoError(t, err)
	assert.Equal(t, "https://matrix.example.org", cfg.Bot.Homeserver)
	assert.Equal(t, "@bot:example.org", cfg.Bot.UserID)
	assert.Equal(t, "secret", cfg.Bot.AccessToken)
	assert.Equal(t, 4096, cfg.Search.VectorDimensions)
}

func TestLoad_missing_file(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	require.Error(t, err)
}

func TestLoad_invalid_yaml(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	err := os.WriteFile(cfgPath, []byte("::: invalid yaml {{{"), 0644)
	require.NoError(t, err)

	_, err = Load(cfgPath)
	require.Error(t, err)
}

func TestLoad_empty_yaml(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	err := os.WriteFile(cfgPath, []byte(""), 0644)
	require.NoError(t, err)

	_, err = Load(cfgPath)
	require.Error(t, err)
}

func TestValidate_mode_bot_valid(t *testing.T) {
	cfg := &Config{
		Bot: BotConfig{
			Homeserver:  "https://matrix.example.org",
			UserID:      "@bot:example.org",
			AccessToken: "secret",
		},
		LLM: LLMConfig{
			BaseURL:        "http://localhost:8080",
			EmbeddingBaseURL: "http://localhost:8080",
			EmbeddingModel: "text-embedding",
			ImageModel:     "vision-model",
			ImagePrompt:    "describe",
		},
	}
	err := cfg.Validate(ModeBot)
	assert.NoError(t, err)
}

func TestValidate_mode_bot_missing_homeserver(t *testing.T) {
	cfg := &Config{
		Bot: BotConfig{
			UserID:      "@bot:example.org",
			AccessToken: "secret",
		},
		LLM: LLMConfig{
			BaseURL:        "http://localhost:8080",
			EmbeddingBaseURL: "http://localhost:8080",
			EmbeddingModel: "text-embedding",
			ImageModel:     "vision-model",
			ImagePrompt:    "describe",
		},
	}
	err := cfg.Validate(ModeBot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "homeserver")
}

func TestValidate_mode_bot_missing_user_id(t *testing.T) {
	cfg := &Config{
		Bot: BotConfig{
			Homeserver:  "https://matrix.example.org",
			AccessToken: "secret",
		},
		LLM: LLMConfig{
			BaseURL:        "http://localhost:8080",
			EmbeddingBaseURL: "http://localhost:8080",
			EmbeddingModel: "text-embedding",
			ImageModel:     "vision-model",
			ImagePrompt:    "describe",
		},
	}
	err := cfg.Validate(ModeBot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "user_id")
}

func TestValidate_mode_bot_missing_access_token(t *testing.T) {
	cfg := &Config{
		Bot: BotConfig{
			Homeserver: "https://matrix.example.org",
			UserID:     "@bot:example.org",
		},
		LLM: LLMConfig{
			BaseURL:        "http://localhost:8080",
			EmbeddingBaseURL: "http://localhost:8080",
			EmbeddingModel: "text-embedding",
			ImageModel:     "vision-model",
			ImagePrompt:    "describe",
		},
	}
	err := cfg.Validate(ModeBot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "access_token")
}

func TestValidate_mode_reembed_valid(t *testing.T) {
	cfg := &Config{
		LLM: LLMConfig{
			EmbeddingBaseURL: "http://localhost:8080",
			EmbeddingModel:   "text-embedding",
			ImageModel:       "vision-model",
			ImagePrompt:      "describe",
		},
	}
	err := cfg.Validate(ModeReembed)
	assert.NoError(t, err)
}

func TestValidate_mode_reembed_missing_llm(t *testing.T) {
	cfg := &Config{}
	err := cfg.Validate(ModeReembed)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "llm.base_url")
}

func TestValidate_llm_only_base_url(t *testing.T) {
	cfg := &Config{
		LLM: LLMConfig{
			BaseURL:        "http://localhost:8080",
			EmbeddingModel: "text-embedding",
			ImageModel:     "vision-model",
			ImagePrompt:    "describe",
		},
	}
	err := cfg.Validate(ModeReembed)
	assert.NoError(t, err)
	assert.Equal(t, "http://localhost:8080", cfg.LLM.EmbeddingBaseURL)
}

func TestValidate_llm_only_embedding_base_url(t *testing.T) {
	cfg := &Config{
		LLM: LLMConfig{
			EmbeddingBaseURL: "http://localhost:8081",
			EmbeddingModel:   "text-embedding",
			ImageModel:       "vision-model",
			ImagePrompt:      "describe",
		},
	}
	err := cfg.Validate(ModeReembed)
	assert.NoError(t, err)
}

func TestValidate_llm_both_empty(t *testing.T) {
	cfg := &Config{
		LLM: LLMConfig{
			EmbeddingModel: "text-embedding",
			ImageModel:     "vision-model",
			ImagePrompt:    "describe",
		},
	}
	err := cfg.Validate(ModeReembed)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "llm.base_url")
}

func TestValidate_default_values(t *testing.T) {
	cfg := &Config{
		Bot: BotConfig{
			Homeserver:  "https://matrix.example.org",
			UserID:      "@bot:example.org",
			AccessToken: "secret",
		},
		LLM: LLMConfig{
			BaseURL:        "http://localhost:8080",
			EmbeddingBaseURL: "http://localhost:8080",
			EmbeddingModel: "text-embedding",
			ImageModel:     "vision-model",
			ImagePrompt:    "describe",
		},
	}
	err := cfg.Validate(ModeBot)
	require.NoError(t, err)
	assert.Equal(t, 768, cfg.Search.VectorDimensions)
	assert.Equal(t, "ru", cfg.Search.Analyzer)
	assert.Equal(t, 5, cfg.Search.ResultLimit)
}

func TestValidate_room_cutoff_valid(t *testing.T) {
	cfg := &Config{
		Bot: BotConfig{
			Homeserver:  "https://matrix.example.org",
			UserID:      "@bot:example.org",
			AccessToken: "secret",
		},
		LLM: LLMConfig{
			BaseURL:        "http://localhost:8080",
			EmbeddingBaseURL: "http://localhost:8080",
			EmbeddingModel: "text-embedding",
			ImageModel:     "vision-model",
			ImagePrompt:    "describe",
		},
		Rooms: map[string]*RoomConfig{
			"#room:server.org": {HistoryScanCutoff: "01.02.2006"},
		},
	}
	err := cfg.Validate(ModeBot)
	assert.NoError(t, err)
	// "01.02.2006" in format "02.01.2006" means Feb 1, 2006 = Unix 1138752000
	assert.Equal(t, int64(1138752000), cfg.Rooms["#room:server.org"].ScanCutoffUnix)
}

func TestValidate_room_cutoff_invalid_format(t *testing.T) {
	cfg := &Config{
		Bot: BotConfig{
			Homeserver:  "https://matrix.example.org",
			UserID:      "@bot:example.org",
			AccessToken: "secret",
		},
		LLM: LLMConfig{
			BaseURL:        "http://localhost:8080",
			EmbeddingBaseURL: "http://localhost:8080",
			EmbeddingModel: "text-embedding",
			ImageModel:     "vision-model",
			ImagePrompt:    "describe",
		},
		Rooms: map[string]*RoomConfig{
			"#room:server.org": {HistoryScanCutoff: "01.02.06"},
		},
	}
	err := cfg.Validate(ModeBot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid history_scan_cutoff")
}

func TestValidate_room_cutoff_wrong_format(t *testing.T) {
	cfg := &Config{
		Bot: BotConfig{
			Homeserver:  "https://matrix.example.org",
			UserID:      "@bot:example.org",
			AccessToken: "secret",
		},
		LLM: LLMConfig{
			BaseURL:        "http://localhost:8080",
			EmbeddingBaseURL: "http://localhost:8080",
			EmbeddingModel: "text-embedding",
			ImageModel:     "vision-model",
			ImagePrompt:    "describe",
		},
		Rooms: map[string]*RoomConfig{
			"#room:server.org": {HistoryScanCutoff: "2006.02.01"},
		},
	}
	err := cfg.Validate(ModeBot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid history_scan_cutoff")
}

func TestValidate_room_cutoff_empty_string(t *testing.T) {
	cfg := &Config{
		Bot: BotConfig{
			Homeserver:  "https://matrix.example.org",
			UserID:      "@bot:example.org",
			AccessToken: "secret",
		},
		LLM: LLMConfig{
			BaseURL:        "http://localhost:8080",
			EmbeddingBaseURL: "http://localhost:8080",
			EmbeddingModel: "text-embedding",
			ImageModel:     "vision-model",
			ImagePrompt:    "describe",
		},
		Rooms: map[string]*RoomConfig{
			"#room:server.org": {HistoryScanCutoff: ""},
		},
	}
	err := cfg.Validate(ModeBot)
	assert.NoError(t, err)
}
