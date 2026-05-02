package embedding

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rkfg/guiltyspark/config"
	"github.com/rkfg/guiltyspark/retry"
)

type Client struct {
	httpClient     *http.Client
	baseURL        string
	apiKey         string
	model          string
	imageModel     string
	imagePrompt    string
	requestTimeout time.Duration
	backoff        *retry.BackoffManager
	lastImageDesc  string
}

func NewClient(cfg *config.LLMConfig, retryCfg retry.BackoffConfig, timeout time.Duration) *Client {
	return &Client{
		httpClient:     &http.Client{Timeout: timeout},
		baseURL:        cfg.BaseURL,
		apiKey:         cfg.APIKey,
		model:          cfg.EmbeddingModel,
		imageModel:     cfg.ImageModel,
		imagePrompt:    cfg.ImagePrompt,
		requestTimeout: timeout,
		backoff:        retry.NewBackoffManager(retryCfg),
	}
}

type embeddingResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

func (c *Client) CreateEmbedding(text string) ([]float32, error) {
	var result embeddingResponse
	var lastErr error

	for attempt := 0; c.backoff.ShouldRetry(attempt); attempt++ {
		if attempt > 0 {
			delay := c.backoff.GetDelay(attempt)
			log.Printf("DEBUG embedding: retry attempt %d after %v", attempt, delay)
			time.Sleep(delay)
		}

		result, lastErr = c.doEmbedding(text)
		if lastErr == nil {
			return result.Data[0].Embedding, nil
		}
		log.Printf("WARN embedding: attempt %d failed: %v", attempt, lastErr)
	}

	return nil, fmt.Errorf("embedding failed after retries: %w", lastErr)
}

func (c *Client) doEmbedding(text string) (embeddingResponse, error) {
	var result embeddingResponse

	payload := map[string]any{
		"model": c.model,
		"input": []string{text},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return result, fmt.Errorf("marshal embedding request: %w", err)
	}

	req, err := http.NewRequest("POST", c.baseURL+"/embeddings", bytes.NewReader(body))
	if err != nil {
		return result, fmt.Errorf("create embedding request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return result, fmt.Errorf("embedding request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return result, fmt.Errorf("embedding API error %d: %s", resp.StatusCode, string(respBody))
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return result, fmt.Errorf("decode embedding response: %w", err)
	}

	if len(result.Data) == 0 || len(result.Data[0].Embedding) == 0 {
		return result, fmt.Errorf("empty embedding response")
	}

	return result, nil
}

type chatMessage struct {
	Role    string        `json:"role"`
	Content []contentItem `json:"content"`
}

type contentItem struct {
	Type     string  `json:"type"`
	Text     string  `json:"text,omitempty"`
	ImageURL *imgURL `json:"image_url,omitempty"`
}

type imgURL struct {
	URL string `json:"url"`
}

type chatRequest struct {
	Model        string        `json:"model"`
	Messages     []chatMessage `json:"messages"`
	MaxTokens    int           `json:"max_tokens"`
	Temperature  float32       `json:"temperature"`
}

type chatChoice struct {
	Message chatMessage `json:"message"`
}

type chatResponse struct {
	Choices []chatChoice `json:"choices"`
}

// chatResponseString is for models that return content as a string
type chatResponseString struct {
	Choices []struct {
		Message chatMessageString `json:"message"`
	} `json:"choices"`
}

// chatMessageString is for models that return content as a string
type chatMessageString struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func (c *Client) DescribeImage(base64Data string, mimeType string) (string, error) {
	var lastErr error

	for attempt := 0; c.backoff.ShouldRetry(attempt); attempt++ {
		if attempt > 0 {
			delay := c.backoff.GetDelay(attempt)
			log.Printf("DEBUG embedding: retry DescribeImage attempt %d after %v", attempt, delay)
			time.Sleep(delay)
		}

		lastErr = c.doDescribeImage(base64Data, mimeType)
		if lastErr == nil {
			return c.lastImageDesc, nil
		}
		log.Printf("WARN embedding: DescribeImage attempt %d failed: %v", attempt, lastErr)
	}

	return "", fmt.Errorf("DescribeImage failed after retries: %w", lastErr)
}

func (c *Client) doDescribeImage(base64Data string, mimeType string) error {
	dataURI := fmt.Sprintf("data:%s;base64,%s", mimeType, base64Data)

	payload := chatRequest{
		Model: c.imageModel,
		Messages: []chatMessage{
			{
				Role: "user",
				Content: []contentItem{
					{Type: "text", Text: c.imagePrompt},
					{Type: "image_url", ImageURL: &imgURL{URL: dataURI}},
				},
			},
		},
		Temperature: 0.6,
		MaxTokens:   32768,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal image request: %w", err)
	}

	req, err := http.NewRequest("POST", c.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create image request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("image request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("image API error %d: %s", resp.StatusCode, string(respBody))
	}

	// Read response body for parsing
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	// Try parsing as array first
	var result chatResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		// Fallback: try parsing as string content
		var resultStr chatResponseString
		if err2 := json.Unmarshal(respBody, &resultStr); err2 != nil {
			return fmt.Errorf("decode image response (array): %w, (string): %w", err, err2)
		}
		if len(resultStr.Choices) == 0 {
			return fmt.Errorf("empty image response (string)")
		}
		c.lastImageDesc = resultStr.Choices[0].Message.Content
		return nil
	}

	if len(result.Choices) == 0 {
		return fmt.Errorf("empty image response")
	}

	// Concatenate all text content items
	var parts []string
	for _, item := range result.Choices[0].Message.Content {
		if item.Type == "text" {
			parts = append(parts, item.Text)
		}
	}
	c.lastImageDesc = strings.Join(parts, "\n")
	return nil
}

func ReadAndEncodeImage(path string) (string, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", "", fmt.Errorf("read image: %w", err)
	}

	mimeType := http.DetectContentType(data)
	encoded := base64.StdEncoding.EncodeToString(data)

	return encoded, mimeType, nil
}
