package indexer

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/rkfg/guiltyspark/config"
	"github.com/rkfg/guiltyspark/embedding"
)

type ImageProcessor struct {
	cfg         *config.ImageProcConfig
	embedClient *embedding.Client
	homeserver  string
	accessToken string
}

func NewImageProcessor(cfg *config.ImageProcConfig, embedClient *embedding.Client) *ImageProcessor {
	return &ImageProcessor{
		cfg:         cfg,
		embedClient: embedClient,
	}
}

func (p *ImageProcessor) SetHomeserver(homeserver, accessToken string) {
	p.homeserver = homeserver
	p.accessToken = accessToken
}

func (p *ImageProcessor) DescribeImageOnly(mxcURL string, timestamp int64) (string, error) {
	imgData, err := p.downloadImage(mxcURL)
	if err != nil {
		return "", fmt.Errorf("download image: %w", err)
	}

	tmpDir := p.cfg.CacheDir
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return "", fmt.Errorf("create cache dir: %w", err)
	}

	tmpFile := filepath.Join(tmpDir, fmt.Sprintf("img_%d.jpg", timestamp))

	if err := p.convertImage(imgData, tmpFile); err != nil {
		return "", fmt.Errorf("convert image: %w", err)
	}

	base64Data, encodedMime, err := embedding.ReadAndEncodeImage(tmpFile)
	if err != nil {
		os.Remove(tmpFile)
		return "", fmt.Errorf("encode image: %w", err)
	}

	description, err := p.embedClient.DescribeImage(base64Data, encodedMime)
	os.Remove(tmpFile)
	if err != nil {
		return "", fmt.Errorf("describe image: %w", err)
	}

	return description, nil
}

func (p *ImageProcessor) downloadImage(mxcURL string) ([]byte, error) {
	if p.homeserver == "" || p.accessToken == "" {
		return nil, fmt.Errorf("homeserver and access token not set for image download")
	}

	// Parse mxc:// URL
	// Format: mxc://server.name/id
	mxcPrefix := "mxc://"
	if !strings.HasPrefix(mxcURL, mxcPrefix) {
		return nil, fmt.Errorf("invalid MXC URL format: %s", mxcURL)
	}

	mxcPath := strings.TrimPrefix(mxcURL, mxcPrefix)
	parts := strings.SplitN(mxcPath, "/", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid MXC URL format: %s", mxcURL)
	}

	serverName := parts[0]
	imageID := parts[1]

	// Use homeserver URL for download (not the server name from MXC)
	// Ensure homeserver has protocol prefix
	homeServer := p.homeserver
	if !strings.HasPrefix(homeServer, "http://") && !strings.HasPrefix(homeServer, "https://") {
		homeServer = "https://" + homeServer
	}
	// Use /client/v1/media/download/ endpoint (newer Matrix spec)
	downloadURL := fmt.Sprintf("%s/_matrix/client/v1/media/download/%s/%s", homeServer, url.PathEscape(serverName), url.PathEscape(imageID))

	req, err := http.NewRequest("GET", downloadURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.accessToken)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download failed with status: %s", resp.Status)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	return data, nil
}

func (p *ImageProcessor) convertImage(data []byte, outputPath string) error {
	inputFile := outputPath + ".input"
	if err := os.WriteFile(inputFile, data, 0644); err != nil {
		return err
	}
	defer os.Remove(inputFile)

	args := []string{
		inputFile + "[0]",
		"-strip", "-interlace", "Plane", "-colorspace", "sRGB",
		"-quality", fmt.Sprintf("%d", p.cfg.OutputQuality),
	}

	if p.cfg.MaxLongSide > 0 {
		// Use -resize with geometry string in quotes to prevent shell interpretation
		args = append(args, "-resize", fmt.Sprintf("%dx%d>", p.cfg.MaxLongSide, p.cfg.MaxLongSide))
	}

	args = append(args, outputPath)

	cmd := exec.Command(p.cfg.ConvertBinary, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		os.Remove(outputPath)
		return fmt.Errorf("convert: %s: %w", string(output), err)
	}

	return nil
}
