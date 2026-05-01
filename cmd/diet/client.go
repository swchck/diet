package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// apiClient is a thin Directus REST client with token-refresh support.
// Concurrency controls how many goroutines pull or push in parallel; the
// underlying http.Transport is sized to match so we don't fight ourselves
// for connections.
type apiClient struct {
	baseURL     string
	token       string
	email       string
	password    string
	lastAuth    time.Time
	mu          sync.RWMutex
	http        *http.Client
	Concurrency int // parallel workers for data pull/insert
	BatchSize   int // items per batch POST
	RetryPasses int // max retry passes for FK failures
}

// clientOptions tunes the HTTP client and worker pool. Zero means "use
// the package default" (see newClientWithOptions).
type clientOptions struct {
	Timeout     int // seconds, 0 = default (60)
	Concurrency int // 0 = default (6)
	BatchSize   int // 0 = default (100)
	RetryPasses int // 0 = default (5)
}

func newClient(baseURL, token string) *apiClient {
	return newClientWithOptions(baseURL, token, clientOptions{})
}

func newClientWithOptions(baseURL, token string, opts clientOptions) *apiClient {
	timeout := 60
	if opts.Timeout > 0 {
		timeout = opts.Timeout
	}
	concurrency := 6
	if opts.Concurrency > 0 {
		concurrency = opts.Concurrency
	}
	batchSize := 100
	if opts.BatchSize > 0 {
		batchSize = opts.BatchSize
	}
	retryPasses := 5
	if opts.RetryPasses > 0 {
		retryPasses = opts.RetryPasses
	}
	return &apiClient{
		baseURL:     baseURL,
		token:       token,
		lastAuth:    time.Now(),
		Concurrency: concurrency,
		BatchSize:   batchSize,
		RetryPasses: retryPasses,
		http: &http.Client{
			Timeout: time.Duration(timeout) * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        concurrency * 3,
				MaxIdleConnsPerHost: concurrency * 3,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (c *apiClient) getToken() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.token
}

// refreshToken re-authenticates if the token is older than 10 minutes.
// Directus access tokens default to a 15-minute TTL — refreshing 5 minutes
// early keeps long imports from failing mid-stream on a token edge.
// No-op when email/password aren't supplied (static-token mode).
func (c *apiClient) refreshToken() {
	if c.email == "" || c.password == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Since(c.lastAuth) < 10*time.Minute {
		return
	}
	body, _ := json.Marshal(map[string]string{"email": c.email, "password": c.password})
	resp, err := c.http.Post(c.baseURL+"/auth/login", "application/json", bytes.NewReader(body))
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var result struct {
		Data struct {
			AccessToken string `json:"access_token"`
		} `json:"data"`
	}
	respBody, _ := io.ReadAll(resp.Body)
	if json.Unmarshal(respBody, &result) == nil && result.Data.AccessToken != "" {
		c.token = result.Data.AccessToken
		c.lastAuth = time.Now()
	}
}

func (c *apiClient) get(path string) ([]byte, error) {
	req, err := http.NewRequest("GET", c.baseURL+path, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", path, err)
	}
	req.Header.Set("Authorization", "Bearer "+c.getToken())
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("GET %s: HTTP %d: %s", path, resp.StatusCode, truncate(string(body), 200))
	}
	return body, nil
}

func (c *apiClient) getJSON(path string, out any) error {
	body, err := c.get(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, out)
}

func (c *apiClient) post(path string, data []byte) ([]byte, int, error) {
	c.refreshToken()
	req, err := http.NewRequest("POST", c.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.getToken())
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return body, resp.StatusCode, nil
}

func (c *apiClient) postJSON(path string, payload any) ([]byte, int, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, 0, err
	}
	return c.post(path, data)
}

func (c *apiClient) patch(path string, payload any) error {
	c.refreshToken()
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	req, err := http.NewRequest("PATCH", c.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.getToken())
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("PATCH %s: HTTP %d: %s", path, resp.StatusCode, truncate(string(body), 200))
	}
	return nil
}

func (c *apiClient) del(path string) (int, error) {
	c.refreshToken()
	req, err := http.NewRequest("DELETE", c.baseURL+path, http.NoBody)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.getToken())
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, nil
}

// pullAllItems fetches all items with offset-based pagination.
func (c *apiClient) pullAllItems(collection string) ([]json.RawMessage, error) {
	return c.pullPaginated("/items/" + url.PathEscape(collection))
}

// pullPaginated fetches all items from a paginated endpoint. On mid-stream
// errors it returns whatever was collected before the failure rather than
// an empty slice — large exports shouldn't lose 10k previously-fetched rows
// because page 47 hit a transient 5xx. Caller logs the partial outcome.
func (c *apiClient) pullPaginated(basePath string) ([]json.RawMessage, error) {
	var all []json.RawMessage
	limit := 500
	offset := 0

	for {
		path := fmt.Sprintf("%s?limit=%d&offset=%d", basePath, limit, offset)
		body, err := c.get(path)
		if err != nil {
			if len(all) > 0 {
				break
			}
			return nil, err
		}
		var resp struct {
			Data []json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			if len(all) > 0 {
				break
			}
			return nil, fmt.Errorf("parse %s response: %w", basePath, err)
		}
		if len(resp.Data) == 0 {
			break
		}
		all = append(all, resp.Data...)
		if len(resp.Data) < limit {
			break
		}
		offset += limit
	}
	return all, nil
}
