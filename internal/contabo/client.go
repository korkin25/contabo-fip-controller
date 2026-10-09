// Package contabo is a minimal client for the part of the Contabo API the
// controller needs: OAuth2 password-grant tokens and VIP (floating IP)
// lookup and assignment.
package contabo

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	DefaultAPIURL  = "https://api.contabo.com"
	DefaultAuthURL = "https://auth.contabo.com/auth/realms/contabo/protocol/openid-connect/token"

	// tokenSkew renews a token this long before it expires.
	tokenSkew = 30 * time.Second
)

// ErrNotFound is returned when the account has no VIP with the given address.
var ErrNotFound = errors.New("vip not found")

// Credentials are the four values the Contabo password grant needs.
type Credentials struct {
	ClientID     string
	ClientSecret string
	Username     string
	Password     string
}

// APIError is a non-2xx answer of the Contabo API.
type APIError struct {
	Method string
	Path   string
	Status int
	Body   string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("contabo %s %s: HTTP %d: %s", e.Method, e.Path, e.Status, e.Body)
}

// VIP is the subset of a Contabo VIP the controller reads.
type VIP struct {
	IP           string
	Type         string // "floating" or "additional"
	DataCenter   string
	ResourceType string // "instances", "bare-metal" or "null"
	ResourceID   string
	ResourceName string
}

// InstanceID returns the instance the VIP is assigned to, or "" when it is
// unassigned or assigned to something other than a VPS/VDS instance.
func (v VIP) InstanceID() string {
	if v.ResourceType != "instances" {
		return ""
	}
	return v.ResourceID
}

// Client talks to the Contabo API. It is safe for concurrent use.
type Client struct {
	APIURL  string
	AuthURL string
	Creds   Credentials
	HTTP    *http.Client

	mu     sync.Mutex
	token  string
	expiry time.Time
	now    func() time.Time
}

// New returns a client for the public Contabo endpoints.
func New(creds Credentials) *Client {
	return &Client{
		APIURL:  DefaultAPIURL,
		AuthURL: DefaultAuthURL,
		Creds:   creds,
		HTTP:    &http.Client{Timeout: 20 * time.Second},
		now:     time.Now,
	}
}

// GetVIP returns the VIP with the given address.
func (c *Client) GetVIP(ctx context.Context, ip string) (VIP, error) {
	var resp struct {
		Data []struct {
			Type         string `json:"type"`
			DataCenter   string `json:"dataCenter"`
			ResourceType string `json:"resourceType"`
			ResourceID   string `json:"resourceId"`
			ResourceName string `json:"resourceName"`
			V4           struct {
				IP string `json:"ip"`
			} `json:"v4"`
		} `json:"data"`
	}
	if err := c.do(ctx, http.MethodGet, "/v1/vips/"+url.PathEscape(ip), &resp); err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound {
			return VIP{}, ErrNotFound
		}
		return VIP{}, err
	}
	if len(resp.Data) == 0 {
		return VIP{}, ErrNotFound
	}
	d := resp.Data[0]
	v := VIP{
		IP:           d.V4.IP,
		Type:         d.Type,
		DataCenter:   d.DataCenter,
		ResourceType: d.ResourceType,
		ResourceID:   d.ResourceID,
		ResourceName: d.ResourceName,
	}
	if v.IP == "" {
		v.IP = ip
	}
	return v, nil
}

// Assign assigns the VIP to the instance.
func (c *Client) Assign(ctx context.Context, ip, instanceID string) error {
	return c.do(ctx, http.MethodPost, vipInstancePath(ip, instanceID), nil)
}

// Unassign removes the VIP from the instance.
func (c *Client) Unassign(ctx context.Context, ip, instanceID string) error {
	return c.do(ctx, http.MethodDelete, vipInstancePath(ip, instanceID), nil)
}

func vipInstancePath(ip, instanceID string) string {
	return "/v1/vips/" + url.PathEscape(ip) + "/instances/" + url.PathEscape(instanceID)
}

// do sends one API request and decodes a JSON answer into out when out is
// not nil. A 401 drops the cached token and retries once.
func (c *Client) do(ctx context.Context, method, path string, out any) error {
	for attempt := 0; ; attempt++ {
		token, err := c.accessToken(ctx)
		if err != nil {
			return err
		}
		var body io.Reader
		if method != http.MethodGet {
			// The API rejects an empty body when Content-Type is JSON.
			body = strings.NewReader("{}")
		}
		req, err := http.NewRequestWithContext(ctx, method, c.APIURL+path, body)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-request-id", uuid4())
		resp, err := c.HTTP.Do(req)
		if err != nil {
			return fmt.Errorf("contabo %s %s: %w", method, path, err)
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("contabo %s %s: read body: %w", method, path, err)
		}
		if resp.StatusCode == http.StatusUnauthorized && attempt == 0 {
			c.dropToken()
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return &APIError{Method: method, Path: path, Status: resp.StatusCode, Body: truncate(string(data), 512)}
		}
		if out == nil || len(data) == 0 {
			return nil
		}
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("contabo %s %s: decode: %w", method, path, err)
		}
		return nil
	}
}

func (c *Client) accessToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" && c.now().Before(c.expiry) {
		return c.token, nil
	}
	form := url.Values{
		"grant_type":    {"password"},
		"client_id":     {c.Creds.ClientID},
		"client_secret": {c.Creds.ClientSecret},
		"username":      {c.Creds.Username},
		"password":      {c.Creds.Password},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.AuthURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", fmt.Errorf("contabo token: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("contabo token: read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		// The body of a failed grant names the error, never the credentials.
		return "", &APIError{Method: http.MethodPost, Path: "token", Status: resp.StatusCode, Body: truncate(string(data), 256)}
	}
	var tok struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(data, &tok); err != nil {
		return "", fmt.Errorf("contabo token: decode: %w", err)
	}
	if tok.AccessToken == "" {
		return "", errors.New("contabo token: empty access_token")
	}
	c.token = tok.AccessToken
	c.expiry = c.now().Add(time.Duration(tok.ExpiresIn)*time.Second - tokenSkew)
	return c.token, nil
}

func (c *Client) dropToken() {
	c.mu.Lock()
	c.token = ""
	c.mu.Unlock()
}

func uuid4() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
