// Package rv 实现客户端侧的"协调/发现":通过 HTTPS 把本节点注册到协调服务
// (部署在 Cloudflare Workers 上的 im-rv),并拉取同房间其它在线 peer 的可拨号地址。
//
// 协调服务只维护一份"房间 -> peer"的短期注册表,用于让 peer 互相发现;
// 它不转发、不存储任何聊天内容。拿到地址之后,聊天与媒体全部在客户端之间
// 点对点直连传输。
package rv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lhx333/im-p2p/internal/wire"
)

const (
	registerPath = "/v1/register"
	discoverPath = "/v1/discover"

	// maxRespBytes 限制响应体读取量,避免异常响应耗尽内存。
	maxRespBytes = 1 << 20
)

// Client 是客户端侧的协调器:定期向协调服务注册自己并拉取同房间的 peer 列表。
type Client struct {
	URL      string         // 协调服务地址,例如 https://im-rv.<子域>.workers.dev
	Room     string         // 房间名
	Nick     string         // 昵称
	SelfID   string         // 本节点 peer ID
	Addrs    func() []string // 返回本节点当前可被拨号的地址
	Interval time.Duration  // 注册/发现的周期,默认 5 秒
	HTTP     *http.Client   // 为空则使用默认客户端(10 秒超时)

	OnPeers  func([]wire.PeerAddr)
	OnStatus func(string)
	Log      func(format string, args ...any)
}

func (c *Client) logf(format string, args ...any) {
	if c.Log != nil {
		c.Log(format, args...)
	}
}

func (c *Client) status(s string) {
	if c.OnStatus != nil {
		c.OnStatus(s)
	}
}

func (c *Client) room() string {
	if c.Room == "" {
		return "lobby"
	}
	return c.Room
}

func (c *Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: 10 * time.Second}
}

// Run 阻塞运行协调循环,直到 ctx 结束。请求失败会自动重试。
func (c *Client) Run(ctx context.Context) {
	interval := c.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	c.status("连接协调服务器中…")
	for ctx.Err() == nil {
		if err := c.round(ctx); err != nil {
			c.logf("rendezvous round failed: %v", err)
			c.status("协调服务不可用,重试中…")
			if !sleep(ctx, 2*time.Second) {
				return
			}
			continue
		}
		c.status("已连接协调服务器,房间: " + c.room())
		if !sleep(ctx, interval) {
			return
		}
	}
}

// round 完成一次"注册 + 发现"。
func (c *Client) round(ctx context.Context) error {
	base, err := c.baseURL()
	if err != nil {
		return err
	}

	var addrs []string
	if c.Addrs != nil {
		addrs = c.Addrs()
	}
	body, err := json.Marshal(wire.Msg{ID: c.SelfID, Nick: c.Nick, Room: c.room(), Addrs: addrs})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+registerPath, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	var ack wire.Msg
	if err := c.do(req, &ack); err != nil {
		return fmt.Errorf("注册失败: %w", err)
	}

	q := url.Values{}
	q.Set("room", c.room())
	q.Set("id", c.SelfID)
	req, err = http.NewRequestWithContext(ctx, http.MethodGet, base+discoverPath+"?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	var res wire.Msg
	if err := c.do(req, &res); err != nil {
		return fmt.Errorf("发现失败: %w", err)
	}
	if c.OnPeers != nil {
		c.OnPeers(res.Peers)
	}
	return nil
}

// do 发送请求并把响应解码进 v,响应不是成功语义时返回错误。
func (c *Client) do(req *http.Request, v *wire.Msg) error {
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxRespBytes))
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("协调服务返回非 JSON(HTTP %d)", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK || !v.OK {
		if v.Error != "" {
			return fmt.Errorf("HTTP %d: %s", resp.StatusCode, v.Error)
		}
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

// baseURL 校验并规整协调服务地址。
func (c *Client) baseURL() (string, error) {
	raw := strings.TrimRight(strings.TrimSpace(c.URL), "/")
	if raw == "" {
		return "", errors.New("协调服务地址为空")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("协调服务地址非法: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("协调服务地址非法: %s", raw)
	}
	return raw, nil
}

// sleep 等待 d,ctx 结束时返回 false。
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
