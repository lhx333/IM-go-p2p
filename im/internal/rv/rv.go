// Package rv 实现"协调/发现"(rendezvous)协议。
//
// 中央服务器只做两件事:
//   - register: 记录某个 peer 加入的房间、昵称与其可达地址;
//   - discover: 把同一房间内其它在线 peer 的地址返回给请求方。
//
// 服务器不转发、不存储任何聊天消息,因此它只承担"协调"职责。
// 协议跑在一条普通的 libp2p 流上,请求/响应按顺序一来一回。
package rv

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/lhx333/im-p2p/internal/addrs"
	"github.com/lhx333/im-p2p/internal/wire"
)

// ProtocolID 是协调协议的协议标识。
const ProtocolID = protocol.ID("/im/rv/1.0.0")

const (
	OpRegister = "register"
	OpDiscover = "discover"
)

type entry struct {
	nick  string
	addrs []string
	seen  time.Time
}

// Registry 是服务器内存中的"房间 -> peer"注册表。
type Registry struct {
	mu    sync.Mutex
	rooms map[string]map[peer.ID]*entry
	ttl   time.Duration
}

func NewRegistry(ttl time.Duration) *Registry {
	return &Registry{rooms: make(map[string]map[peer.ID]*entry), ttl: ttl}
}

func (r *Registry) register(id peer.ID, room, nick string, addrs []string) {
	if room == "" {
		room = "lobby"
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.rooms[room]
	if !ok {
		m = make(map[peer.ID]*entry)
		r.rooms[room] = m
	}
	m[id] = &entry{nick: nick, addrs: addrs, seen: time.Now()}
}

func (r *Registry) discover(room string, self peer.ID) []wire.PeerAddr {
	if room == "" {
		room = "lobby"
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.rooms[room]
	if m == nil {
		return nil
	}
	now := time.Now()
	out := make([]wire.PeerAddr, 0, len(m))
	for id, e := range m {
		if now.Sub(e.seen) > r.ttl {
			delete(m, id) // 清理过期条目
			continue
		}
		if id == self {
			continue
		}
		out = append(out, wire.PeerAddr{ID: id.String(), Nick: e.nick, Addrs: e.addrs})
	}
	return out
}

// ServerHandler 返回用于 host.SetStreamHandler 的处理函数。
func ServerHandler(reg *Registry) network.StreamHandler {
	return func(s network.Stream) {
		defer s.Close()
		r := wire.NewReader(s)
		w := wire.NewWriter(s)
		for {
			m, err := r.ReadCtrl()
			if err != nil {
				return
			}
			switch m.Op {
			case OpRegister:
				// 客户端自报地址 + 服务器观测到的对端地址(后者在 NAT 场景下更有用)。
				announced := append([]string{}, m.Addrs...)
				if obs := s.Conn().RemoteMultiaddr(); obs != nil {
					announced = append(announced, obs.String())
				}
				reg.register(s.Conn().RemotePeer(), m.Room, m.Nick, dedup(announced))
				if err := w.WriteCtrl(wire.Msg{Op: OpRegister, OK: true, Room: m.Room}); err != nil {
					return
				}
			case OpDiscover:
				peers := reg.discover(m.Room, s.Conn().RemotePeer())
				if err := w.WriteCtrl(wire.Msg{Op: OpDiscover, OK: true, Peers: peers}); err != nil {
					return
				}
			default:
				if err := w.WriteCtrl(wire.Msg{OK: false, Error: "unknown op"}); err != nil {
					return
				}
			}
			if err := w.Flush(); err != nil {
				return
			}
		}
	}
}

// Client 是客户端侧的协调器:定期向服务器注册并拉取同房间的 peer 列表。
type Client struct {
	Host     host.Host
	Server   peer.AddrInfo
	Room     string
	Nick     string
	Interval time.Duration
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

// Run 阻塞运行协调循环,直到 ctx 结束。断线会自动重连。
func (c *Client) Run(ctx context.Context) {
	if c.Interval <= 0 {
		c.Interval = 5 * time.Second
	}
	for ctx.Err() == nil {
		c.session(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (c *Client) session(ctx context.Context) {
	c.status("连接协调服务器中…")
	if err := c.Host.Connect(ctx, c.Server); err != nil {
		c.logf("connect server failed: %v", err)
		c.status("无法连接协调服务器,重试中…")
		return
	}
	s, err := c.Host.NewStream(ctx, c.Server.ID, ProtocolID)
	if err != nil {
		c.logf("open rendezvous stream failed: %v", err)
		c.status("无法建立协调通道,重试中…")
		return
	}
	defer s.Close()
	c.status("已连接协调服务器")

	r := wire.NewReader(s)
	w := wire.NewWriter(s)

	tick := time.NewTicker(c.Interval)
	defer tick.Stop()
	for {
		if err := c.round(s, r, w, true); err != nil {
			c.logf("rendezvous round failed: %v", err)
			c.status("协调通道断开,重连中…")
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// round 完成一轮 register + discover 的请求/响应。
func (c *Client) round(_ network.Stream, r *wire.Reader, w *wire.Writer, register bool) error {
	if register {
		myAddrs := addrs.Reachable(c.Host)
		if err := w.WriteCtrl(wire.Msg{
			Op: OpRegister, Room: c.Room, Nick: c.Nick, Addrs: myAddrs,
		}); err != nil {
			return err
		}
		if err := w.Flush(); err != nil {
			return err
		}
		if _, err := r.ReadCtrl(); err != nil {
			return err
		}
	}

	if err := w.WriteCtrl(wire.Msg{Op: OpDiscover, Room: c.Room}); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	resp, err := r.ReadCtrl()
	if err != nil {
		return err
	}
	if c.OnPeers != nil {
		c.OnPeers(resp.Peers)
	}
	return nil
}

func dedup(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
