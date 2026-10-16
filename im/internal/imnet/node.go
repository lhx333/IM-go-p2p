// Package imnet 是 IM 应用的核心:在 go-libp2p 之上实现点对点聊天、
// 图片/文件传输与语音消息。它不依赖任何 UI,PC 端 Web 桥接与后续的移动端
// 都复用这一层。
package imnet

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"

	"github.com/lhx333/im-p2p/internal/rv"
	"github.com/lhx333/im-p2p/internal/wire"
)

// ProtocolID 是 peer 之间传输聊天与媒体的协议标识。
const ProtocolID = protocol.ID("/im/msg/1.0.0")

// NewID 生成一个随机的消息/媒体 id。
func NewID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Config 描述节点启动参数。
type Config struct {
	IdentityPath string   // 私钥持久化路径(为空则每次生成随机身份)
	Nick         string   // 昵称
	Room         string   // 房间名
	ListenAddrs  []string // 监听地址,为空则用 libp2p 默认
	Server       peer.AddrInfo
	BlobDir      string

	OnMessage func(Message)
	OnPeers   func([]Peer)
	OnStatus  func(string)
	Log       func(format string, args ...any)
}

// Message 是抛给 UI 层的一条消息事件。
type Message struct {
	Type     string `json:"type"`
	ID       string `json:"id"`
	From     string `json:"from"`
	FromNick string `json:"from_nick"`
	Text     string `json:"text,omitempty"`
	FileName string `json:"file_name,omitempty"`
	Mime     string `json:"mime,omitempty"`
	Size     int64  `json:"size,omitempty"`
	Duration int64  `json:"duration,omitempty"`
	Ts       int64  `json:"ts"`
	MediaURL string `json:"media_url,omitempty"`
	Self     bool   `json:"self"`
}

// Peer 描述 UI 可见的一个 peer。
type Peer struct {
	ID        string   `json:"id"`
	Nick      string   `json:"nick"`
	Addrs     []string `json:"addrs"`
	Connected bool     `json:"connected"`
}

type peerState struct {
	id    peer.ID
	nick  string
	addrs []string
}

type outStream struct {
	mu sync.Mutex
	w  *wire.Writer
	s  network.Stream
}

// Node 是一个运行中的 IM 节点。
type Node struct {
	cfg   Config
	store *Store
	Host  host.Host

	handlerMu sync.Mutex

	mu    sync.Mutex
	peers map[peer.ID]*peerState
	out   map[peer.ID]*outStream

	rvClient *rv.Client
}

// New 创建节点:初始化(或复用)身份、启动 libp2p Host 并注册消息处理协议。
func New(cfg Config) (*Node, error) {
	if cfg.BlobDir == "" {
		cfg.BlobDir = filepath.Join(os.TempDir(), "im-p2p-blobs")
	}
	opts := []libp2p.Option{
		libp2p.NATPortMap(),
		libp2p.EnableHolePunching(),
	}
	if cfg.IdentityPath != "" {
		priv, err := loadOrCreateKey(cfg.IdentityPath)
		if err != nil {
			return nil, err
		}
		opts = append(opts, libp2p.Identity(priv))
	}
	if len(cfg.ListenAddrs) > 0 {
		opts = append(opts, libp2p.ListenAddrStrings(cfg.ListenAddrs...))
	}
	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}
	store, err := NewStore(cfg.BlobDir)
	if err != nil {
		_ = h.Close()
		return nil, err
	}
	n := &Node{
		cfg:   cfg,
		store: store,
		Host:  h,
		peers: make(map[peer.ID]*peerState),
		out:   make(map[peer.ID]*outStream),
	}
	h.SetStreamHandler(ProtocolID, n.handleMsgStream)
	return n, nil
}

func (n *Node) logf(format string, args ...any) {
	if n.cfg.Log != nil {
		n.cfg.Log(format, args...)
	}
}

// Start 启动协调客户端,开始发现同房间的 peer。
func (n *Node) Start(ctx context.Context) {
	n.rvClient = &rv.Client{
		Host:     n.Host,
		Server:   n.cfg.Server,
		Room:     n.cfg.Room,
		Nick:     n.cfg.Nick,
		Interval: 5 * time.Second,
		OnPeers:  n.onDiscovered,
		OnStatus: n.emitStatus,
		Log:      n.logf,
	}
	go n.rvClient.Run(ctx)
}

// Close 关闭节点。
func (n *Node) Close() error { return n.Host.Close() }

func (n *Node) SelfID() string { return n.Host.ID().String() }
func (n *Node) Nick() string   { return n.cfg.Nick }
func (n *Node) Room() string   { return n.cfg.Room }
func (n *Node) Store() *Store  { return n.store }

// Peers 返回当前已知的 peer 列表(含连接状态)。
func (n *Node) Peers() []Peer {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]Peer, 0, len(n.peers))
	for pid, ps := range n.peers {
		out = append(out, Peer{
			ID:        pid.String(),
			Nick:      ps.nick,
			Addrs:     ps.addrs,
			Connected: n.Host.Network().Connectedness(pid) == network.Connected,
		})
	}
	return out
}

// ---------------------------------------------------------------- 事件回调

func (n *Node) emitMessage(m Message) {
	n.handlerMu.Lock()
	cb := n.cfg.OnMessage
	n.handlerMu.Unlock()
	if cb != nil {
		cb(m)
	}
}

func (n *Node) emitPeers(p []Peer) {
	n.handlerMu.Lock()
	cb := n.cfg.OnPeers
	n.handlerMu.Unlock()
	if cb != nil {
		cb(p)
	}
}

func (n *Node) emitStatus(s string) {
	n.handlerMu.Lock()
	cb := n.cfg.OnStatus
	n.handlerMu.Unlock()
	if cb != nil {
		cb(s)
	}
}

// ---------------------------------------------------------------- 发现/连接

func (n *Node) onDiscovered(peers []wire.PeerAddr) {
	for _, p := range peers {
		pid, err := peer.Decode(p.ID)
		if err != nil || pid == n.Host.ID() {
			continue
		}
		n.mu.Lock()
		ps := n.peers[pid]
		if ps == nil {
			ps = &peerState{id: pid}
			n.peers[pid] = ps
		}
		ps.nick = p.Nick
		ps.addrs = p.Addrs
		n.mu.Unlock()
		go n.ensureConn(pid, p.Addrs)
	}
	n.emitPeers(n.Peers())
}

func (n *Node) ensureConn(pid peer.ID, addrs []string) {
	if n.Host.Network().Connectedness(pid) == network.Connected {
		return
	}
	ai := peer.AddrInfo{ID: pid}
	for _, a := range addrs {
		if ma, err := multiaddr.NewMultiaddr(a); err == nil {
			ai.Addrs = append(ai.Addrs, ma)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := n.Host.Connect(ctx, ai); err != nil {
		n.logf("dial %s failed: %v", pid, err)
		return
	}
	n.logf("connected to %s", pid)
	n.emitPeers(n.Peers())
}

func (n *Node) addrInfo(pid peer.ID) peer.AddrInfo {
	n.mu.Lock()
	defer n.mu.Unlock()
	ai := peer.AddrInfo{ID: pid}
	if ps := n.peers[pid]; ps != nil {
		for _, a := range ps.addrs {
			if ma, err := multiaddr.NewMultiaddr(a); err == nil {
				ai.Addrs = append(ai.Addrs, ma)
			}
		}
	}
	return ai
}

func (n *Node) resolveTargets(to string) ([]peer.ID, error) {
	if to != "" && to != "all" {
		pid, err := peer.Decode(to)
		if err != nil {
			return nil, err
		}
		return []peer.ID{pid}, nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []peer.ID
	for pid := range n.peers {
		if n.Host.Network().Connectedness(pid) == network.Connected {
			out = append(out, pid)
		}
	}
	return out, nil
}

// ---------------------------------------------------------------- 发送

// SendText 发送文字或表情消息。to 为 peer ID,或 "all"/"" 表示广播给所有在线 peer。
func (n *Node) SendText(to, kind, text string) error {
	if kind == "" {
		kind = wire.TypeText
	}
	targets, err := n.resolveTargets(to)
	if err != nil {
		return err
	}
	id := NewID()
	ts := time.Now().UnixMilli()
	msg := wire.Msg{ID: id, Type: kind, Nick: n.cfg.Nick, Text: text, Ts: ts}
	var firstErr error
	for _, p := range targets {
		if err := n.sendCtrl(p, msg); err != nil {
			n.logf("send to %s failed: %v", p, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	n.emitMessage(Message{
		Type: kind, ID: id, From: n.SelfID(), FromNick: n.cfg.Nick,
		Text: text, Ts: ts, Self: true,
	})
	if len(targets) == 0 {
		return errors.New("当前没有已连接的 peer")
	}
	return firstErr
}

// SendBlob 把本地已存好的媒体(按 id)发送给目标。to 语义同 SendText。
func (n *Node) SendBlob(id, to string) error {
	meta, ok := n.store.Meta(id)
	if !ok {
		return errors.New("媒体不存在: " + id)
	}
	targets, err := n.resolveTargets(to)
	if err != nil {
		return err
	}
	var firstErr error
	for _, p := range targets {
		if err := n.sendMediaTo(p, id); err != nil {
			n.logf("send media to %s failed: %v", p, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	n.emitMessage(mediaMessage(wire.Msg{
		ID: meta.ID, Type: meta.Kind, FileName: meta.Name,
		Mime: meta.Mime, Size: meta.Size, Duration: meta.Duration,
	}, meta, true))
	if len(targets) == 0 {
		return errors.New("当前没有已连接的 peer")
	}
	return firstErr
}

func (n *Node) sendCtrl(p peer.ID, msg wire.Msg) error {
	os, err := n.peerStream(p)
	if err != nil {
		return err
	}
	os.mu.Lock()
	defer os.mu.Unlock()
	if err := os.w.WriteCtrl(msg); err != nil {
		n.dropStream(p, os)
		return err
	}
	return os.w.Flush()
}

func (n *Node) sendMediaTo(p peer.ID, id string) error {
	f, meta, err := n.store.Open(id)
	if err != nil {
		return err
	}
	defer f.Close()

	os, err := n.peerStream(p)
	if err != nil {
		return err
	}
	os.mu.Lock()
	defer os.mu.Unlock()

	msg := wire.Msg{
		ID: meta.ID, Type: meta.Kind, Nick: n.cfg.Nick, FileName: meta.Name,
		Mime: meta.Mime, Size: meta.Size, Duration: meta.Duration,
		Ts: time.Now().UnixMilli(),
	}
	if err := os.w.WriteCtrl(msg); err != nil {
		n.dropStream(p, os)
		return err
	}
	buf := make([]byte, 32*1024)
	for {
		nn, rerr := f.Read(buf)
		if nn > 0 {
			if werr := os.w.WriteBlob(buf[:nn]); werr != nil {
				n.dropStream(p, os)
				return werr
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return rerr
		}
	}
	if err := os.w.WriteEnd(); err != nil {
		n.dropStream(p, os)
		return err
	}
	return os.w.Flush()
}

func (n *Node) peerStream(p peer.ID) (*outStream, error) {
	n.mu.Lock()
	existing := n.out[p]
	n.mu.Unlock()
	if existing != nil {
		return existing, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if n.Host.Network().Connectedness(p) != network.Connected {
		if err := n.Host.Connect(ctx, n.addrInfo(p)); err != nil {
			return nil, err
		}
	}
	s, err := n.Host.NewStream(ctx, p, ProtocolID)
	if err != nil {
		return nil, err
	}
	os := &outStream{w: wire.NewWriter(s), s: s}
	n.mu.Lock()
	n.out[p] = os
	n.mu.Unlock()
	return os, nil
}

func (n *Node) dropStream(p peer.ID, os *outStream) {
	_ = os.s.Close()
	n.mu.Lock()
	if n.out[p] == os {
		delete(n.out, p)
	}
	n.mu.Unlock()
}

// ---------------------------------------------------------------- 接收

type inbound struct {
	meta wire.Msg
	f    *os.File
	got  int64
}

func (n *Node) handleMsgStream(s network.Stream) {
	remote := s.Conn().RemotePeer()
	// 记录对端可达地址,便于我们主动回发消息。
	n.Host.Peerstore().AddAddrs(remote, []multiaddr.Multiaddr{s.Conn().RemoteMultiaddr()}, time.Hour)

	r := wire.NewReader(s)
	var in *inbound
	defer func() {
		if in != nil {
			_ = in.f.Close()
		}
		_ = s.Close()
	}()

	for {
		t, p, err := r.ReadFrame()
		if err != nil {
			return
		}
		switch t {
		case wire.FrameCtrl:
			var m wire.Msg
			if err := json.Unmarshal(p, &m); err != nil {
				return
			}
			m.From = remote.String()
			if m.Nick != "" {
				n.noteNick(remote, m.Nick)
			}
			switch m.Type {
			case wire.TypeFile, wire.TypeImage, wire.TypeVoice:
				sid := safeID(m.ID)
				f, err := n.store.Create(sid)
				if err != nil {
					n.logf("create blob failed: %v", err)
					return
				}
				m.ID = sid
				in = &inbound{meta: m, f: f}
			default:
				n.emitMessage(Message{
					Type: m.Type, ID: m.ID, From: remote.String(), FromNick: m.Nick,
					Text: m.Text, Ts: m.Ts,
				})
			}

		case wire.FrameBlob:
			if in == nil {
				continue
			}
			nn, err := in.f.Write(p)
			in.got += int64(nn)
			if err != nil {
				_ = in.f.Close()
				in = nil
			}

		case wire.FrameEnd:
			if in == nil {
				continue
			}
			_ = in.f.Close()
			meta := BlobMeta{
				ID: in.meta.ID, Name: in.meta.FileName, Mime: in.meta.Mime,
				Kind: in.meta.Type, Size: in.got, Duration: in.meta.Duration,
			}
			n.store.SetMeta(meta)
			n.emitMessage(mediaMessage(in.meta, meta, false))
			in = nil
		}
	}
}

func mediaMessage(ctrl wire.Msg, meta BlobMeta, self bool) Message {
	return Message{
		Type:     meta.Kind,
		ID:       meta.ID,
		FromNick: ctrl.Nick,
		FileName: meta.Name,
		Mime:     meta.Mime,
		Size:     meta.Size,
		Duration: meta.Duration,
		Ts:       ctrl.Ts,
		MediaURL: "/media/" + meta.ID,
		Self:     self,
	}
}

func (n *Node) noteNick(pid peer.ID, nick string) {
	n.mu.Lock()
	ps := n.peers[pid]
	if ps == nil {
		ps = &peerState{id: pid}
		n.peers[pid] = ps
	}
	ps.nick = nick
	n.mu.Unlock()
}

// ---------------------------------------------------------------- 身份

func loadOrCreateKey(path string) (crypto.PrivKey, error) {
	if b, err := os.ReadFile(path); err == nil {
		if k, err := crypto.UnmarshalPrivateKey(b); err == nil {
			return k, nil
		}
	}
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, err
	}
	b, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		return nil, err
	}
	return priv, nil
}
