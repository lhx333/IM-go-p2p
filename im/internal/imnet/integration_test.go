package imnet_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lhx333/im-p2p/internal/imnet"
	"github.com/lhx333/im-p2p/internal/wire"
)

// fakeRV 是协调服务的测试替身,契约与 Cloudflare Workers 上的 im-rv 一致:
//   - POST /v1/register  记录 peer 的房间/昵称/地址
//   - GET  /v1/discover  返回同房间其它 peer
type fakeRV struct {
	mu    sync.Mutex
	rooms map[string]map[string]wire.PeerAddr
}

func (f *fakeRV) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/v1/register" && r.Method == http.MethodPost:
		var m wire.Msg
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		room := m.Room
		if room == "" {
			room = "lobby"
		}
		if f.rooms == nil {
			f.rooms = map[string]map[string]wire.PeerAddr{}
		}
		if f.rooms[room] == nil {
			f.rooms[room] = map[string]wire.PeerAddr{}
		}
		f.rooms[room][m.ID] = wire.PeerAddr{ID: m.ID, Nick: m.Nick, Addrs: m.Addrs}
		writeJSON(w, wire.Msg{OK: true, Room: room})

	case r.URL.Path == "/v1/discover" && r.Method == http.MethodGet:
		q := r.URL.Query()
		room := q.Get("room")
		if room == "" {
			room = "lobby"
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		res := wire.Msg{OK: true}
		for id, p := range f.rooms[room] {
			if id != q.Get("id") {
				res.Peers = append(res.Peers, p)
			}
		}
		writeJSON(w, res)

	default:
		http.NotFound(w, r)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// TestEndToEnd 启动一个协调服务与两个客户端,验证:
// 发现 -> 直连 -> 文字消息 -> 图片/文件媒体传输 的完整链路。
func TestEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 1. 协调服务(仅负责发现)
	srv := httptest.NewServer(&fakeRV{})
	defer srv.Close()

	// 2. 两个客户端节点
	base := t.TempDir()
	msgB := make(chan imnet.Message, 32)

	mkNode := func(name, nick string, sink func(imnet.Message)) *imnet.Node {
		d := filepath.Join(base, name)
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		n, err := imnet.New(imnet.Config{
			IdentityPath: filepath.Join(d, "id.key"),
			Nick:         nick,
			Room:         "room1",
			RVURL:        srv.URL,
			BlobDir:      filepath.Join(d, "blobs"),
			ListenAddrs:  []string{"/ip4/127.0.0.1/tcp/0"},
			OnMessage:    sink,
			Log:          func(f string, a ...any) { t.Logf("["+nick+"] "+f, a...) },
		})
		if err != nil {
			t.Fatalf("new node %s: %v", name, err)
		}
		return n
	}

	na := mkNode("alice", "alice", func(m imnet.Message) {})
	nb := mkNode("bob", "bob", func(m imnet.Message) {
		if !m.Self {
			msgB <- m
		}
	})
	defer na.Close()
	defer nb.Close()
	na.Start(ctx)
	nb.Start(ctx)

	// 3. 等待双方通过协调服务互相发现并直连
	waitConnected(t, ctx, na, nb)

	// 4. 文字消息
	if err := na.SendText("all", "text", "hello bob"); err != nil {
		t.Fatalf("send text: %v", err)
	}
	txt := recv(t, msgB)
	if txt.Type != "text" || txt.Text != "hello bob" || txt.Self {
		t.Fatalf("unexpected text msg: %+v", txt)
	}
	t.Logf("bob received text from %s: %q", txt.From, txt.Text)

	// 5. 表情消息
	if err := na.SendText("all", "emoji", "🎉"); err != nil {
		t.Fatalf("send emoji: %v", err)
	}
	emo := recv(t, msgB)
	if emo.Type != "emoji" || emo.Text != "🎉" {
		t.Fatalf("unexpected emoji msg: %+v", emo)
	}

	// 6. 图片/文件媒体传输
	payload := bytes.Repeat([]byte("im-p2p-media-payload-"), 4096) // ~90KB
	blobID := imnet.NewID()
	if _, err := na.Store().Put(blobID, bytes.NewReader(payload)); err != nil {
		t.Fatalf("store put: %v", err)
	}
	na.Store().SetMeta(imnet.BlobMeta{
		ID: blobID, Name: "photo.png", Mime: "image/png", Kind: "image", Size: int64(len(payload)),
	})
	if err := na.SendBlob(blobID, "all"); err != nil {
		t.Fatalf("send blob: %v", err)
	}
	media := recv(t, msgB)
	if media.Type != "image" || media.FileName != "photo.png" || media.Size != int64(len(payload)) {
		t.Fatalf("unexpected media msg: %+v", media)
	}

	// 7. 校验 bob 落盘的内容与发送一致
	path, ok := nb.Store().PathOf(blobID)
	if !ok {
		t.Fatalf("bob did not store blob %s", blobID)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read received blob: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("received blob mismatch: got %d bytes, want %d", len(got), len(payload))
	}
	t.Logf("bob received media %q (%d bytes) at %s", media.FileName, media.Size, media.MediaURL)
}

func waitConnected(t *testing.T, ctx context.Context, nodes ...*imnet.Node) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		allOK := true
		for _, n := range nodes {
			ok := false
			for _, p := range n.Peers() {
				if p.Connected {
					ok = true
					break
				}
			}
			if !ok {
				allOK = false
				break
			}
		}
		if allOK {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("ctx done while waiting for connection")
		case <-time.After(300 * time.Millisecond):
		}
	}
	for _, n := range nodes {
		t.Logf("node %s peers: %+v", n.Nick(), n.Peers())
	}
	t.Fatal("timeout waiting for peers to connect")
}

func recv(t *testing.T, ch chan imnet.Message) imnet.Message {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for message")
		return imnet.Message{}
	}
}
