package imnet_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/lhx333/im-p2p/internal/addrs"
	"github.com/lhx333/im-p2p/internal/imnet"
	"github.com/lhx333/im-p2p/internal/rv"
)

func toMA(ss []string) []multiaddr.Multiaddr {
	out := make([]multiaddr.Multiaddr, 0, len(ss))
	for _, s := range ss {
		if ma, err := multiaddr.NewMultiaddr(s); err == nil {
			out = append(out, ma)
		}
	}
	return out
}

// TestEndToEnd 启动一个协调服务器与两个客户端,验证:
// 发现 -> 直连 -> 文字消息 -> 图片/文件媒体传输 的完整链路。
func TestEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 1. 协调服务器(仅负责发现)
	sh, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("start server host: %v", err)
	}
	defer sh.Close()
	reg := rv.NewRegistry(30 * time.Second)
	sh.SetStreamHandler(rv.ProtocolID, rv.ServerHandler(reg))
	serverInfo := peer.AddrInfo{ID: sh.ID(), Addrs: toMA(addrs.Reachable(sh))}
	if len(serverInfo.Addrs) == 0 {
		t.Fatal("server has no dialable addr")
	}

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
			Server:       serverInfo,
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

	// 3. 等待双方通过服务器互相发现并直连
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
	_ = toMA(nil)
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
