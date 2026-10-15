// Command imserver 是"仅协调"的中央服务器。
//
// 它只负责:接收 peer 注册(房间 + 昵称 + 地址)并按房间返回在线 peer 列表。
// 它不转发、不存储任何聊天内容,所有消息都在客户端之间点对点直连传输。
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/multiformats/go-multiaddr"

	"github.com/lhx333/im-p2p/internal/addrs"
	"github.com/lhx333/im-p2p/internal/rv"
)

func main() {
	listen := flag.String("listen", "/ip4/0.0.0.0/tcp/7001,/ip4/0.0.0.0/udp/7001/quic-v1", "监听地址,逗号分隔")
	idPath := flag.String("identity", filepath.Join(defaultDataDir(), "server.key"), "服务器身份私钥路径")
	ttl := flag.Duration("ttl", 30*time.Second, "注册条目过期时间")
	flag.Parse()

	priv, err := loadOrCreateKey(*idPath)
	if err != nil {
		log.Fatalf("加载身份失败: %v", err)
	}

	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(strings.Split(*listen, ",")...),
		libp2p.NATPortMap(),
	)
	if err != nil {
		log.Fatalf("启动 libp2p 节点失败: %v", err)
	}
	defer h.Close()

	reg := rv.NewRegistry(*ttl)
	h.SetStreamHandler(rv.ProtocolID, rv.ServerHandler(reg))

	fmt.Println("========================================")
	fmt.Println(" IM P2P 协调服务器已启动 (仅负责发现/协调)")
	fmt.Println("========================================")
	fmt.Printf("Peer ID: %s\n", h.ID())
	fmt.Println("把下面任一地址作为客户端的 -server 参数:")
	p2pMA, err := multiaddr.NewMultiaddr("/p2p/" + h.ID().String())
	if err != nil {
		log.Fatalf("构造 p2p 地址失败: %v", err)
	}
	for _, a := range addrs.Reachable(h) {
		ma, err := multiaddr.NewMultiaddr(a)
		if err != nil {
			continue
		}
		fmt.Printf("  %s\n", ma.Encapsulate(p2pMA))
	}
	fmt.Println("按 Ctrl-C 退出")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	<-ctx.Done()
	fmt.Println("\n正在关闭协调服务器…")
}

func defaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return filepath.Join(os.TempDir(), "im-p2p")
	}
	return filepath.Join(home, ".im-p2p")
}

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
	return priv, os.WriteFile(path, b, 0o600)
}
