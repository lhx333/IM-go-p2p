// Command imdesktop 是 PC 端客户端。
//
// 它在一个进程内运行 go-libp2p 节点(负责发现与点对点传输),同时起一个
// 仅监听本机的 HTTP 服务:浏览器打开该地址即为 IM 界面,界面通过 WebSocket
// 与 Go 节点通信。这就是"IM 界面 ↔ go-libp2p"的桥接层。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/lhx333/im-p2p/internal/imnet"
)

func main() {
	serverMA := flag.String("server", "", "协调服务器 multiaddr(必填),例如 /ip4/127.0.0.1/tcp/7001/p2p/<id>")
	room := flag.String("room", "lobby", "房间名")
	nick := flag.String("nick", "", "昵称")
	httpAddr := flag.String("http", "127.0.0.1:8080", "本地 UI 监听地址")
	dataDir := flag.String("data", "", "数据目录(身份/媒体),默认 ~/.im-p2p")
	autoOpen := flag.Bool("open", true, "启动后自动打开浏览器")
	flag.Parse()

	if *serverMA == "" {
		fmt.Println("缺少 -server 参数。请先运行 imserver,复制它打印的地址,例如:")
		fmt.Println("  imdesktop -server /ip4/192.168.1.10/tcp/7001/p2p/12D3KooW...")
		os.Exit(2)
	}
	server, err := peer.AddrInfoFromString(*serverMA)
	if err != nil {
		log.Fatalf("解析 -server 失败: %v", err)
	}

	dir := *dataDir
	if dir == "" {
		dir = defaultDataDir()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Fatalf("创建数据目录失败: %v", err)
	}

	nk := *nick
	if nk == "" {
		nk = defaultNick()
	}

	a := &app{room: *room, selfNick: nk}
	a.hub = newHub()

	node, err := imnet.New(imnet.Config{
		IdentityPath: filepath.Join(dir, "identity.key"),
		Nick:         nk,
		Room:         *room,
		Server:       *server,
		BlobDir:      filepath.Join(dir, "blobs"),
		OnMessage:    a.onMessage,
		OnPeers:      a.onPeers,
		OnStatus:     a.onStatus,
		Log:          log.Printf,
	})
	if err != nil {
		log.Fatalf("创建节点失败: %v", err)
	}
	a.node = node
	defer node.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	node.Start(ctx)

	mux := http.NewServeMux()
	a.registerRoutes(mux)
	srv := &http.Server{Addr: *httpAddr, Handler: mux}
	go func() {
		log.Printf("本地 IM 界面: http://%s", *httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP 服务失败: %v", err)
		}
	}()

	url := "http://" + *httpAddr
	log.Printf("本机 Peer ID: %s | 房间: %s | 昵称: %s", node.SelfID(), *room, nk)
	if *autoOpen {
		go openBrowser(url)
	}

	<-ctx.Done()
	shutCtx, c2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer c2()
	_ = srv.Shutdown(shutCtx)
	fmt.Println("已退出")
}

func defaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return filepath.Join(os.TempDir(), "im-p2p")
	}
	return filepath.Join(home, ".im-p2p")
}

func defaultNick() string {
	if u, err := user.Current(); err == nil && u.Username != "" {
		return u.Username
	}
	return "user-" + imnet.NewID()[:4]
}

func openBrowser(url string) {
	var cmd string
	var args []string
	switch runtime.GOOS {
	case "darwin":
		cmd = "open"
	case "windows":
		cmd = "rundll32"
		args = []string{"url.dll,FileProtocolHandler"}
	default:
		cmd = "xdg-open"
	}
	_ = exec.Command(cmd, append(args, url)...).Start()
}
