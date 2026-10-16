package main

import (
	"embed"
	"encoding/json"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/lhx333/im-p2p/internal/imnet"
)

//go:embed web
var webAssets embed.FS

// app 把 libp2p 节点与浏览器端的 WebSocket 桥接起来。
type app struct {
	node     *imnet.Node
	hub      *hub
	room     string
	selfNick string

	statusMu   sync.Mutex
	lastStatus string
}

func (a *app) onMessage(m imnet.Message) { a.hub.broadcast(event("msg", m)) }
func (a *app) onPeers(p []imnet.Peer)   { a.hub.broadcast(event("peers", p)) }

func (a *app) onStatus(s string) {
	a.statusMu.Lock()
	a.lastStatus = s
	a.statusMu.Unlock()
	a.hub.broadcast(event("status", map[string]string{"text": s}))
}

// currentStatus 返回最近一次协调状态,便于新连接的页面立即拿到真实状态。
func (a *app) currentStatus() string {
	a.statusMu.Lock()
	defer a.statusMu.Unlock()
	if a.lastStatus == "" {
		return "正在连接协调服务器…"
	}
	return a.lastStatus
}

func (a *app) registerRoutes(mux *http.ServeMux) {
	sub, err := fs.Sub(webAssets, "web")
	if err != nil {
		panic(err)
	}
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(sub))))
	mux.HandleFunc("/", a.handleIndex)
	mux.HandleFunc("/ws", a.handleWS)
	mux.HandleFunc("/api/upload", a.handleUpload)
	mux.HandleFunc("/media/", a.handleMedia)
}

func event(ev string, data any) map[string]any {
	return map[string]any{"ev": ev, "data": data}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (a *app) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	sub, _ := fs.Sub(webAssets, "web")
	b, err := fs.ReadFile(sub, "index.html")
	if err != nil {
		http.Error(w, "index.html not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(b)
}

// ---------------------------------------------------------------- WebSocket

type wsCmd struct {
	Cmd  string `json:"cmd"`
	To   string `json:"to"`
	Kind string `json:"kind"`
	Text string `json:"text"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

type wsClient struct {
	conn *websocket.Conn
	send chan []byte
}

func (c *wsClient) push(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	select {
	case c.send <- b:
	default:
	}
}

func (c *wsClient) writePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case msg, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, nil)
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (a *app) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &wsClient{conn: conn, send: make(chan []byte, 64)}
	a.hub.add(c)
	defer func() {
		a.hub.remove(c)
		_ = conn.Close()
	}()
	go c.writePump()

	c.push(event("self", map[string]any{
		"id": a.node.SelfID(), "nick": a.selfNick, "room": a.room,
	}))
	c.push(event("status", map[string]string{"text": a.currentStatus()}))
	c.push(event("peers", a.node.Peers()))

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var cmd wsCmd
		if json.Unmarshal(data, &cmd) != nil {
			continue
		}
		a.handleCmd(c, cmd)
	}
}

func (a *app) handleCmd(c *wsClient, cmd wsCmd) {
	switch cmd.Cmd {
	case "send":
		kind := cmd.Kind
		if kind == "" {
			kind = "text"
		}
		if err := a.node.SendText(cmd.To, kind, cmd.Text); err != nil {
			c.push(event("error", map[string]string{"text": err.Error()}))
		}
	case "refresh":
		c.push(event("peers", a.node.Peers()))
	case "self":
		c.push(event("self", map[string]any{
			"id": a.node.SelfID(), "nick": a.selfNick, "room": a.room,
		}))
	}
}

// ---------------------------------------------------------------- 上传 / 媒体

func (a *app) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		http.Error(w, "解析上传失败: "+err.Error(), http.StatusBadRequest)
		return
	}
	to := r.FormValue("to")
	kind := r.FormValue("kind")
	if kind == "" {
		kind = "file"
	}
	dur, _ := strconv.ParseInt(r.FormValue("duration"), 10, 64)

	f, hdr, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "缺少 file 字段", http.StatusBadRequest)
		return
	}
	defer f.Close()

	id := imnet.NewID()
	size, err := a.node.Store().Put(id, f)
	if err != nil {
		http.Error(w, "保存失败: "+err.Error(), http.StatusInternalServerError)
		return
	}
	ct := hdr.Header.Get("Content-Type")
	if ct == "" {
		ct = mime.TypeByExtension(filepath.Ext(hdr.Filename))
	}
	if ct == "" {
		ct = "application/octet-stream"
	}
	name := hdr.Filename
	if name == "" {
		name = kind
	}
	a.node.Store().SetMeta(imnet.BlobMeta{
		ID: id, Name: name, Mime: ct, Kind: kind, Size: size, Duration: dur,
	})

	if err := a.node.SendBlob(id, to); err != nil {
		writeJSON(w, map[string]any{"ok": true, "id": id, "warning": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"ok": true, "id": id})
}

func (a *app) handleMedia(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/media/")
	meta, ok := a.node.Store().Meta(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	f, err := os.Open(meta.Path)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	if meta.Mime != "" {
		w.Header().Set("Content-Type", meta.Mime)
	}
	if meta.Kind == "file" {
		w.Header().Set("Content-Disposition", "attachment; filename=\""+meta.Name+"\"")
	}
	_, _ = io.Copy(w, f)
}

// ---------------------------------------------------------------- hub

type hub struct {
	mu      sync.Mutex
	clients map[*wsClient]struct{}
}

func newHub() *hub { return &hub{clients: make(map[*wsClient]struct{})} }

func (h *hub) add(c *wsClient) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *hub) remove(c *wsClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		close(c.send)
	}
}

func (h *hub) broadcast(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		select {
		case c.send <- b:
		default: // 客户端消费太慢,丢弃该事件
		}
	}
}
