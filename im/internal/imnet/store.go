package imnet

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

// BlobMeta 描述一份落盘的媒体文件(图片/文件/语音)。
type BlobMeta struct {
	ID       string
	Name     string
	Mime     string
	Kind     string // image / file / voice
	Size     int64
	Duration int64 // 语音时长(毫秒)
	Path     string
	Ts       int64
}

// Store 把收到的媒体文件落盘,并按 id 索引。
type Store struct {
	dir  string
	mu   sync.Mutex
	meta map[string]BlobMeta
}

func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Store{dir: dir, meta: make(map[string]BlobMeta)}, nil
}

var idRe = regexp.MustCompile(`[^a-zA-Z0-9_-]`)

// safeID 清洗外部传入的 id,避免路径穿越。
func safeID(id string) string {
	clean := idRe.ReplaceAllString(id, "")
	if len(clean) > 64 {
		clean = clean[:64]
	}
	if clean == "" {
		clean = NewID()
	}
	return clean
}

// Create 为 id 创建一个新的空文件,返回可写的文件句柄。
func (s *Store) Create(id string) (*os.File, error) {
	id = safeID(id)
	return os.Create(filepath.Join(s.dir, id))
}

// Put 直接把 r 的内容写入一个新建文件并返回实际字节数。
func (s *Store) Put(id string, r io.Reader) (int64, error) {
	f, err := s.Create(id)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return io.Copy(f, r)
}

// SetMeta 记录媒体元信息。
func (s *Store) SetMeta(m BlobMeta) {
	m.ID = safeID(m.ID)
	m.Path = filepath.Join(s.dir, m.ID)
	if m.Ts == 0 {
		m.Ts = time.Now().UnixMilli()
	}
	s.mu.Lock()
	s.meta[m.ID] = m
	s.mu.Unlock()
}

func (s *Store) Meta(id string) (BlobMeta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.meta[id]
	return m, ok
}

// PathOf 返回 id 对应文件的落盘路径。
func (s *Store) PathOf(id string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.meta[id]
	if !ok {
		return "", false
	}
	return m.Path, true
}

// Open 打开 id 对应的文件用于发送。
func (s *Store) Open(id string) (*os.File, BlobMeta, error) {
	m, ok := s.Meta(id)
	if !ok {
		return nil, BlobMeta{}, fmt.Errorf("blob %q not found", id)
	}
	f, err := os.Open(m.Path)
	if err != nil {
		return nil, BlobMeta{}, err
	}
	return f, m, nil
}
