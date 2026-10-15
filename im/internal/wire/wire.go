// Package wire 定义了 IM 应用层的线路协议:一个极简的定长帧编解码器。
//
// 帧格式:
//
//	[1 byte type][4 byte big-endian payload length][payload...]
//
//	type 取值:
//	  0x01 ctrl  控制帧,payload 为 JSON 编码的 Msg
//	  0x02 blob  二进制分块帧,payload 为媒体文件的一段原始字节
//	  0x03 end   结束帧,无 payload,标记一次媒体传输结束
//
// 之所以用这种"控制 JSON + 二进制分块"的组合,是为了让文字/表情走轻量 JSON,
// 而图片/文件/语音等媒体直接走原始字节,避免 base64 膨胀。
package wire

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

const (
	FrameCtrl byte = 0x01
	FrameBlob byte = 0x02
	FrameEnd  byte = 0x03
)

// maxFrame 单帧最大长度,防止对端发送超大帧导致内存耗尽。
const maxFrame = 4 << 20 // 4 MiB

// Msg 是控制平面的消息体,既用于 peer 之间的聊天,也用于与协调服务器的交互。
type Msg struct {
	ID       string `json:"id,omitempty"`
	Type     string `json:"type,omitempty"`
	From     string `json:"from,omitempty"`
	Nick     string `json:"nick,omitempty"`
	Room     string `json:"room,omitempty"`
	Text     string `json:"text,omitempty"`
	FileName string `json:"file_name,omitempty"`
	Mime     string `json:"mime,omitempty"`
	Size     int64  `json:"size,omitempty"`
	Duration int64  `json:"duration,omitempty"` // 语音时长(毫秒)
	Ts       int64  `json:"ts,omitempty"`

	// 协调(rendezvous)协议字段
	Op    string     `json:"op,omitempty"` // register / discover
	OK    bool       `json:"ok,omitempty"`
	Error string     `json:"error,omitempty"`
	Peers []PeerAddr `json:"peers,omitempty"`
	Addrs []string   `json:"addrs,omitempty"`
}

// PeerAddr 描述协调服务器返回的一个在线 peer。
type PeerAddr struct {
	ID    string   `json:"id"`
	Nick  string   `json:"nick,omitempty"`
	Addrs []string `json:"addrs"`
}

// 消息类型常量
const (
	TypeText  = "text"
	TypeEmoji = "emoji"
	TypeFile  = "file"
	TypeImage = "image"
	TypeVoice = "voice"
)

// Writer 带帧写入。
type Writer struct {
	w *bufio.Writer
}

func NewWriter(w io.Writer) *Writer { return &Writer{w: bufio.NewWriter(w)} }

func (w *Writer) WriteCtrl(m Msg) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return w.writeFrame(FrameCtrl, b)
}

func (w *Writer) WriteBlob(p []byte) error { return w.writeFrame(FrameBlob, p) }

func (w *Writer) WriteEnd() error { return w.writeFrame(FrameEnd, nil) }

func (w *Writer) writeFrame(t byte, p []byte) error {
	var hdr [5]byte
	hdr[0] = t
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(p)))
	if _, err := w.w.Write(hdr[:]); err != nil {
		return err
	}
	if len(p) > 0 {
		if _, err := w.w.Write(p); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) Flush() error { return w.w.Flush() }

// Reader 带帧读取。
type Reader struct {
	r *bufio.Reader
}

func NewReader(r io.Reader) *Reader { return &Reader{r: bufio.NewReader(r)} }

// ReadFrame 读取一个帧,返回帧类型与 payload。FrameEnd 的 payload 为 nil。
func (r *Reader) ReadFrame() (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r.r, hdr[:]); err != nil {
		return 0, nil, err
	}
	t := hdr[0]
	n := binary.BigEndian.Uint32(hdr[1:])
	if n > maxFrame {
		return 0, nil, fmt.Errorf("wire: frame too large: %d bytes", n)
	}
	if n == 0 {
		return t, nil, nil
	}
	p := make([]byte, n)
	if _, err := io.ReadFull(r.r, p); err != nil {
		return 0, nil, err
	}
	return t, p, nil
}

// ReadCtrl 读取一个控制帧并解码为 Msg。
func (r *Reader) ReadCtrl() (Msg, error) {
	t, p, err := r.ReadFrame()
	if err != nil {
		return Msg{}, err
	}
	if t != FrameCtrl {
		return Msg{}, fmt.Errorf("wire: expected ctrl frame, got %#x", t)
	}
	var m Msg
	if err := json.Unmarshal(p, &m); err != nil {
		return Msg{}, err
	}
	return m, nil
}
