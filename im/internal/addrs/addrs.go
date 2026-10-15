// Package addrs 提供把 libp2p 监听地址转换成"可被其它 peer 拨号"的地址。
//
// libp2p 监听在 0.0.0.0/:: 时,host.Addrs() 返回的是通配地址,其它机器无法
// 直接拨号。这里把通配 IP 展开成本机各网卡的具体 IP(含回环),让局域网内的
// 点对点发现真正可用。
package addrs

import (
	"net"
	"strings"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/multiformats/go-multiaddr"
)

// Reachable 返回 host 当前可用的拨号地址字符串列表。
func Reachable(h host.Host) []string {
	v4, v6 := interfaceIPs()
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, ma := range h.Addrs() {
		for _, e := range expandWildcard(ma, v4, v6) {
			s := e.String()
			if _, ok := seen[s]; ok {
				continue
			}
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

func interfaceIPs() (v4, v6 []string) {
	v4 = append(v4, "127.0.0.1")
	v6 = append(v6, "::1")
	ifaces, err := net.InterfaceAddrs()
	if err != nil {
		return v4, v6
	}
	for _, a := range ifaces {
		ipnet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ip := ipnet.IP
		if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
			continue
		}
		if v := ip.To4(); v != nil {
			v4 = append(v4, v.String())
		} else if ip.To16() != nil {
			v6 = append(v6, ip.String())
		}
	}
	return v4, v6
}

func expandWildcard(ma multiaddr.Multiaddr, v4, v6 []string) []multiaddr.Multiaddr {
	s := ma.String()
	if ip, err := ma.ValueForProtocol(multiaddr.P_IP4); err == nil && ip == "0.0.0.0" && len(v4) > 0 {
		return rebuild(s, "/ip4/0.0.0.0", "/ip4/", v4)
	}
	if ip, err := ma.ValueForProtocol(multiaddr.P_IP6); err == nil && ip == "::" && len(v6) > 0 {
		return rebuild(s, "/ip6/::", "/ip6/", v6)
	}
	return []multiaddr.Multiaddr{ma}
}

func rebuild(s, old, prefix string, ips []string) []multiaddr.Multiaddr {
	out := make([]multiaddr.Multiaddr, 0, len(ips))
	for _, ip := range ips {
		if nm, err := multiaddr.NewMultiaddr(strings.Replace(s, old, prefix+ip, 1)); err == nil {
			out = append(out, nm)
		}
	}
	return out
}
