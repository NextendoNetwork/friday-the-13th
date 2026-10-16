package main

// stations.go gives joiners the host's real UDP endpoints, the way the Dragon Ball FighterZ
// server does (its NEX P2P, also Unreal over plain NEX stations, works between two instances on
// one PC). Register precedes the game's UDP socket and advertises port 1; the station a player
// sends with RequestProbeInitiationExt carries its actual endpoint plus its LAN candidate
// (Ntrpa/Ntrpp). Learn that per authenticated connection, never per IP, and when answering
// GetSessionURLs only refresh address/port: transport, stream, type and identity stay exactly
// as registered.

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	nex "github.com/NextendoNetwork/nextendo-nex"
)

const (
	protoNATTraversal               = 0x3
	methodRequestProbeInitiationExt = 0x3
	methodReportNATProperties       = 0x5
)

type peerEndpoints struct {
	mu       sync.RWMutex
	stations map[uint32][]*nex.StationURL // connection ID -> [public, local]
	natPorts map[uint32]int               // connection ID -> nncs port seen at its NAT check
}

var peers = &peerEndpoints{stations: map[uint32][]*nex.StationURL{}, natPorts: map[uint32]int{}}

func (p *peerEndpoints) remove(c *nex.Connection) {
	p.mu.Lock()
	delete(p.stations, c.ID)
	delete(p.natPorts, c.ID)
	p.mu.Unlock()
}

// learn records the endpoint a player advertised for itself (from DBFZ peer_endpoints.go).
func (p *peerEndpoints) learn(c *nex.Connection, u *nex.StationURL) {
	ip := net.ParseIP(u.Get("address"))
	port := u.GetInt("port")
	if ip == nil || ip.IsUnspecified() || ip.IsMulticast() || ip.IsLoopback() || port <= 1 || port > 65535 {
		return
	}
	u = u.Copy()
	u.Remove("PRID")
	u.Remove("probeinit")
	u.Set("PID", strconv.FormatUint(c.PID, 10))
	u.SetInt("RVCID", int(c.ID))

	p.mu.Lock()
	defer p.mu.Unlock()
	var local, public *nex.StationURL
	for _, old := range p.stations[c.ID] {
		if addr := net.ParseIP(old.Get("address")); addr != nil && addr.IsPrivate() {
			local = old
		} else {
			public = old
		}
	}
	if ip.IsPrivate() {
		local = u
		local.Remove("type")
		local.Remove("Pa")
	} else {
		observed, _, err := net.SplitHostPort(c.RemoteAddr)
		if err != nil || !ip.Equal(net.ParseIP(observed)) {
			return
		}
		public = u
		lanIP := net.ParseIP(u.Get("Ntrpa"))
		lanPort := u.GetInt("Ntrpp")
		if lanIP != nil && lanIP.IsPrivate() && lanPort > 1 && lanPort <= 65535 {
			local = u.Copy()
			local.Set("address", lanIP.String())
			local.SetInt("port", lanPort)
			local.Remove("type")
			local.Remove("Pa")
		}
	}
	urls := make([]*nex.StationURL, 0, 2)
	if public != nil {
		urls = append(urls, public)
	}
	if local != nil {
		urls = append(urls, local)
	}
	p.stations[c.ID] = urls
	fmt.Printf("[Friday13 Stations] learned connection=%d pid=%d UDP port=%d candidates=%d\n", c.ID, c.PID, port, len(urls))
}

// natPortFromFile reads the nncs responder's current observation for ip straight from its file
// (the library's copy is cached for 2 s, long enough to hand back another player's port).
func natPortFromFile(ip string) int {
	data, err := os.ReadFile(envOr("NNCS_NAT_FILE", "/nat/nat_endpoints.txt"))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if f := strings.Fields(line); len(f) == 2 && f[0] == ip {
			if port, err := strconv.Atoi(f[1]); err == nil && port > 0 && port < 65536 {
				return port
			}
		}
	}
	return 0
}

// noteNAT learns endpoints from the NAT traversal calls a player makes.
func noteNAT(c *nex.Connection, req *nex.RMCMessage) {
	if req.Protocol != protoNATTraversal {
		return
	}
	switch req.Method {
	case methodReportNATProperties:
		// The NAT check just ran, so a shared-IP nncs entry is this player's own.
		if ip, _, err := net.SplitHostPort(c.RemoteAddr); err == nil {
			if port := natPortFromFile(ip); port != 0 {
				peers.mu.Lock()
				peers.natPorts[c.ID] = port
				peers.mu.Unlock()
				fmt.Printf("[Friday13 Stations] connection=%d pid=%d NAT check -> udp port %d\n", c.ID, c.PID, port)
			}
		}
	case methodRequestProbeInitiationExt:
		in := nex.NewStreamIn(req.Body, c.Settings)
		n := in.U32()
		if n > 64 {
			return
		}
		for i := uint32(0); i < n; i++ {
			_ = in.String()
		}
		own := in.String()
		if in.Err() == nil {
			if u := nex.ParseStationURL(own); u != nil {
				peers.learn(c, u)
			}
		}
	}
}

// sessionStations answers GetSessionURLs with the host's registered stations, refreshing only
// address and port from what the host itself advertised (or, before it has, its NAT check).
func sessionStations(joiner, host *nex.Connection) []*nex.StationURL {
	raw := host.Stations()
	natMap, natFilter := host.NATProps()
	peers.mu.RLock()
	learned := peers.stations[host.ID]
	natPort := peers.natPorts[host.ID]
	peers.mu.RUnlock()

	var public, local *nex.StationURL
	for _, u := range raw {
		corrected := u.Copy()
		// Register happens before NAT discovery; use the host's reported result.
		corrected.SetInt("natm", int(natMap))
		corrected.SetInt("natf", int(natFilter))
		ip := net.ParseIP(u.Get("address"))
		fixed := false
		for _, e := range learned {
			eip := net.ParseIP(e.Get("address"))
			if ip == nil || eip == nil || ip.IsPrivate() != eip.IsPrivate() {
				continue
			}
			corrected.Set("address", e.Get("address"))
			corrected.SetInt("port", e.GetInt("port"))
			fixed = true
			break
		}
		if !fixed && natPort != 0 {
			corrected.SetInt("port", natPort)
		}
		if ip != nil && ip.IsPrivate() {
			local = corrected
		} else {
			public = corrected
		}
	}
	out := make([]*nex.StationURL, 0, 2)
	if public != nil {
		out = append(out, public)
	}
	if local != nil {
		out = append(out, local)
	}
	source := "nat-check"
	if len(learned) > 0 {
		source = "advertised"
	}
	fmt.Printf("[Friday13 Stations] host=%d joiner=%d source=%s\n", host.PID, joiner.PID, source)
	return out
}
