package main

// stations.go hands joiners the host stations Friday the 13th's NEX P2P (UNEXNetDriver, not
// Pia) can use. The shared bridge shapes stations for Pia (type=11, Pa=<private>, natf/natm
// from Register = 0) and guesses the UDP port from the nncs observation per public IP, which
// picks the wrong player when two share an IP. Here the port comes from the station the host
// reported itself in RequestProbeInitiationExt when known, and the shape follows plain NEX:
// type=3, no Pa, the host's reported NAT mapping/filtering.

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
)

var (
	selfPortsMu sync.Mutex
	selfPorts   = map[uint64]int{} // PID -> public UDP port its own NEX reported
)

const methodReportNATProperties = 0x5

// natPortFromFile reads the nncs responder's current observation for ip straight from its file
// (the library's copy is cached for 2 s, long enough to hand back the other player's port).
func natPortFromFile(ip string) int {
	data, err := os.ReadFile(envOr("NNCS_NAT_FILE", "/nat/nat_endpoints.txt"))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if f := strings.Fields(line); len(f) == 2 && f[0] == ip {
			if p, err := strconv.Atoi(f[1]); err == nil && p > 0 && p < 65536 {
				return p
			}
		}
	}
	return 0
}

// noteSelfStation records each player's own public UDP port: from the nncs observation at the
// moment it reports its NAT properties (its NAT check just ran, so a shared-IP entry is its
// own), and from the station it advertises when it asks for NAT probes
// (list<string> targets, string stationToProbe).
func noteSelfStation(c *nex.Connection, req *nex.RMCMessage) {
	if req.Protocol != protoNATTraversal {
		return
	}
	if req.Method == methodReportNATProperties {
		if ip, _, err := net.SplitHostPort(c.RemoteAddr); err == nil {
			if port := natPortFromFile(ip); port != 0 {
				selfPortsMu.Lock()
				selfPorts[c.PID] = port
				selfPortsMu.Unlock()
				fmt.Printf("[Friday13 Stations] pid=%d NAT check -> udp port %d\n", c.PID, port)
			}
		}
		return
	}
	if req.Method != methodRequestProbeInitiationExt {
		return
	}
	in := nex.NewStreamIn(req.Body, c.Settings)
	_ = nex.ReadList(in, func(i *nex.StreamIn) string { return i.String() })
	station := nex.ParseStationURL(in.String())
	if in.Err() != nil || station == nil {
		return
	}
	if port := station.GetInt("port"); port > 0 && port < 65536 {
		selfPortsMu.Lock()
		selfPorts[c.PID] = port
		selfPortsMu.Unlock()
	}
}

func selfPort(pid uint64) int {
	selfPortsMu.Lock()
	defer selfPortsMu.Unlock()
	return selfPorts[pid]
}

func isPrivateAddr(s string) bool {
	ip := net.ParseIP(s)
	return ip != nil && (ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast())
}

// sessionStations builds [lan, public] for host as seen by joiner.
func sessionStations(joiner, host *nex.Connection) []*nex.StationURL {
	raw := host.Stations()
	var local, public *nex.StationURL
	for _, u := range raw {
		if isPrivateAddr(u.Get("address")) {
			if local == nil {
				local = u
			}
		} else if public == nil {
			public = u
		}
	}
	if local == nil || public == nil {
		return raw
	}

	port, source := selfPort(host.PID), "self-reported"
	if port == 0 {
		observed, ok := nex.NatPortForIP(public.Get("address"))
		if !ok {
			fmt.Printf("[Friday13 Stations] host=%d: no port known -> raw stations\n", host.PID)
			return raw
		}
		port, source = observed, "nncs"
		// Two players behind one IP share a single nncs entry, which then names the joiner.
		if j := selfPort(joiner.PID); j != 0 && j == observed && joiner.PID != host.PID {
			fmt.Printf("[Friday13 Stations] host=%d: nncs port %d is the joiner's own -> raw stations\n", host.PID, observed)
			return raw
		}
	}

	natMap, natFilter := host.NATBehaviour()
	// Register's URLs carry the client↔server transport (Tpt=2, PRUDP over WebSocket); the
	// stations NEX builds for its own peer probes use Tpt=0, the UDP transport peers speak.
	lan := local.Copy()
	lan.SetInt("Tpt", 0)
	lan.SetInt("port", port)
	lan.SetInt("natm", int(natMap))
	lan.SetInt("natf", int(natFilter))
	lan.Remove("type")
	lan.Remove("Pa")

	pub := public.Copy()
	pub.SetInt("Tpt", 0)
	pub.SetInt("port", port)
	pub.SetInt("natm", int(natMap))
	pub.SetInt("natf", int(natFilter))
	pub.SetInt("type", 3)
	pub.Remove("Pa")

	fmt.Printf("[Friday13 Stations] host=%d joiner=%d port=%d (%s) natm=%d natf=%d\n",
		host.PID, joiner.PID, port, source, natMap, natFilter)
	return []*nex.StationURL{lan, pub}
}
