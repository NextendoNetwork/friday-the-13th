package main

// messages.go routes NEX MessageDelivery (0x1B) by the message's own recipient. Friday the 13th
// sends DeliverMessage when joining a friend from the Friends list: the sender is not in the
// friend's lobby yet, so the shared handler (which broadcasts inside the sender's own lobby)
// delivered it to nobody and the join failed with "Game session does not exist!".

import (
	"encoding/binary"
	"fmt"

	nex "github.com/NextendoNetwork/nextendo-nex"
)

const (
	recipientPrincipal = 1
	recipientGathering = 2
)

// messageRecipient reads AnyDataHolder<UserMessage> far enough to find its MessageRecipient.
func messageRecipient(body []byte, s *nex.Settings) (typeName string, rType uint32, pid uint64, gid uint32, ok bool) {
	in := nex.NewStreamIn(body, s)
	typeName = in.String()
	_ = in.U32() // total length
	_ = in.U32() // data length
	if !s.StructHeader {
		return typeName, 0, 0, 0, false
	}
	_ = in.U8()        // Data level version
	_ = in.Substream() // Data level (no fields)
	_ = in.U8()        // UserMessage level version
	um := in.Substream()
	_ = um.U32()    // id
	_ = um.U32()    // parent id
	_ = um.PID()    // sender
	_ = um.U64()    // reception time
	_ = um.U32()    // lifetime
	_ = um.U32()    // flags
	_ = um.String() // subject
	_ = um.String() // sender name
	_ = um.U8()     // MessageRecipient version
	rc := um.Substream()
	rType = rc.U32()
	pid = rc.PID()
	gid = rc.U32()
	if in.Err() != nil || um.Err() != nil || rc.Err() != nil {
		return typeName, 0, 0, 0, false
	}
	return typeName, rType, pid, gid, true
}

// stampSender writes the sender's PID into UserMessage.m_pidSender, as the real server does on
// delivery: clients send 0 there, and the recipient answers whoever that field names (Friday
// the 13th replies to a "QUERY" with "SESSION" = "NEX|<gid>" addressed to it). Layout with
// struct headers: string type name, u32 total, u32 length, Data level (u8 + u32 0), UserMessage
// level (u8 + u32), id u32, parent id u32, then the sender u64.
func stampSender(body []byte, pid uint64) []byte {
	if len(body) < 2 {
		return body
	}
	nameLen := int(binary.LittleEndian.Uint16(body))
	off := 2 + nameLen + 4 + 4 + 5 + 5 + 4 + 4
	if off+8 > len(body) {
		return body
	}
	out := append([]byte(nil), body...)
	binary.LittleEndian.PutUint64(out[off:], pid)
	return out
}

func messageDeliveryHandler(mm *nex.Matchmaking) nex.RMCHandler {
	broadcast := mm.MessageDeliveryHandler()
	return func(c *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
		typeName, rType, pid, gid, ok := messageRecipient(req.Body, c.Settings)
		fmt.Printf("[Friday13 Msg] from=%d type=%q recipientType=%d pid=%d gid=%d parsed=%v body=%x\n",
			c.PID, typeName, rType, pid, gid, ok, req.Body)
		if !ok {
			return broadcast(c, req)
		}

		var targets []uint64
		switch rType {
		case recipientPrincipal:
			targets = []uint64{pid}
		case recipientGathering:
			for _, g := range mm.Snapshot() {
				if g.ID == gid {
					targets = g.Participants
					break
				}
			}
		default:
			return broadcast(c, req)
		}

		body := stampSender(req.Body, c.PID)
		delivered := 0
		for _, t := range targets {
			if t == c.PID {
				continue
			}
			if dst := c.Endpoint.FindConnectionByPID(t); dst != nil {
				dst.SendRMC(nex.NewRMCRequest(dst.Settings, nex.ProtocolMessageDelivery, nex.MethodDeliverMessage, req.CallID, body))
				delivered++
			}
		}
		fmt.Printf("[Friday13 Msg] from=%d -> delivered to %d of %d recipient(s)\n", c.PID, delivered, len(targets))
		return nil // DeliverMessage has no RMC response
	}
}
