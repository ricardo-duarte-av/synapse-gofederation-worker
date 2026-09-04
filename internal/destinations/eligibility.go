// Package destinations decides which remote servers an event is sent to.
//
// This is the part of the sender with the most room to be subtly wrong, because
// almost every mistake produces a plausible answer: a filter that is too eager
// silently stops sending some class of event, and one that is too lax sends
// events remote servers will reject. Neither shows up as an error. So each rule
// here names the Synapse code it comes from, and the tests assert the DEFAULT
// for every flag as well as the flag itself.
package destinations

import (
	"strings"

	"github.com/tidwall/gjson"
)

// Metadata is the subset of an event's internal_metadata the sender needs.
//
// None of these three are in the event body; they live in
// event_json.internal_metadata, and most events have an empty object there --
// which is exactly why the defaults matter more than the parsing.
type Metadata struct {
	// OutOfBandMembership defaults to false (internal_metadata.rs:308).
	OutOfBandMembership bool
	// ProactivelySend defaults to TRUE (internal_metadata.rs:355). Getting this
	// default wrong is the worst available bug in this file: almost every event
	// has `{}` metadata, so defaulting to false would drop essentially all
	// federation traffic while every individual check still looked correct.
	ProactivelySend bool
	// SendOnBehalfOf is the server we are forwarding an event for, used by
	// send_join. Empty when unset.
	SendOnBehalfOf string
}

// ParseMetadata reads event_json.internal_metadata.
//
// Unparseable metadata yields the defaults rather than an error. Synapse builds
// the object from whatever keys are present and treats a missing key as its
// default, so there is no such thing as metadata it rejects; failing here would
// invent a failure Synapse does not have.
func ParseMetadata(raw []byte) Metadata {
	m := Metadata{ProactivelySend: true}
	if len(raw) == 0 {
		return m
	}
	r := gjson.ParseBytes(raw)
	if v := r.Get("out_of_band_membership"); v.Exists() {
		m.OutOfBandMembership = v.Bool()
	}
	if v := r.Get("proactively_send"); v.Exists() {
		m.ProactivelySend = v.Bool()
	}
	if v := r.Get("send_on_behalf_of"); v.Exists() {
		m.SendOnBehalfOf = v.String()
	}
	return m
}

// SkipReason says why an event is not federated, or "" if it is.
type SkipReason string

// The reasons, matching the checks in federation/sender/__init__.py:554-604.
const (
	SkipNotOurs        SkipReason = "remote origin"
	SkipOutOfBand      SkipReason = "out-of-band membership"
	SkipNotProactive   SkipReason = "proactively_send is false"
	SkipRejected       SkipReason = "rejected"
	SkipNoPrevEvents   SkipReason = "no prev events"
	SkipNoDestinations SkipReason = "no remote destinations"
)

// Eligible reports whether an event should be federated at all.
//
// The three rules are Synapse's, in its order
// (federation/sender/__init__.py:556-604):
//
//  1. Send only OUR events, unless we are forwarding on another server's
//     behalf. A remote-origin event is that server's job to distribute.
//  2. Never send out-of-band memberships: invites received over federation,
//     rejections of invites to federated rooms, rescinded knocks, and knocks we
//     sent. In every case the other server is responsible, and these are
//     outliers, so trying to fetch the room's membership for one would fail
//     anyway.
//  3. Never send events flagged proactively_send: false, which is how Synapse
//     marks dummy events that exist only to advance the DAG.
func Eligible(sender, serverName string, m Metadata, rejectionReason string) (bool, SkipReason) {
	// Rejected events are not in Synapse's list because a rejected event never
	// reaches the sender's loop -- but we read the same table without that
	// filtering, so the check has to be made here instead.
	if rejectionReason != "" {
		return false, SkipRejected
	}
	if !isMine(sender, serverName) && m.SendOnBehalfOf == "" {
		return false, SkipNotOurs
	}
	if m.OutOfBandMembership {
		return false, SkipOutOfBand
	}
	if !m.ProactivelySend {
		return false, SkipNotProactive
	}
	return true, ""
}

// DomainOf is Synapse's get_domain_from_id: everything after the first colon.
func DomainOf(id string) string {
	_, domain, ok := strings.Cut(id, ":")
	if !ok {
		return ""
	}
	return domain
}

func isMine(id, serverName string) bool {
	return DomainOf(id) == serverName
}
