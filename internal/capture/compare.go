package capture

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"maunium.net/go/mautrix/crypto/canonicaljson"
)

// Diff is the result of comparing two captures of the same destination.
//
// Note what is NOT in here: anything about transaction boundaries, transaction
// ids or origin_server_ts. Which PDUs share a transaction depends on what was
// queued when a sender's loop happened to run, so two correct senders differ
// there and a diff of it would be a diff of timing. Both captures are flattened
// to the things that carry meaning -- which events reached the destination,
// with what bytes, and which EDUs went with them.
type Diff struct {
	Destination string
	// Window is the period compared. Records outside it are ignored on both
	// sides.
	Window Window
	// Compared is how many distinct PDUs the comparison actually looked at.
	//
	// Zero means the comparison proved nothing, and a caller must not read
	// that as agreement. An empty comparison reporting a green light is worse
	// than no comparison at all, because it gets believed.
	Compared int

	// Transactions on each side, reported for context only. A difference here
	// is expected and is not a finding.
	SynapseTransactions int
	WorkerTransactions  int

	// PDUs, compared as a set of event ids.
	// PDUsIdentifiedByContent is how many PDUs had no event_id and were matched
	// on a content hash instead. When this is non-zero, a MISSING and an EXTRA
	// may be the same event with different content rather than two lost ones.
	PDUsIdentifiedByContent int

	PDUsBoth        int
	PDUsOnlySynapse []PDURef
	PDUsOnlyWorker  []PDURef
	// PDUsDiffering are event ids present on both sides whose serialised bytes
	// are not identical. This is the finding that matters most: the same event
	// delivered differently.
	PDUsDiffering []PDUDiff

	// EDUs, compared per type. Content is compared where it is deterministic
	// and merely counted where it is not; see EDUCompare.
	EDUs []EDUDiff
}

// PDUDiff is one event whose bytes differ between the two senders.
type PDUDiff struct {
	EventID string `json:"event_id"`
	Synapse string `json:"synapse"`
	Worker  string `json:"worker"`
}

// PDURef names one PDU in a report.
//
// Room version 3 and later derive the event id from the content and do not
// carry it in the event, and this deployment's default room version is 12 -- so
// in practice almost NO PDU here has an event_id field. Computing the real
// Matrix id would need the room version, which is not in the PDU and not
// available to an offline tool, so the identity is a content hash instead.
//
// The consequence is worth stating plainly: for a content-identified PDU, a
// changed event appears as one MISSING and one EXTRA rather than as a DIFFERS,
// because there is nothing that says the two are the same event. Display
// carries room, type, sender and timestamp so the event can still be found.
type PDURef struct {
	// ID is the matching identity: the event_id when the event carries one,
	// otherwise "sha:" plus a content hash.
	ID string `json:"id"`
	// Display is a human-findable description.
	Display string `json:"display"`
}

// EDUDiff is one EDU type's comparison.
type EDUDiff struct {
	Type    string `json:"edu_type"`
	Synapse int    `json:"synapse"`
	Worker  int    `json:"worker"`
	// ContentBoth and ContentOnly* compare canonical EDU contents as multisets,
	// for the types where that is meaningful.
	ContentBoth        int      `json:"content_both"`
	ContentOnlySynapse []string `json:"content_only_synapse,omitempty"`
	ContentOnlyWorker  []string `json:"content_only_worker,omitempty"`
	// Comparable is false for EDU types whose content legitimately differs
	// between two senders, so a reader does not treat the counts as failures.
	Comparable bool   `json:"comparable"`
	Note       string `json:"note,omitempty"`
	// Implemented is false for EDU types this worker does not send at all.
	//
	// Without it a permanently-zero worker column reads like "none happened in
	// this window" when it actually means "this is not built yet" -- which is
	// exactly the kind of gap that goes unnoticed for weeks because the report
	// looked fine.
	Implemented bool `json:"implemented"`
}

// eduImplemented lists the EDU types this worker actually emits.
//
// Kept here rather than inferred from the capture, because inferring it is
// what produces the bug: a type nobody happened to send in the sample window
// is indistinguishable from one that was never built.
var eduImplemented = map[string]bool{
	"m.direct_to_device":            true,
	"m.device_list_update":          true,
	"m.signing_key_update":          true,
	"org.matrix.signing_key_update": true,
	"m.receipt":                     true,
	"m.typing":                      true,
	"m.presence":                    true,
}

// eduComparable says whether an EDU type's content can be compared at all.
//
// Some EDUs are a function of durable state and two correct senders must
// produce the same bytes. Others are snapshots of something that moves --
// presence and typing describe the moment the transaction was built, and
// receipts are batched by whatever arrived first -- so comparing their content
// between senders that ran at different instants tests the clock, not the code.
var eduComparable = map[string]struct {
	comparable bool
	note       string
}{
	"m.direct_to_device": {true, "content comes from device_federation_outbox and must match"},
	"m.device_list_update": {true,
		"content is built from the device tables; ours is deliberately incomplete in phase 1"},
	"m.signing_key_update": {true, "content is the user's cross-signing keys"},
	"org.matrix.signing_key_update": {true,
		"the pre-stabilisation name, sent alongside the stable one for servers that predate it"},
	"m.receipt": {false,
		"batched by arrival, so the grouping differs between senders even when the receipts do not"},
	"m.typing":   {false, "a snapshot of who was typing when the transaction was built"},
	"m.presence": {false, "a snapshot of presence when the transaction was built"},
}

// Window bounds a comparison in time.
//
// It is not optional in practice and leaving it out is the mistake this type
// exists to prevent. The recorder's file is cumulative -- everything since it
// was deployed -- while the worker's covers only the period the worker was
// running. Comparing the two whole files reports every transaction Synapse sent
// outside that period as MISSING, which is true and useless, and is exactly the
// false alarm the routing comparison's floor already had to solve.
type Window struct {
	Since time.Time
	Until time.Time

	// ByEventTime windows on the PDUs' own origin_server_ts instead of on when
	// each side processed the transaction.
	//
	// Needed whenever one side is replaying history: a worker re-processing
	// yesterday's events records them with today's timestamp, so the two
	// captures never overlap in processing time even though they describe the
	// same events. The event's own timestamp is a property of the event and is
	// identical on both sides however either got there.
	//
	// EDUs carry no such timestamp and are not filtered in this mode.
	ByEventTime bool
}

// Empty reports whether the window can contain anything at all.
//
// An inverted window -- Since after Until -- means the two captures observed
// disjoint periods, so there is nothing to compare. Saying so is essential: the
// alternative is a comparison over zero records reporting agreement, which is a
// green light that means nothing.
func (w Window) Empty() bool {
	return !w.Since.IsZero() && !w.Until.IsZero() && w.Since.After(w.Until)
}

// Contains reports whether a time falls in the window. A zero bound is open.
func (w Window) Contains(t time.Time) bool {
	if !w.Since.IsZero() && t.Before(w.Since) {
		return false
	}
	if !w.Until.IsZero() && t.After(w.Until) {
		return false
	}
	return true
}

// Overlap returns the period both captures could have observed: the
// intersection of their time ranges.
//
// This is the honest default. Anything outside it is a period only one side was
// watching, so a difference there says nothing about whether the two senders
// agree.
func Overlap(a, b []Record) Window {
	aMin, aMax := timeRange(a)
	bMin, bMax := timeRange(b)
	if aMin.IsZero() || bMin.IsZero() {
		return Window{}
	}
	w := Window{Since: aMin, Until: aMax}
	if bMin.After(w.Since) {
		w.Since = bMin
	}
	if bMax.Before(w.Until) {
		w.Until = bMax
	}
	return w
}

// OverlapByEventTime returns the intersection of the two captures' EVENT time
// ranges, taken from the PDUs' origin_server_ts.
//
// Use this when either side replayed history rather than following the stream
// live, which is the normal case while the worker is being run by hand.
func OverlapByEventTime(a, b []Record) Window {
	aMin, aMax := eventTimeRange(a)
	bMin, bMax := eventTimeRange(b)
	if aMin.IsZero() || bMin.IsZero() {
		return Window{ByEventTime: true}
	}
	w := Window{Since: aMin, Until: aMax, ByEventTime: true}
	if bMin.After(w.Since) {
		w.Since = bMin
	}
	if bMax.Before(w.Until) {
		w.Until = bMax
	}
	return w
}

func eventTimeRange(records []Record) (min, max time.Time) {
	for _, r := range records {
		for _, p := range gjson.GetBytes(r.Body, "pdus").Array() {
			ts := p.Get("origin_server_ts").Int()
			if ts == 0 {
				continue
			}
			t := time.UnixMilli(ts)
			if min.IsZero() || t.Before(min) {
				min = t
			}
			if max.IsZero() || t.After(max) {
				max = t
			}
		}
	}
	return min, max
}

func timeRange(records []Record) (min, max time.Time) {
	for _, r := range records {
		if r.Time.IsZero() {
			continue
		}
		if min.IsZero() || r.Time.Before(min) {
			min = r.Time
		}
		if max.IsZero() || r.Time.After(max) {
			max = r.Time
		}
	}
	return min, max
}

// ExcludeOurOwn removes records whose transaction id carries our prefix.
//
// The recorder sits in front of the test homeserver and captures EVERY
// transaction that arrives, which once this worker is sending for real
// includes its own. Comparing that file against the worker's would then be
// comparing the worker with itself -- a green light that means nothing and that
// nothing else would catch, because both halves genuinely match.
//
// The transaction id prefix is what makes the two separable, which is a second
// reason to keep it beyond avoiding id collisions.
func ExcludeOurOwn(records []Record, prefix string) (kept []Record, removed int) {
	if prefix == "" {
		return records, 0
	}
	kept = make([]Record, 0, len(records))
	for _, r := range records {
		if strings.HasPrefix(r.TxnID, prefix) {
			removed++
			continue
		}
		kept = append(kept, r)
	}
	return kept, removed
}

// Compare diffs two sets of records for one destination, within a window.
//
// The synapse side must not contain this worker's own transactions; pass it
// through ExcludeOurOwn first.
func Compare(destination string, window Window, synapse, worker []Record) (Diff, error) {
	d := Diff{Destination: destination, Window: window}

	synPDUs, synEDUs, n, err := flatten(synapse, destination, window)
	if err != nil {
		return Diff{}, err
	}
	d.SynapseTransactions = n

	ourPDUs, ourEDUs, n, err := flatten(worker, destination, window)
	if err != nil {
		return Diff{}, err
	}
	d.WorkerTransactions = n

	for id, syn := range synPDUs {
		if strings.HasPrefix(id, contentIDPrefix) {
			d.PDUsIdentifiedByContent++
		}
		ours, ok := ourPDUs[id]
		if !ok {
			d.PDUsOnlySynapse = append(d.PDUsOnlySynapse, syn.ref)
			continue
		}
		d.PDUsBoth++
		if syn.canonical != ours.canonical {
			d.PDUsDiffering = append(d.PDUsDiffering, PDUDiff{
				EventID: id, Synapse: syn.canonical, Worker: ours.canonical,
			})
		}
	}
	for id, ours := range ourPDUs {
		if _, ok := synPDUs[id]; !ok {
			d.PDUsOnlyWorker = append(d.PDUsOnlyWorker, ours.ref)
		}
	}
	sortRefs(d.PDUsOnlySynapse)
	sortRefs(d.PDUsOnlyWorker)
	sort.Slice(d.PDUsDiffering, func(i, j int) bool {
		return d.PDUsDiffering[i].EventID < d.PDUsDiffering[j].EventID
	})

	d.Compared = d.PDUsBoth + len(d.PDUsOnlySynapse) + len(d.PDUsOnlyWorker)
	d.EDUs = compareEDUs(synEDUs, ourEDUs)
	return d, nil
}

// flatten reduces a capture to its PDUs by event id and its EDUs by type.
//
// PDU bytes are re-canonicalised before comparison. Both sides already emit
// canonical JSON, so this changes nothing in practice -- but it means a
// difference reported here is a difference in CONTENT rather than in encoding,
// which is the only kind worth reading.
func flatten(records []Record, destination string, window Window) (
	pdus map[string]pduEntry, edus map[string][]string, transactions int, err error,
) {
	pdus = map[string]pduEntry{}
	edus = map[string][]string{}

	for _, r := range records {
		if destination != "" && r.Destination != destination {
			continue
		}
		// In processing-time mode the whole record is in or out. In
		// event-time mode the record always passes here and its PDUs are
		// judged individually below, on their own timestamps.
		if !window.ByEventTime && !window.Contains(r.Time) {
			continue
		}
		t, decodeErr := r.Decode()
		if decodeErr != nil {
			// A body we cannot parse is worth surfacing rather than skipping:
			// on the proxy side it would mean Synapse sent something
			// unexpected, which is itself the finding.
			return nil, nil, 0, decodeErr
		}
		transactions++

		for _, p := range t.PDUs {
			if window.ByEventTime {
				ts := gjson.GetBytes(p, "origin_server_ts").Int()
				if ts == 0 || !window.Contains(time.UnixMilli(ts)) {
					continue
				}
			}
			id, ref := identify(p)
			pdus[id] = pduEntry{canonical: canonical(p), ref: ref}
		}
		for _, e := range t.EDUs {
			edus[e.Type] = append(edus[e.Type], canonical(e.Content))
		}
	}
	return pdus, edus, transactions, nil
}

func canonical(raw json.RawMessage) string {
	b, err := canonicaljson.Marshal(raw)
	if err != nil {
		return string(raw)
	}
	return string(b)
}

func compareEDUs(synapse, worker map[string][]string) []EDUDiff {
	types := map[string]bool{}
	for t := range synapse {
		types[t] = true
	}
	for t := range worker {
		types[t] = true
	}

	out := make([]EDUDiff, 0, len(types))
	for t := range types {
		meta := eduComparable[t]
		d := EDUDiff{
			Type: t, Synapse: len(synapse[t]), Worker: len(worker[t]),
			Comparable: meta.comparable, Note: meta.note,
			Implemented: eduImplemented[t],
		}
		if meta.comparable {
			d.ContentBoth, d.ContentOnlySynapse, d.ContentOnlyWorker =
				diffMultiset(synapse[t], worker[t])
		}
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Type < out[j].Type })
	return out
}

// diffMultiset compares two bags of canonical contents.
//
// A multiset rather than a set: the same EDU content can legitimately appear
// twice, and collapsing duplicates would hide a sender emitting one where the
// other emitted two.
func diffMultiset(a, b []string) (both int, onlyA, onlyB []string) {
	counts := map[string]int{}
	for _, s := range a {
		counts[s]++
	}
	for _, s := range b {
		if counts[s] > 0 {
			counts[s]--
			both++
			continue
		}
		onlyB = append(onlyB, s)
	}
	for s, n := range counts {
		for i := 0; i < n; i++ {
			onlyA = append(onlyA, s)
		}
	}
	sort.Strings(onlyA)
	sort.Strings(onlyB)
	return both, onlyA, onlyB
}

// Agreed reports whether the diff shows no PDU-level disagreement.
//
// EDUs are deliberately excluded from the verdict for now: the
// m.device_list_update body is knowingly incomplete in phase 1, so a red light
// there would be permanently on and would stop meaning anything.
func (d Diff) Agreed() bool {
	return len(d.PDUsOnlySynapse) == 0 &&
		len(d.PDUsOnlyWorker) == 0 &&
		len(d.PDUsDiffering) == 0
}

// Inconclusive reports that the comparison looked at nothing.
//
// Distinguished from agreement on purpose. A run that compared zero PDUs has
// established nothing, and reporting it as a pass is how a broken rig goes
// unnoticed -- the output is green and nobody asks what it was green about.
func (d Diff) Inconclusive() bool { return d.Compared == 0 || d.Window.Empty() }

// contentIDPrefix marks an identity derived from the content rather than read
// from an event_id field.
const contentIDPrefix = "sha:"

type pduEntry struct {
	canonical string
	ref       PDURef
}

// identify returns a PDU's matching identity and a human-findable description.
func identify(p json.RawMessage) (string, PDURef) {
	canon := canonical(p)
	r := gjson.Parse(canon)

	display := fmt.Sprintf("%s in %s from %s at %d",
		orUnknown(r.Get("type").String()),
		orUnknown(r.Get("room_id").String()),
		orUnknown(r.Get("sender").String()),
		r.Get("origin_server_ts").Int())

	if id := r.Get("event_id").String(); id != "" {
		return id, PDURef{ID: id, Display: id + " (" + display + ")"}
	}

	// No event_id: room version 3 and later derive it from the content. A
	// short content hash is a stable identity that does not pretend to be the
	// Matrix event id.
	sum := sha256.Sum256([]byte(canon))
	id := contentIDPrefix + hex.EncodeToString(sum[:8])
	return id, PDURef{ID: id, Display: id + " (" + display + ")"}
}

func orUnknown(s string) string {
	if s == "" {
		return "?"
	}
	return s
}

func sortRefs(refs []PDURef) {
	sort.Slice(refs, func(i, j int) bool { return refs[i].Display < refs[j].Display })
}
