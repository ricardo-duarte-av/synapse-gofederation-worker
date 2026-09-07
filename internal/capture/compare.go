package capture

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

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
	"m.direct_to_device":   true,
	"m.device_list_update": true,
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
	"m.receipt": {false,
		"batched by arrival, so the grouping differs between senders even when the receipts do not"},
	"m.typing":   {false, "a snapshot of who was typing when the transaction was built"},
	"m.presence": {false, "a snapshot of presence when the transaction was built"},
}

// Compare diffs two sets of records for one destination.
func Compare(destination string, synapse, worker []Record) (Diff, error) {
	d := Diff{Destination: destination}

	synPDUs, synEDUs, n, err := flatten(synapse, destination)
	if err != nil {
		return Diff{}, err
	}
	d.SynapseTransactions = n

	ourPDUs, ourEDUs, n, err := flatten(worker, destination)
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

	d.EDUs = compareEDUs(synEDUs, ourEDUs)
	return d, nil
}

// flatten reduces a capture to its PDUs by event id and its EDUs by type.
//
// PDU bytes are re-canonicalised before comparison. Both sides already emit
// canonical JSON, so this changes nothing in practice -- but it means a
// difference reported here is a difference in CONTENT rather than in encoding,
// which is the only kind worth reading.
func flatten(records []Record, destination string) (
	pdus map[string]pduEntry, edus map[string][]string, transactions int, err error,
) {
	pdus = map[string]pduEntry{}
	edus = map[string][]string{}

	for _, r := range records {
		if destination != "" && r.Destination != destination {
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
