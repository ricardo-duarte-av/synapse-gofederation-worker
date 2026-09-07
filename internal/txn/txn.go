// Package txn assembles and signs federation transactions.
//
// The signing is done with mautrix-go's federation.SigningKey rather than by
// hand. Canonical JSON and the X-Matrix header are places where a subtle
// mistake is a signature every remote server rejects -- or worse, one that
// happens to verify over the wrong bytes -- and they are exactly the kind of
// thing a shared, tested implementation should own.
//
// Transactions are built and signed even in shadow mode, deliberately. Signing
// is cheap, it is a step that can be wrong, and a signature computed over the
// wrong bytes is the class of bug that stays invisible until the day we go
// live. The signature covers a uri/origin/destination/content object that never
// leaves the process, so producing one has no external effect.
package txn

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"maunium.net/go/mautrix/crypto/canonicaljson"
	"maunium.net/go/mautrix/federation"
)

// SendPathPrefix is the federation transaction endpoint
// (synapse/api/urls.py:35 plus transport/client.py:257).
const SendPathPrefix = "/_matrix/federation/v1/send/"

// Transaction is the body PUT to a remote server.
//
// Note what is NOT here: transaction_id and destination. Synapse's Transaction
// object carries both, but get_dict drops them (federation/units.py:104) -- the
// txn id travels in the URL only. Including them would change the bytes we sign
// and every remote would reject the signature.
type Transaction struct {
	Origin         string            `json:"origin"`
	OriginServerTS int64             `json:"origin_server_ts"`
	PDUs           []json.RawMessage `json:"pdus"`
	// EDUs is omitted when empty, matching Synapse.
	EDUs []EDU `json:"edus,omitempty"`
}

// EDU is one ephemeral data unit. Synapse's Edu.get_dict emits exactly these
// two fields (federation/units.py:52); origin and destination are internal to
// the sender and are not put on the wire.
type EDU struct {
	Type    string          `json:"edu_type"`
	Content json.RawMessage `json:"content"`
}

// EDU types the sender emits.
const (
	EDUTypeDirectToDevice   = "m.direct_to_device"
	EDUTypeDeviceListUpdate = "m.device_list_update"
	EDUTypeSigningKeyUpdate = "m.signing_key_update"
	EDUTypePresence         = "m.presence"
	EDUTypeReceipt          = "m.receipt"
	EDUTypeTyping           = "m.typing"
)

// IDGenerator hands out transaction ids.
//
// Synapse seeds its counter with the current time in milliseconds and
// increments it, shared across all destinations rather than per destination
// (transaction_manager.py:75). We match that: the id is not per-destination
// state, so two destinations in the same batch get different ids, and a restart
// cannot reuse an id it used before.
//
// The prefix is not decoration, and leaving it empty while another sender is
// also delivering to the same destination is dangerous. A receiving Synapse
// deduplicates inbound transactions on (origin, transaction_id) and returns the
// CACHED RESPONSE for a repeat (federation_server.py:400) -- so if two senders
// from the same origin ever pick the same id, the second transaction's events
// are silently discarded. Both senders seed from milliseconds-since-epoch and
// increment by one, so their id ranges are two intervals that drift into each
// other; over a busy hour a collision is not exotic.
//
// A prefix removes the possibility rather than reducing it. transaction_id is
// an opaque string in the spec and Synapse's route accepts anything without a
// slash (transport/server/federation.py:80), and it has the useful side effect
// of making the receiving server's logs say which sender produced a
// transaction.
type IDGenerator struct {
	prefix string
	next   atomic.Int64
}

// DefaultIDPrefix keeps our transaction ids out of Synapse's id space.
//
// Defaulted rather than opt-in because the dangerous configuration -- running
// alongside Synapse's own senders -- is the one this worker starts life in.
const DefaultIDPrefix = "gofed-"

// NewIDGenerator seeds the counter from the clock, as Synapse does, and
// namespaces it with prefix.
func NewIDGenerator(prefix string) *IDGenerator {
	g := &IDGenerator{prefix: prefix}
	g.next.Store(time.Now().UnixMilli())
	return g
}

// Next returns the next transaction id.
func (g *IDGenerator) Next() string {
	return g.prefix + strconv.FormatInt(g.next.Add(1)-1, 10)
}

// Signer builds signed requests for one homeserver.
type Signer struct {
	serverName string
	key        *federation.SigningKey
}

// NewSigner builds a Signer from a Synapse-format key line
// ("ed25519 <version> <unpadded-b64 seed>").
func NewSigner(serverName, synapseKeyLine string) (*Signer, error) {
	key, err := federation.ParseSynapseKey(synapseKeyLine)
	if err != nil {
		return nil, fmt.Errorf("txn: parsing signing key: %w", err)
	}
	return &Signer{serverName: serverName, key: key}, nil
}

// KeyID is the id of the key transactions are signed with, e.g. "ed25519:a_Yofy".
func (s *Signer) KeyID() string { return string(s.key.ID) }

// Request is a signed transaction, ready to send or to log.
type Request struct {
	TransactionID string
	Destination   string
	// Path is the request path, which is also what was signed.
	Path string
	// Body is the canonical JSON that was signed and that must be sent
	// byte-for-byte. Re-marshalling it would produce different bytes and
	// invalidate the signature.
	Body []byte
	// AuthHeader is the full Authorization header value.
	AuthHeader string
}

// Build assembles, signs and returns a transaction.
//
// The bytes in Request.Body are the bytes that were signed. Synapse has the
// same property (matrixfederationclient.py sends encode_canonical_json of the
// object it signed) and it is not incidental: the signature covers the content
// field of the auth object, so any re-encoding between signing and sending
// breaks it.
func (s *Signer) Build(txnID, destination string, t Transaction) (*Request, error) {
	t.Origin = s.serverName
	if t.PDUs == nil {
		t.PDUs = []json.RawMessage{}
	}

	body, err := canonicalJSON(t)
	if err != nil {
		return nil, fmt.Errorf("txn: encoding transaction: %w", err)
	}

	path := SendPathPrefix + url.PathEscape(txnID)

	// The object Synapse signs (matrixfederationclient.py:905). uri is the path
	// and query only -- no scheme, no host -- and content is the parsed body,
	// not a string containing it.
	auth := map[string]any{
		"method":      "PUT",
		"uri":         path,
		"origin":      s.serverName,
		"destination": destination,
		"content":     json.RawMessage(body),
	}
	sig, err := s.key.SignJSON(auth)
	if err != nil {
		return nil, fmt.Errorf("txn: signing: %w", err)
	}

	return &Request{
		TransactionID: txnID,
		Destination:   destination,
		Path:          path,
		Body:          body,
		AuthHeader: fmt.Sprintf(
			`X-Matrix origin="%s",key="%s",sig="%s",destination="%s"`,
			s.serverName, s.key.ID, sig, destination),
	}, nil
}

// canonicalJSON encodes a value as Matrix canonical JSON: keys sorted, no
// insignificant whitespace, and no HTML escaping of <, > and &.
//
// mautrix's implementation is used rather than encoding/json directly because
// Go escapes those three characters by default and canonical JSON does not.
// Leaving that on produces bytes that differ from Synapse's for any event
// containing an ampersand, which is a lot of them -- and the difference is
// invisible until a remote server rejects the signature.
func canonicalJSON(v any) ([]byte, error) {
	b, err := canonicaljson.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}
