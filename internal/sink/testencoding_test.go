package sink

import "encoding/base64"

// base64RawStdForTest matches signedjson's seed encoding: standard alphabet,
// no padding.
var base64RawStdForTest = base64.RawStdEncoding
