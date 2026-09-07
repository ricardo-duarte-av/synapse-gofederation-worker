package difflog

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// sampleFile is a rotating JSONL file of individual routing decisions.
//
// One rotation, not a series: the current file becomes .1 and the old .1 is
// discarded. Samples are for reading a recent window by hand against the real
// sender's logs, and an unbounded archive of them would be a disk-filling
// liability on a worker whose whole point is to be harmless.
type sampleFile struct {
	path     string
	maxBytes int64

	mu      sync.Mutex
	f       *os.File
	written int64
}

func openSampleFile(path string, maxBytes int64) (*sampleFile, error) {
	if maxBytes <= 0 {
		maxBytes = 64 << 20
	}
	s := &sampleFile{path: path, maxBytes: maxBytes}
	if err := s.reopen(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *sampleFile) reopen() error {
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("difflog: opening samples: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("difflog: %w", err)
	}
	s.f = f
	s.written = info.Size()
	return nil
}

func (s *sampleFile) write(r Route) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	b = append(b, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	if s.written+int64(len(b)) > s.maxBytes {
		if err := s.rotateLocked(); err != nil {
			return err
		}
	}
	n, err := s.f.Write(b)
	s.written += int64(n)
	return err
}

func (s *sampleFile) rotateLocked() error {
	if err := s.f.Close(); err != nil {
		return err
	}
	// Rename over any existing .1: one generation is kept deliberately.
	if err := os.Rename(s.path, s.path+".1"); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("difflog: rotating samples: %w", err)
	}
	return s.reopen()
}

func (s *sampleFile) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}
