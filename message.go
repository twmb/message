// Package message provides basic interfaces for getting
// and sending messages and acknowledging they have been
// processed.
package message

import (
	"errors"
	"sync"
)

// EOMs is the error returned when a Getter has no more messages
// to give after a call to GetMessage.
var EOMs = errors.New("EOMs")

// Getter is the basic interface that wraps GetMessage.
//
// GetMessage should return a non-empty message. Empty messages
// can be discared. If GetMessage reads the last message, EOMs
// should be returned to signify "end of messages".
//
// GetMessage can still be called after EOMs is returned
// and should return (nil, EOMs) until a new message
// is ready to be got.
type Getter interface {
	GetMessage() (msg []byte, err error)
}

// Sender is the basic interface that wraps SendMessage.
//
// SendMessage returns the number of bytes sent and an
// an error if the message cannot be entirely successfully written.
type Sender interface {
	SendMessage([]byte) (n int, err error)
}

// AckGetter wraps the Getter interface and AckMsgGot.
//
// AckMsgGot will be called at the end of processing
// the message received from the AckGetter. This enables
// the AckGetter to save the message until AckMsgGot
// confirms the message has been processed. An error
// can be returned if the AckGetter cannot checkpoint.
//
// AckMsgGot should only be called if GetMessage returns
// a length > 0 message.
type AckGetter interface {
	Getter
	AckMsgGot() error
}

// GetSender is a Sender and a Getter.
type GetSender interface {
	Getter
	Sender
}

// AckGetSender is an AckGetter and a Sender.
type AckGetSender interface {
	AckGetter
	Sender
}

// Strings is a struct that wraps a string slice
// to implement the GetSender interface in a
// concurrency-safe way.
type Strings struct {
	m sync.Mutex
	s []string
}

func NewStrings(source []string) *Strings {
	return &Strings{s: source}
}

// TODO: when https://code.google.com/p/go/issues/detail?id=6980
// is dealt with, move mutex unlocks to defer functions.

func (s *Strings) GetMessage() ([]byte, error) {
	s.m.Lock()
	if len(s.s) == 0 {
		s.m.Unlock()
		return nil, EOMs
	}
	msg := []byte(s.s[0])
	s.s = s.s[1:]
	s.m.Unlock()
	return msg, nil
}

func (s *Strings) SendMessage(m []byte) (int, error) {
	s.m.Lock()
	s.s = append(s.s, string(m))
	l := len(m)
	s.m.Unlock()
	return l, nil
}

// ByteStrings is a struct that wraps a slice of
// byte slices to implement the GetSender interface
// in a concurrency-safe way.
type ByteStrings struct {
	m sync.Mutex
	s [][]byte
}

func NewByteStrings(source [][]byte) *ByteStrings {
	return &ByteStrings{s: source}
}

func (b *ByteStrings) GetMessage() ([]byte, error) {
	b.m.Lock()
	if len(b.s) == 0 {
		b.m.Unlock()
		return nil, EOMs
	}
	msg := b.s[0]
	b.s = b.s[1:]
	b.m.Unlock()
	return msg, nil
}

func (b *ByteStrings) SendMessage(m []byte) (int, error) {
	b.m.Lock()
	b.s = append(b.s, m)
	l := len(m)
	b.m.Unlock()
	return l, nil
}
