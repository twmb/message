// Package message provides basic interfaces for getting
// and sending messages and acknowledging they have been
// processed.
package message

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// EOMs is the error returned when a Getter has no more messages
// to give after a call to GetMessage.
var EOMs = errors.New("EOMs")

// MarshalTester is an interface that wraps Marshalled, a function
// that returns whether the messager will return marshalled data.
type MarshalTester interface {
	Marshalled() bool
}

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

// BlockGetter is the basic interface that wraps BlockGetMessage.
//
// BlockGetMessage like GetMessage but blocks until a message is
// available. By design, this means that BlockGetMessage should
// never return EOMs.
type BlockGetter interface {
	BlockGetMessage() (msg []byte, err error)
}

// BlockTimeoutGetter is the basic interface that wraps BlockTimeoutGetMessage.
//
// BlockTimeoutGetMessage is like BlockGetMessage except with a timeout
// for waiting for a new message. If no message is received within that
// timeout, BlockTimeoutGetMessage returns EOMs.
type BlockTimeoutGetter interface {
	BlockTimeoutGetMessage(time.Duration) (msg []byte, err error)
}

// BlockWakeGetter is an wraps BlockGetter with a function to
// force return early with an EOMs error.
type BlockWakeGetter interface {
	BlockGetter
	Wake()
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

// BlockAckGetter wraps BlockGetter and AckMsgGot.
type BlockAckGetter interface {
	BlockGetter
	AckMsgGot() error
}

// BlockTimeoutAckGetter wraps BlockTimeoutGetter and AckMsgGot.
type BlockTimeoutAckGetter interface {
	BlockTimeoutGetter
	AckMsgGot() error
}

// BlockWakeAckGetter wraps BlockWakeGetter and AckMsgGot.
type BlockWakeAckGetter interface {
	BlockWakeGetter
	AckMsgGot() error
}

// GetSender is a Getter and a Sender.
type GetSender interface {
	Getter
	Sender
}

// BlockGetSender is a BlockGetter and a Sender.
type BlockGetSender interface {
	BlockGetter
	Sender
}

// BlockTimeoutGetSender is a BlockTimeoutGetter and a Sender.
type BlockTimeoutGetSender interface {
	BlockTimeoutGetter
	Sender
}

// BlockWakeGetSender is a BlockWakeGetter and a Sender.
type BlockWakeGetSender interface {
	BlockWakeGetter
	Sender
}

// AckGetSender is an AckGetter and a Sender.
type AckGetSender interface {
	AckGetter
	Sender
}

// BlockAckGetSender is a BlockAckGetter and a Sender.
type BlockAckGetSender interface {
	BlockAckGetter
	Sender
}

// BlockTimeoutAckGetSender is a BlockTimeoutAckGetter and a Sender.
type BlockTimeoutAckGetSender interface {
	BlockTimeoutAckGetter
	Sender
}

// BlockWakeAckGetSender is a BlockWakeAckGetter and a Sender
type BlockWakeAckGetSender interface {
	BlockWakeAckGetter
	Sender
}

// Strings is a struct that wraps a string slice
// to implement the GetSender interface in a
// concurrency-safe way.
type Strings struct {
	m    sync.Mutex
	s    []string
	c    *sync.Cond
	t    time.Timer
	wake chan struct{}
}

// NewStrings creates a new Strings to use. It must be called
// to use any Block functions.
func NewStrings(source []string) *Strings {
	var m sync.Mutex
	return &Strings{
		m:    m,
		s:    source,
		c:    sync.NewCond(&m),
		wake: make(chan struct{}),
	}
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

func (s *Strings) BlockGetMessage() ([]byte, error) {
	var err error
	s.m.Lock()
	wakecpy := s.wake
out:
	for {
		select {
		case <-wakecpy:
			err = EOMs
			break out
		default:
			if len(s.s) != 0 {
				break out
			} else {
				s.c.Wait()
			}
		}
	}
	msg := []byte(s.s[0])
	s.s = s.s[1:]
	s.m.Unlock()
	return msg, err
}

func (s *Strings) Wake() {
	s.m.Lock()
	wakeold := s.wake
	s.wake = make(chan struct{})
	close(wakeold)
	s.m.Unlock()
	s.c.Broadcast()
}

func (s *Strings) BlockTimeoutGetMessage(d time.Duration) ([]byte, error) {
	s.m.Lock()
	defer s.m.Unlock()
	var timeout int32
	t := time.AfterFunc(d, func() {
		atomic.StoreInt32(&timeout, 1)
		// alright to broadcast because of for loop managing s.c.Wait()
		s.c.Broadcast()
	})
	for len(s.s) == 0 && atomic.LoadInt32(&timeout) == 0 {
		s.c.Wait()
	}
	t.Stop()
	if atomic.LoadInt32(&timeout) == 1 {
		return nil, EOMs
	}
	msg := []byte(s.s[0])
	s.s = s.s[1:]
	return msg, nil
}

func (s *Strings) SendMessage(m []byte) (int, error) {
	s.m.Lock()
	s.s = append(s.s, string(m))
	l := len(m)
	s.m.Unlock()
	s.c.Signal()
	return l, nil
}

type MarshalledStrings struct {
	Strings
}

func NewMarshalledStrings(source []string) *MarshalledStrings {
	return &MarshalledStrings{*NewStrings(source)}
}

func (ms *MarshalledStrings) Marshalled() bool {
	return true
}

// ByteStrings is a struct that wraps a slice of
// byte slices to implement the GetSender interface
// in a concurrency-safe way.
type ByteStrings struct {
	m    sync.Mutex
	s    [][]byte
	c    *sync.Cond
	wake chan struct{}
}

func NewByteStrings(source [][]byte) *ByteStrings {
	var m sync.Mutex
	return &ByteStrings{
		m:    m,
		s:    source,
		c:    sync.NewCond(&m),
		wake: make(chan struct{}),
	}
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

func (b *ByteStrings) BlockGetMessage() ([]byte, error) {
	var err error
	b.m.Lock()
	wakecpy := b.wake
out:
	for {
		select {
		case <-wakecpy:
			err = EOMs
			break out
		default:
			if len(b.s) != 0 {
				break out
			} else {
				b.c.Wait()
			}
		}
	}
	msg := b.s[0]
	b.s = b.s[1:]
	b.m.Unlock()
	return msg, err
}

func (b *ByteStrings) Wake() {
	b.m.Lock()
	wakeold := b.wake
	b.wake = make(chan struct{})
	close(wakeold)
	b.m.Unlock()
	b.c.Broadcast()
}

func (b *ByteStrings) BlockTimeoutGetMessage(d time.Duration) ([]byte, error) {
	b.m.Lock()
	defer b.m.Unlock()
	var timeout int32
	t := time.AfterFunc(d, func() {
		atomic.StoreInt32(&timeout, 1)
		b.c.Broadcast()
	})
	for len(b.s) == 0 && atomic.LoadInt32(&timeout) == 0 {
		b.c.Wait()
	}
	t.Stop()
	if atomic.LoadInt32(&timeout) == 1 {
		return nil, EOMs
	}
	msg := b.s[0]
	b.s = b.s[1:]
	return msg, nil
}

func (b *ByteStrings) SendMessage(m []byte) (int, error) {
	b.m.Lock()
	b.s = append(b.s, m)
	l := len(m)
	b.m.Unlock()
	b.c.Signal()
	return l, nil
}

type MarshalledByteStrings struct {
	ByteStrings
}

func NewMarshalledByteStrings(source [][]byte) *MarshalledByteStrings {
	return &MarshalledByteStrings{*NewByteStrings(source)}
}

func (mb *MarshalledByteStrings) Marshalled() bool {
	return true
}
