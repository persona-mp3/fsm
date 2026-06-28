// raftLogger serves as a custom logging implementation. By offering derived loggers and
// capturing state of a Node, logs are easy to pass around and monitor between function
// calls and event transitions.
// The intent here is that each State should have it's own logger, and any other side effects
// they might cause like spawing numerous routines that do something in the background
//
// The single implementation at the moment:
//   - [RLogger] This is strictly meant for debugging and is not to be used for strucuted logging.
//     By presenting all possible information about a Node, and creating child loggers
//     from a parent logger, It presents it's own unique formatting. See [Humane]
//
// - [RSLogger] Is a future implementation for structured logging
package raftlogger

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
)

type RLogger interface {
	Println(s string, args ...any)
	Panic(s string, args ...any)
	Inherit(childName string) RLogger
	UpdateTerm(term uint64)
	UpdateOwner(state string)

	Out() io.Writer
}

// for structured logging
type RSLogger interface {
	Level() string
}

// Humane logger parses out logs in the format
// [time.ms] (nodeId:owner:term) Initialising node. Diagnostics {Addr: localhost:9091 }
type Humane struct {
	// id of the node
	id string

	ownerLock sync.RWMutex
	owner     string

	termLock sync.RWMutex
	// term is the term of the Node using the logger
	term uint64

	// raftState is the RaftState of the Node using the logger. This
	// must be safely upated and called via the the [ownerLock]
	// raftState string

	prefix string

	// destination of logs for both the parent and child. If nothing
	// is provided, they default to stdout
	out io.Writer

	// underlying logger
	log *log.Logger
}

// NewHumaneLogger returns a Logger that follows the [RLogger] inteface, that means
// it's mainly used for debugging a Node. It has no strutured logging implemented into
// it. The [args] provided will be used to format the prefix or the logs. The format is
// [Owner] is used to represent what subsection of the node is running. If the node is
// currently a Follower, the owner will be set to Follower. If the Node is running in
// the main loop, it will simply refer to as `node`. If the server, `server`
//
// [time.ms] (nodeId:owner:term) Initialising node. Diagnostics {Addr: localhost:9091 }
//
// value in [args] will get appended to as (nodeId:owner:term.arg.arg)
func NewHumaneLogger(id, owner string, term uint64, out io.Writer) RLogger {
	prefix := fmt.Sprintf("(%s:%s:%d) ", id, owner, term)
	if out == nil {
		out = os.Stdout
	}
	l := log.New(out, prefix, log.Ltime|log.Lmicroseconds|log.Lmsgprefix)
	return &Humane{
		id:        id,
		ownerLock: sync.RWMutex{},
		owner:     owner,
		termLock:  sync.RWMutex{},
		term:      term,
		prefix:    prefix,
		out:       out,
		log:       l,
	}

}

// Println logs the arguments passed into it
func (h *Humane) Println(s string, args ...any) {
	sb := strings.Builder{}
	sb.WriteString(s)
	for _, arg := range args {
		fmt.Fprintf(&sb, " %+v", arg)
	}

	h.log.Println(sb.String())
}

func (h *Humane) Panic(s string, args ...any) {
	h.log.Panicf("%s %+v", s, args)
}

// Inherit returns a child logger that writes to the same output as the parent. This
// is safe for conccurent use as the underlying [log.Logger] gaurantees it through a
// mutex. The child also inherits the flags as the parent
func (h *Humane) Inherit(childName string) RLogger {
	term := h.getTerm()

	childPrefix := fmt.Sprintf("(%s:%s:%d.%s) ", h.id, h.owner, term, childName)
	childOut := h.log.Writer()

	l := log.New(childOut, childPrefix, log.Ltime|log.Lmicroseconds|log.Lmsgprefix)
	return &Humane{
		id:       h.id,
		owner:    childName,
		termLock: sync.RWMutex{},
		term:     term,

		ownerLock: sync.RWMutex{},

		prefix: childPrefix,
		out:    childOut,
		log:    l,
	}
}

func (h *Humane) getTerm() uint64 {
	h.termLock.RLock()
	defer h.termLock.RUnlock()
	return h.term
}

func (h *Humane) getOwner() string {
	h.ownerLock.RLock()
	defer h.ownerLock.RUnlock()
	return h.owner
}

// UpdateTerm updates the term of the logger corresponding with the node. Callers
// are responsible for ensure the term they're providing is the actual term of
// the node.
func (h *Humane) UpdateTerm(term uint64) {
    h.ownerLock.RLock()
    h.termLock.Lock()

    defer h.ownerLock.RUnlock()
    defer h.termLock.Unlock()

    h.term = term
    prefix := fmt.Sprintf("(%s:%s:%d) ", h.id, h.owner, h.term)
    h.prefix = prefix
    h.log.SetPrefix(prefix)
}

// TODO: it's not yet clear when this might be needed, but it feels right to be able
// to update the  owner and term at once, but given child loggers are derived this
// might be useful
func (h *Humane) UpdateOwner(owner string) {
	h.ownerLock.Lock()
	h.termLock.RLock()

	defer h.ownerLock.Unlock()
	defer h.termLock.RUnlock()

	h.owner = owner
	prefix := fmt.Sprintf("(%s:%s:%d) ", h.id, h.owner, h.term)
	h.prefix = prefix
	h.log.SetPrefix(prefix)
}

func (h *Humane) UpdateInfo(owner string, term uint64) {
	h.ownerLock.Lock()
	h.termLock.Lock()

	defer h.ownerLock.Unlock()
	defer h.termLock.Unlock()

	h.owner = owner
	h.term = term
	prefix := fmt.Sprintf("(%s:%s:%d) ", h.id, h.owner, h.term)
	h.prefix = prefix
	h.log.SetPrefix(prefix)
}

func (h *Humane) Out() io.Writer {
	return h.out
}
