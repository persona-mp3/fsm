package main

import (
	"context"
	"fmt"
	db "fsm/database"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
)

type leader struct {
	leaderId       string
	transitionCh   chan RaftState
	networkCh      chan RPC
	workers        []*Worker
	raft           *Raft
	logEntries     *Logs
	connectedPeers []*Peer
	peerAddrs      []string
	logger         *slog.Logger
}

func NewLeader(
	leaderId string,
	networkCh chan RPC,
	raft *Raft,
	logEntries *Logs,
	transitionCh chan RaftState,
	logger *slog.Logger,
	peerAddrs []string,
) *leader {
	connectedPeers := []*Peer{}
	workers := []*Worker{}

	return &leader{
		leaderId:       leaderId,
		transitionCh:   transitionCh,
		networkCh:      networkCh,
		workers:        workers,
		raft:           raft,
		logEntries:     logEntries,
		connectedPeers: connectedPeers,
		logger:         logger,
		peerAddrs:      peerAddrs,
	}
}

func (l *leader) Start(
	ctx context.Context,
	currentTerm uint64,
	db db.Database,
) error {

	childCtx, cancel := context.WithCancel(ctx)
	defer l.cleanUp(cancel)

	allWorkers := []*Worker{}
	workerWg := sync.WaitGroup{}

	latestCommit := l.logEntries.getAtomicCommit()
	prevLogIdx := l.logEntries.PreviousLogIndex

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	for i, peerAddr := range l.peerAddrs {
		// TODO: attach peerId so the worker can dial
		worker := NewWorker(i, latestCommit, prevLogIdx, l.logEntries, logger)
		allWorkers = append(allWorkers, worker)
		workerWg.Go(func() {
			err := worker.run(childCtx, l.leaderId, peerAddr, currentTerm)
			if err != nil {
				fmt.Println("[worker.run] err: ", err)
			}

			fmt.Println("[worker.run] exited: ")
		})

	}

	l.workers = append(l.workers, allWorkers...)

	workersDone := make(chan struct{})
	go backgroundWait("workersWg", &workerWg, workersDone)
	handler := NewLeaderHandler(l.workers, len(l.peerAddrs), db)
	for {
		select {
		case <-ctx.Done():
			errMsg := fmt.Errorf("somone terminated my ctx:: %s", ctx.Err().Error())
			return errMsg
		case rpcPayload := <-l.networkCh:
			l.logger.Info("recvd new payload", slog.Any("paylaod", rpcPayload))
			reply, state, err := handlePayload(
				l.leaderId,
				rpcPayload,
				l.logEntries,
				currentTerm,
				handler,
				l.logger)
			if err != nil {
				panic(err)
			}

			fmt.Printf(
				`replyPayload: %+v
				state: %+v, 
				`, reply.payload, state,
			)

			backgroundSendCh(ctx, rpcPayload.reply, reply)
			if state != Remain {
				l.transitionCh <- state
				// panic("transiting")
				return nil
			}
		case <-workersDone:
			l.transitionCh <- Follower
			l.logger.Info("all workers have returned, need to turn to candidate")
			panic("allWorkers done before transition")
		}
	}
}

func (l *leader) cleanUp(cancel context.CancelFunc) {
	// panic("leader mode exiting")
	cancel()
}

// CURRENTLY TODO
func handlePayload(id string, payload RPC, logEntries *Logs, currentTerm uint64, h RPCHandler, logger *slog.Logger) (RPCReply, RaftState, error) {
	var reply RPCReply
	var state RaftState
	var err error
	previousLogEntry, _ := logEntries.GetPreviousLogEntry()
	latestCommit := logEntries.LastCommited()
	switch req := payload.payload.(type) {
	case AppendEntryRequest:
		reply, state, err = h.AppendEntryRequest(id, &req, &previousLogEntry, currentTerm, latestCommit, logger)
	case VoteRequest:
		reply, state, err = h.VoteRequest(id, &req, &previousLogEntry, currentTerm, latestCommit, logger)
	case SnapshotRequest:
		reply, state, err = h.SnapshotRequest(id, &req, &previousLogEntry, currentTerm, latestCommit, logger)
	case CommandRequest:
		reply, state, err = h.CommandRequest(id, &req, logEntries, currentTerm, latestCommit, logger)
	default:
		reply, state, err = h.UnknownRequest(&req, currentTerm, latestCommit, logger)
	}
	return reply, state, err
}

// replicateEntry tries to send the new entry to all the workers. If a majority of the workers
// are able to send out their RPC with the new entry, repliacteEntry returns true signalling that
// the leader can apply and commit this entry to the database.
func replicateEntry(
	entry Entry, workers []*Worker, clusterSize int, logger *slog.Logger,
) bool {
	replicasMade := atomic.Uint32{}
	replicasMade.Add(1)

	// done is used to signal when a majority of the workers have responded
	// when a majority has been reached, then the quorum will be calculated
	// this is also placed against a hardSet timeout at the moment of 180ms
	// to make sure slow workers don't delay and we don't hang forever
	done := make(chan bool, len(workers))

	replica := replicate{
		entry:   entry,
		success: &replicasMade,
		done:    done,
	}

	for _, worker := range workers {
		select {
		case worker.replicateCh <- replica:
		default:
			logger.Warn("dropped replica packet because worker is blocked")
		}
	}

	timeoutCtx, cancel := context.WithTimeout(context.Background(), WORKER_SEND_TIMEOUT)
	defer cancel()

	quorumTarget := (clusterSize / 2) + 1
	for i := range workers {
		_ = i
		if replicasMade.Load() >= uint32(quorumTarget) {
			fmt.Printf("quorum for replication was reached, %d\n", replicasMade.Load())
			return true
		}
		select {
		case <-timeoutCtx.Done():
			fmt.Printf("failed to reach quorum. Could not replicate entries to all workers within deadline")
			logger.Info("failed to reach quorum. Could not replicate entries to all workers within deadline")
			return false
		case <-done:
		}
	}

	// incase all the workers send and a quorum for replication still hasn't been reached
	if replicasMade.Load() >= uint32(quorumTarget) {
		logger.Info("quorum for replication was reached", slog.Uint64("total", uint64(replicasMade.Load())))
		return true
	}

	logger.Info("failed to reach quorum. Could not replicate entries to all workers",
		slog.Uint64("total", uint64(replicasMade.Load())),
	)

	return false
}
