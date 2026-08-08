package main

import (
	"fmt"
	"fsm/database"
	db "fsm/database"
	"log/slog"
	"os"
)

func (n *Node) Apply(e Entry) (*database.Response, error) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	leaderCommit := n.logs.LastCommited()
	logSize := n.logs.Size() - 1

	// we have x logs and want to know where we stopped, y
	// and what we have left
	// where -1 is this actual log we have to apply
	remainderLogs := logSize - int(leaderCommit)
	if remainderLogs == 0 {

		// then commit it
		cmd := database.Command{
			Operation: e.Operation,
			Key:       e.Key,
			Value:     e.Value,
		}

		res, err := n.database.Commit(cmd)
		if err != nil {
			return nil, err
		}
		n.logs.lastCommited.Store(uint64(e.Idx))
		logger.Info(
			fmt.Sprintf("we can proceed with applyingd %d directly as all logs have been applied", e.Idx),
			slog.Int("logSize", n.logs.Size()),
			slog.Uint64("leaderCommit", n.logs.lastCommited.Load()),
		)
		return res, nil

	} else {
		logger.Info(
			fmt.Sprintf("we need to apply %d logs before applying the current one: %d", remainderLogs, e.Idx),
		)
		fmt.Printf(
			`remaining-logs: %+s
			leaderCommit:: %+v
			`, n.logs.String(),
			n.logs.lastCommited.Load(),
		)
	}

	return &db.Response{
		From:    "mock_stub",
		Message: "not quiet my tempo",
	}, nil
}
