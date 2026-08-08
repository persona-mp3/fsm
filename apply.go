package main

import (
	"fmt"
	"log/slog"
	"os"
)

func (n *Node) Apply(e Entry) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	leaderCommit := n.logs.LastCommited()
	logSize := n.logs.Size() - 1

	// we have x logs and want to know where we stopped, y
	// and what we have left
	// where -1 is this actual log we have to apply
	remainderLogs := logSize - int(leaderCommit)
	if remainderLogs == 0 {
		logger.Info(
			fmt.Sprintf("we can proceed with applyingd %d directly as all logs have been applied", e.Idx),
			slog.Int("logSize", logSize),
			slog.Uint64("leaderCommit", leaderCommit),
		)

	} else {
		logger.Info(
			fmt.Sprintf("we need to apply %d logs before applying the current one: %d", remainderLogs, e.Idx),
		)
	}
	return nil
}
