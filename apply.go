package main

import (
	"fmt"
	"fsm/database"
	db "fsm/database"
	"log/slog"
	"os"
)

func (lh leaderHandler) Apply(e Entry, logEntries *Logs) (*database.Response, error) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	leaderCommit := logEntries.LastCommited()
	logSize := logEntries.Size() - 1

	// we have x logs and want to know where we stopped, y
	// and what we have left
	// where -1 is this actual log we have to apply
	remainderLogs := logSize - int(leaderCommit)
	// if remainderLogs == 0 {
	if logSize == int(leaderCommit) {

		// then commit it
		cmd := database.Command{
			Operation: e.Operation,
			Key:       e.Key,
			Value:     e.Value,
		}

		res, err := lh.db.Commit(cmd)
		if err != nil {
			return nil, err
		}
		logEntries.lastCommited.Add(1)
		return res, nil

	} else {
		logger.Info(
			fmt.Sprintf("we need to apply %d logs before applying the current one: %d", remainderLogs, e.Idx),
		)
		fmt.Printf(
			`remaining-logs: %+s
			leaderCommit:: %+v
			`, logEntries.String(),
			logEntries.lastCommited.Load(),
		)
	}

	return &db.Response{
		From:    "mock_stub",
		Message: "not quiet my tempo",
	}, nil
}
