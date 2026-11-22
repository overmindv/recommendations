package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"

	"github.com/overmindv/recommendations/internal/evaluation"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("plan-eval", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("cases", "evaluation/algorithms.json", "path to an evaluation dataset")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}

		return 2
	}
	if flags.NArg() != 0 {
		_, _ = fmt.Fprintln(stderr, "unexpected positional arguments")

		return 2
	}
	info, err := os.Stat(*path)
	if err != nil || !info.Mode().IsRegular() {
		_, _ = fmt.Fprintln(stderr, "evaluation dataset must be a regular file")

		return 2
	}
	file, err := os.Open(*path)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "cannot open evaluation dataset")

		return 2
	}
	defer func() { _ = file.Close() }()
	report, err := evaluation.Evaluate(ctx, file)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)

		return 2
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		_, _ = fmt.Fprintln(stderr, "cannot write evaluation report")

		return 2
	}
	if !report.Passed {
		return 1
	}

	return 0
}
