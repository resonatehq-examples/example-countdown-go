// Package main runs a durable countdown that sleeps between ticks and posts
// a notification on each one. Demonstrates ctx.Sleep + ctx.RPC against a
// Resonate dev server.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
)

type CountdownArgs struct {
	Start       int    `json:"start"`
	StepSeconds int    `json:"step_seconds"`
	NotifyURL   string `json:"notify_url"`
}

type CountdownResult struct {
	Sent int `json:"sent"`
}

type NotifyArgs struct {
	Count int    `json:"count"`
	URL   string `json:"url"`
}

type NotifyResult struct {
	Status int `json:"status"`
}

// countdown ticks from Start down to 1, posting a notification on each tick
// and sleeping StepSeconds between ticks. The Sleep + Notify pair is durable
// — if the worker crashes mid-countdown, restarting picks up at the next
// pending tick rather than starting over.
func countdown(ctx *resonate.Context, args CountdownArgs) (CountdownResult, error) {
	sent := 0
	for i := args.Start; i > 0; i-- {
		f, err := ctx.RPC("notify", NotifyArgs{Count: i, URL: args.NotifyURL})
		if err != nil {
			return CountdownResult{}, err
		}
		var r NotifyResult
		if err := f.Await(&r); err != nil {
			return CountdownResult{}, fmt.Errorf("notify %d: %w", i, err)
		}
		sent++

		if i > 1 {
			s, err := ctx.Sleep(time.Duration(args.StepSeconds) * time.Second)
			if err != nil {
				return CountdownResult{}, err
			}
			if err := s.Await(nil); err != nil {
				return CountdownResult{}, fmt.Errorf("sleep before %d: %w", i-1, err)
			}
		}
	}
	return CountdownResult{Sent: sent}, nil
}

// notify POSTs the current count to args.URL. If URL is empty, the function
// is a no-op (useful for offline runs).
func notify(_ *resonate.Context, args NotifyArgs) (NotifyResult, error) {
	if args.URL == "" {
		fmt.Printf("  [notify] %d (no URL — skipping HTTP)\n", args.Count)
		return NotifyResult{Status: 0}, nil
	}
	body := strings.NewReader(fmt.Sprintf("countdown: %d", args.Count))
	req, err := http.NewRequest(http.MethodPost, args.URL, body)
	if err != nil {
		return NotifyResult{}, err
	}
	req.Header.Set("Content-Type", "text/plain")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return NotifyResult{}, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	fmt.Printf("  [notify] %d -> %s %s\n", args.Count, args.URL, resp.Status)
	return NotifyResult{Status: resp.StatusCode}, nil
}

func main() {
	start := flag.Int("start", 3, "starting count")
	step := flag.Int("step", 1, "seconds between ticks")
	url := flag.String("url", "", "URL to POST each tick to (optional)")
	flag.Parse()

	r, err := resonate.New(resonate.Config{URL: "http://localhost:8001"})
	if err != nil {
		log.Fatalf("resonate.New: %v", err)
	}
	defer func() { _ = r.Stop() }()

	cdFn, err := resonate.Register(r, "countdown", countdown)
	if err != nil {
		log.Fatalf("Register countdown: %v", err)
	}
	if _, err := resonate.Register(r, "notify", notify); err != nil {
		log.Fatalf("Register notify: %v", err)
	}

	ctx := context.Background()
	id := fmt.Sprintf("countdown-%d", time.Now().UnixNano())
	args := CountdownArgs{Start: *start, StepSeconds: *step, NotifyURL: *url}

	fmt.Printf("[countdown] starting workflow id=%s start=%d step=%ds url=%q\n",
		id, args.Start, args.StepSeconds, args.NotifyURL)
	h, err := cdFn.Run(ctx, id, args)
	if err != nil {
		log.Fatalf("Run: %v", err)
	}
	out, err := h.Result(ctx)
	if err != nil {
		log.Fatalf("Result: %v", err)
	}
	fmt.Printf("[countdown] done; sent %d notifications\n", out.Sent)
}
