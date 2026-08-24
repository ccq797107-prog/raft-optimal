package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/czq/cd-raft/internal/harness"
)

func main() {
	var (
		scenarioName = flag.String("scenario", "smoke", "built-in scenario name")
		scenarioFile = flag.String("scenario-file", "", "JSON scenario file")
		outPath      = flag.String("out", "", "Markdown report output path")
		timeout      = flag.Duration("timeout", 30*time.Second, "overall scenario timeout")
		fastReturn   = flag.Bool("fast-return", false, "enable Fast Return in the harness topology")
		configPath   = flag.String("config", "", "topology config path; default uses local free ports")
		storeProfile = flag.String("store", harness.StoreProfileMemory, "store profile: memory, leveldb-sync, or experimental leveldb-nosync")
		dataDir      = flag.String("data-dir", "", "data directory for leveldb-sync store profile")
	)
	flag.Parse()

	scenario, err := loadScenario(*scenarioName, *scenarioFile)
	if err != nil {
		fail(nil, *outPath, err)
	}
	if *fastReturn {
		scenario.FastReturn = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	h, err := harness.New(harness.Options{
		ConfigPath:        *configPath,
		FastReturnEnabled: scenario.FastReturn,
		StoreProfile:      *storeProfile,
		DataDir:           *dataDir,
	})
	if err != nil {
		fail(nil, *outPath, err)
	}
	defer h.Close()

	report, runErr := h.RunScenario(ctx, scenario)
	if writeErr := writeReport(report, *outPath); writeErr != nil && runErr == nil {
		runErr = writeErr
	}
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "cdraft-harness: %v\n", runErr)
		os.Exit(1)
	}
}

func loadScenario(name, path string) (harness.Scenario, error) {
	if path != "" {
		return harness.LoadScenario(path)
	}
	scenario, ok := harness.BuiltinScenario(name)
	if !ok {
		return harness.Scenario{}, fmt.Errorf("unknown built-in scenario %q", name)
	}
	return scenario, nil
}

func fail(report *harness.Report, out string, err error) {
	if report != nil {
		_ = writeReport(*report, out)
	}
	fmt.Fprintf(os.Stderr, "cdraft-harness: %v\n", err)
	os.Exit(1)
}

func writeReport(report harness.Report, out string) error {
	if out == "" {
		out = filepath.Join(os.TempDir(), fmt.Sprintf("cd-raft-harness-%s.md", time.Now().Format("20060102-150405")))
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(out, []byte(report.Markdown()), 0o644); err != nil {
		return err
	}
	fmt.Printf("harness report written to %s\n", out)
	return nil
}
