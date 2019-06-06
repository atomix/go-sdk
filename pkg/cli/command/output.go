package command

import (
	"fmt"
	"os"
)

const (
	// http://tldp.org/LDP/abs/html/exitcodes.html
	ExitSuccess = iota
	ExitError
	ExitBadConnection
	ExitInvalidInput
	ExitBadFeature
	ExitInterrupted
	ExitIO
	ExitBadArgs = 128
)

func ExitWithOutput(output ...interface{}) {
	fmt.Fprintln(os.Stdout, output...)
	os.Exit(ExitSuccess)
}

func ExitWithSuccess() {
	os.Exit(ExitSuccess)
}

func ExitWithError(code int, err error) {
	fmt.Fprintln(os.Stderr, "Error:", err)
	os.Exit(code)
}
