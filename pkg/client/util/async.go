package util

import (
	"sync"
)

// ExecuteAllAsync executes the given function 'f' n times concurrently with the given 'args'.
// Once all functions have completed or an error is returned, the result is returned.
// If at least one error occurs, an error is returned. Otherwise, all results are returned
// once the function calls have completed.
func ExecuteAllAsync(args []interface{}, f func(interface{}) (interface{}, error)) ([]interface{}, error) {
	wg := sync.WaitGroup{}
	asyncResults := make(chan interface{}, len(args))
	asyncErrors := make(chan error, len(args))

	for _, arg := range args {
		wg.Add(1)
		go func() {
			result, err := f(arg)
			if err != nil {
				asyncErrors <- err
			} else {
				asyncResults <- result
			}
		}()
	}

	go func() {
		wg.Wait()
		close(asyncResults)
		close(asyncErrors)
	}()

	for err := range asyncErrors {
		return nil, err
	}

	results := make([]interface{}, len(args))
	for result := range asyncResults {
		results = append(results, result)
	}
	return results, nil
}
