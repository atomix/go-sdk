package util

import (
	"sync"
)

// IterAsync executes the given function f up to n times concurrently.
// Each call is done in a separate goroutine. On each iteration, the function f
// will be called with a unique sequential index i such that the index can be
// used to reference an element in an array or slice. If an error is returned
// by the function f for any index, an error will be returned. Otherwise,
// a nil result will be returned once all function calls have completed.
func IterAsync(n int, f func(i int) error) error {
	wg := sync.WaitGroup{}
	asyncErrors := make(chan error, n)

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			err := f(i)
			if err != nil {
				asyncErrors <- err
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(asyncErrors)
	}()

	for err := range asyncErrors {
		return err
	}
	return nil
}

// ExecuteAsync executes the given function f up to n times concurrently, populating
// the given results slice with the results of each function call.
// Each call is done in a separate goroutine. On each iteration, the function f
// will be called with a unique sequential index i such that the index can be
// used to reference an element in an array or slice. If an error is returned
// by the function f for any index, an error will be returned. Otherwise,
// a nil result will be returned once all function calls have completed.
func ExecuteAsync(n int, f func(i int) (interface{}, error)) (interface{}, error) {
	wg := sync.WaitGroup{}
	asyncErrors := make(chan error, n)
	asyncResults := make(chan interface{}, n)

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			result, err := f(i)
			if err != nil {
				asyncErrors <- err
			} else {
				asyncResults <- result
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(asyncErrors)
	}()

	for err := range asyncErrors {
		return nil, err
	}

	results := make([]interface{}, 0, n)
	for result := range asyncResults {
		results = append(results, result)
	}
	return results, nil
}
