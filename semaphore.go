package kiroku

type semaphore chan struct{}

// send will attempt to send a signal to a semaphore
// Note: This function is non-blocking
func (s semaphore) send() {
	select {
	// Attempt to send signal
	case s <- struct{}{}:
		// There was space in the semaphore for the signal
	default:
		// Semaphore is full, continue on
	}
}
