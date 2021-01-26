package history

type semaphore chan struct{}

func (s semaphore) send() {
	select {
	case s <- struct{}{}:
		// Receiver is waiting for signal
	default:
		// Semaphore is full
	}
}

func (s semaphore) receive() {
	<-s
}
