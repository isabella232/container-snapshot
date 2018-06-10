package watcher

import (
	"time"

	"github.com/fsnotify/fsnotify"
)

// Path observes changes to a set of paths on the filesystem and invokes a
// user-provided function.
type Path struct {
	paths     []string
	onChanged func([]string) error

	changed  chan struct{}
	trigger  chan struct{}
	delay    time.Duration
	maxDelay int
}

// New invokes fn when changes occur to any of the listed paths (if the path
// points to a directory, any changes to the files in the directory are made). fn is always
// invoked at least once when Run() is invoked.
func New(paths []string, fn func(paths []string) error) *Path {
	return &Path{
		paths:     paths,
		onChanged: fn,
		changed:   make(chan struct{}, 1),
		trigger:   make(chan struct{}, 1),
	}
}

// SetMinimumInterval prevents changes from being sent more often than this interval.
func (w *Path) SetMinimumInterval(min time.Duration) {
	w.delay = min
}

// SetMaxDelays sets the upper bound for how many successive intervals
// with a change to the filesystem are collapsed.
func (w *Path) SetMaxDelays(max int) {
	w.maxDelay = max
}

func (w *Path) changeCollapser() {
	for {
		select {
		case _, ok := <-w.changed:
			if !ok {
				return
			}
		}
		if w.delay > 0 {
			time.Sleep(w.delay)
			for i := w.maxDelay; i > 0; i-- {
				select {
				case _, ok := <-w.changed:
					if !ok {
						return
					}
					time.Sleep(w.delay)
				default:
					i = 0
				}
			}
		}
		w.trigger <- struct{}{}
	}
}

// Run starts the content watcher. The registered function will always be
// invoked at least once. Run exits when the registered function returns an
// error or a filesystem error occurs.
func (w *Path) Run(stopCh <-chan struct{}) error {
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer fsw.Close()

	fsDone := make(chan error)
	fnDone := make(chan error)

	// this goroutine translates filesystem notifications into
	// entries on the changed channel
	go func() {
		// we always trigger on startup, before we get our first event
		w.trigger <- struct{}{}
		defer close(fsDone)
		defer close(w.changed)
		for {
			select {
			case <-stopCh:
				fsw.Close()
				// drain the channels
				for range fsw.Errors {
				}
				for range fsw.Events {
				}
				return
			case err, ok := <-fsw.Errors:
				if !ok {
					return
				}
				fsDone <- err
			case evt, ok := <-fsw.Events:
				if !ok {
					return
				}
				switch evt.Op {
				case fsnotify.Create, fsnotify.Write, fsnotify.Remove:
					select {
					case w.changed <- struct{}{}:
					default:
					}
				}
			}
		}
	}()

	// this goroutine rate collapses frequent changes into a smaller
	// number of triggers
	go w.changeCollapser()

	// this goroutine passes trigger notifications along to the registered
	// function
	go func() {
		defer close(fnDone)
		for range w.trigger {
			if err := w.onChanged(w.paths); err != nil {
				fnDone <- err
				return
			}
		}
	}()

	// register for notifications
	for _, path := range w.paths {
		if err := fsw.Add(path); err != nil {
			return err
		}
	}

	// wait until we get an error from the filesystem or from the change
	// notifier and then exit
	select {
	case err := <-fsDone:
		return err
	case err := <-fnDone:
		return err
	}
}
