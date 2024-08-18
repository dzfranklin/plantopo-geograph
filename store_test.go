package geograph

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"os/exec"
	"sync"
	"testing"
)

func TestOpen(t *testing.T) {
	subject := untarSampleSubject(t)
	got, err := subject.Get(989839)
	require.NoError(t, err)
	fmt.Println(got)
}

func TestConcurrentRead(t *testing.T) {
	subject := untarSampleSubject(t)
	ids := []int32{989839, 5806946, 219680, 6241422, 7613507, 7670236, 7729020, 6273971, 6272257, 7729333, 5149189, 5165007, 6396030, 1929866, 780772, 7625305, 7466254, 874085, 413477, 494216, 7200791}
	var wg sync.WaitGroup
	for range 10 {
		for range 100 {
			for _, id := range ids {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := subject.Get(id)
					require.NoError(t, err)
				}()
			}
		}
		wg.Wait()
	}
}

func untarSampleSubject(t *testing.T) *Store {
	// Some ids in the sample: 989839, 5806946, 219680, 6241422, 7613507, 7670236, 7729020, 6273971, 6272257, 7729333, 5149189, 5165007, 6396030, 1929866, 780772, 7625305, 7466254, 874085, 413477, 494216, 7200791

	t.Helper()
	dir, err := os.MkdirTemp("", "")
	if err != nil {
		panic(err)
	}

	if err := exec.Command("tar", "xf", "./sample.tar", "--directory="+dir).Run(); err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Log(err)
		}
	})

	subject := Open(dir)
	t.Cleanup(func() {
		if err := subject.Close(); err != nil {
			t.Error(err)
		}
	})
	return subject
}
