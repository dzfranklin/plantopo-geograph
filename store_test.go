package geograph

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math"
	"sync"
	"testing"
)

func TestOpen(t *testing.T) {
	subject := sampleSubject(t)
	got, err := subject.Get(102097)
	require.NoError(t, err)
	fmt.Println(got)
}

func TestConcurrentRead(t *testing.T) {
	subject := sampleSubject(t)
	ids := []int32{4, 5, 6, 7, 8, 102095, 102096, 102097, 102098, 102099}
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

func TestOpenFull(t *testing.T) {
	// t.Skip()

	// naive: 8m 1s
	// one compaction: 3m 37s

	subject := Open("./import/out/meta.ndjson.gz")
	err := subject.Close()
	require.NoError(t, err)
}

func sampleSubject(t *testing.T) *Store {
	t.Helper()
	subject := Open("./sample.ndjson.gz")
	t.Cleanup(func() {
		if err := subject.Close(); err != nil {
			t.Error(err)
		}
	})
	return subject
}

func TestHaversine(t *testing.T) {
	got := haversineDistanceMeters(Point(-0.1275, 51.507222), Point(-1.9025, 52.48))
	assert.Equal(t, float64(163), math.Round(float64(got)/1000))
}
