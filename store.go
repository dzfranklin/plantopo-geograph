package geograph

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/cockroachdb/pebble"
	"github.com/tidwall/sjson"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"strings"
)

var ErrNotFound = errors.New("not found")

type Store struct {
	index      *inMemoryIndex
	db         *pebble.DB
	scratchDir string
}

type Tag struct {
	Prefix string `json:"prefix,omitempty"`
	Tag    string `json:"tag,omitempty"`
}

func Open(metaFile string) *Store {
	scratchDir, err := os.MkdirTemp("", "")
	if err != nil {
		panic(err)
	}
	slog.Info("Using scratchDir " + scratchDir)

	var metaF io.ReadCloser
	if strings.HasPrefix(metaFile, "https://") || strings.HasPrefix(metaFile, "http://") {
		resp, err := http.Get(metaFile)
		if err != nil {
			panic(err)
		}
		metaF = resp.Body
	} else {
		var err error
		metaF, err = os.Open(metaFile)
		if err != nil {
			panic(err)
		}
	}
	defer func() { _ = metaF.Close() }()

	metaR, err := gzip.NewReader(metaF)
	if err != nil {
		panic(err)
	}
	metaD := json.NewDecoder(metaR)

	dbOpts := new(pebble.Options)
	dbOpts.ErrorIfExists = true
	dbOpts.L0StopWritesThreshold = math.MaxInt32
	dbOpts.DisableAutomaticCompactions = true

	db, err := pebble.Open(scratchDir, dbOpts)
	if err != nil {
		panic(err)
	}

	indexData := indexContents{}

	i := 0
	for {
		var record json.RawMessage
		err := metaD.Decode(&record)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			panic(err)
		}

		var data struct {
			ID           int32   `json:"gridimage_id"`
			SubjectLng   float32 `json:"wgs84_long"`
			SubjectLat   float32 `json:"wgs84_lat"`
			ViewpointLng float32 `json:"viewpoint_wgs84_long"`
			ViewpointLat float32 `json:"viewpoint_wgs84_lat"`
		}
		if err := json.Unmarshal(record, &data); err != nil {
			panic(err)
		}
		indexData.ID = append(indexData.ID, data.ID)
		indexData.SubjectLng = append(indexData.SubjectLng, data.SubjectLng)
		indexData.SubjectLat = append(indexData.SubjectLat, data.SubjectLat)
		indexData.ViewpointLng = append(indexData.ViewpointLng, data.ViewpointLng)
		indexData.ViewpointLat = append(indexData.ViewpointLat, data.ViewpointLat)

		if err := db.Set(idToKey(data.ID), record, &pebble.WriteOptions{Sync: false}); err != nil {
			panic(err)
		}

		i++
		if i%100_000 == 0 {
			slog.Info("loading", "i", i)
		}
	}

	slog.Info("compacting")
	if err := db.Compact(idToKey(0), idToKey(math.MaxInt32), true); err != nil {
		panic(err)
	}

	slog.Info("loading index")
	index := loadIndex(indexData)

	slog.Info("store ready")

	return &Store{
		index:      index,
		db:         db,
		scratchDir: scratchDir,
	}
}

func (s *Store) Close() error {
	dbErr := s.db.Close()
	rmScratchErr := os.RemoveAll(s.scratchDir)

	if dbErr != nil {
		return dbErr
	} else if rmScratchErr != nil {
		return rmScratchErr
	}

	slog.Info("closed store")
	return nil
}

func (s *Store) Within(min, max [2]float32, index IndexType, maxItems, cursor int) (bool, int, []string, error) {
	page, err := s.index.within(min, max, index, maxItems, cursor)
	if err != nil {
		return false, 0, nil, err
	}

	out := make([]string, 0, len(page.items))
	for _, id := range page.items {
		value, err := s.Get(id)
		if err != nil {
			return false, 0, nil, err
		}
		out = append(out, value)
	}

	return page.hasNext, page.nextCursor, out, nil
}

func (s *Store) Near(target [2]float32, index IndexType, maxItems, cursor int) (bool, int, []string, error) {
	page, err := s.index.near(target, index, maxItems, cursor)
	if err != nil {
		return false, 0, nil, err
	}

	out := make([]string, 0, len(page.items))
	for i, id := range page.items {
		value, err := s.Get(id)
		if err != nil {
			return false, 0, nil, err
		}

		value, err = sjson.Set(value, "meters_from_target", haversineDistanceMeters(page.itemPoints[i], target))
		if err != nil {
			return false, 0, nil, err
		}

		out = append(out, value)
	}

	return page.hasNext, page.nextCursor, out, nil
}

func (s *Store) Get(id int32) (string, error) {
	valueBytes, closer, err := s.db.Get(idToKey(id))
	if errors.Is(err, pebble.ErrNotFound) {
		return "", ErrNotFound
	} else if err != nil {
		return "", err
	}
	value := string(valueBytes)
	if err := closer.Close(); err != nil {
		return "", err
	}
	return value, nil
}

func idToKey(id int32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(id))
	return b
}

func degreesToRadians(d float64) float64 {
	return d * math.Pi / 180
}

func haversineDistanceMeters(p, q [2]float32) int32 {
	lng1 := degreesToRadians(float64(p[0]))
	lat1 := degreesToRadians(float64(p[1]))
	lng2 := degreesToRadians(float64(q[0]))
	lat2 := degreesToRadians(float64(q[1]))

	diffLat := lat2 - lat1
	diffLon := lng2 - lng1

	a := math.Pow(math.Sin(diffLat/2), 2) + math.Cos(lat1)*math.Cos(lat2)*
		math.Pow(math.Sin(diffLon/2), 2)

	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return int32(math.Round(c * 6371e3))
}
