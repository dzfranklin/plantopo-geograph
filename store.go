package geograph

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/valyala/gozstd"
	"os"
	"path"
)

type Store struct {
	dir   string
	dd    *gozstd.DDict
	index *inMemoryIndex
}

func Open(dir string) *Store {
	dictBytes, err := os.ReadFile(path.Join(dir, "dictionary"))
	if err != nil {
		panic(err)
	}

	dict, err := gozstd.NewDDict(dictBytes)
	if err != nil {
		panic(err)
	}

	// Read index data

	indexBytes, err := os.ReadFile(path.Join(dir, "index"))
	if err != nil {
		panic(err)
	}
	indexContents := DecodeIndex(indexBytes)
	index := loadIndex(indexContents)

	return &Store{
		dir:   dir,
		dd:    dict,
		index: index,
	}
}

func (s *Store) Close() error {
	s.dd.Release()
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
	for _, id := range page.items {
		value, err := s.Get(id)
		if err != nil {
			return false, 0, nil, err
		}
		out = append(out, value)
	}

	return page.hasNext, page.nextCursor, out, nil
}

func (s *Store) Get(id int32) (string, error) {
	compressed, err := os.ReadFile(path.Join(s.dir, IDToPath(id)))
	if err != nil {
		return "", err
	}
	v, err := gozstd.DecompressDict(nil, compressed, s.dd)
	if err != nil {
		return "", err
	}
	return string(v), nil
}

func IDToPath(id int32) string {
	fname := fmt.Sprintf("%08d", id)
	dirA := fname[:3]
	dirB := fname[3:5]
	return path.Join(dirA, dirB, fname)
}

type IndexContents struct {
	ID           []int32
	SubjectLng   []float32
	SubjectLat   []float32
	ViewpointLng []float32
	ViewpointLat []float32
}

func EncodeIndex(index IndexContents) []byte {
	sanityCheckIndex(index)
	var out bytes.Buffer
	if err := gob.NewEncoder(&out).Encode(index); err != nil {
		panic(err)
	}
	return out.Bytes()
}

func DecodeIndex(encoded []byte) IndexContents {
	var out IndexContents
	if err := gob.NewDecoder(bytes.NewReader(encoded)).Decode(&out); err != nil {
		panic(err)
	}
	return out
}
