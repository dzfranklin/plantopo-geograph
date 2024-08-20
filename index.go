package geograph

import (
	"github.com/tidwall/rtree"
	"math"
)

type IndexType int

const (
	SubjectIndex IndexType = iota
	ViewpointIndex
)

type indexRTree = rtree.RTreeGN[float32, int32]

type inMemoryIndex struct {
	subject   indexRTree
	viewpoint indexRTree
}

type indexPage struct {
	items      []int32
	itemPoints [][2]float32
	hasNext    bool
	nextCursor int
}

type indexContents struct {
	ID           []int32
	SubjectLng   []float32
	SubjectLat   []float32
	ViewpointLng []float32
	ViewpointLat []float32
}

func loadIndex(contents indexContents) *inMemoryIndex {
	sanityCheckIndex(contents)

	var subject indexRTree
	var viewpoint indexRTree
	for i, id := range contents.ID {
		subjectPoint := Point(contents.SubjectLng[i], contents.SubjectLat[i])
		if !isZeroPoint(subjectPoint) {
			subject.Insert(subjectPoint, subjectPoint, id)
		}

		viewpointPoint := Point(contents.ViewpointLng[i], contents.ViewpointLat[i])
		if !isZeroPoint(viewpointPoint) {
			viewpoint.Insert(viewpointPoint, viewpointPoint, id)
		}
	}

	return &inMemoryIndex{
		subject:   subject,
		viewpoint: viewpoint,
	}
}

func (d *inMemoryIndex) within(min, max [2]float32, index IndexType, maxItems, cursor int) (indexPage, error) {
	i := 0
	ids := make([]int32, 0, maxItems)
	points := make([][2]float32, 0, maxItems)
	hasMore := false
	d.of(index).Search(min, max, func(point, _ [2]float32, id int32) bool {
		// Skip up to cursor
		if i < cursor {
			i++
			return true
		}

		// Stop if past max
		if i-cursor > maxItems-1 {
			hasMore = true
			return false
		}

		ids = append(ids, id)
		points = append(points, point)

		i++
		return true
	})
	return indexPage{hasNext: hasMore, nextCursor: i, items: ids, itemPoints: points}, nil
}

func (d *inMemoryIndex) near(target [2]float32, index IndexType, maxItems, cursor int) (indexPage, error) {
	i := 0
	ids := make([]int32, 0, maxItems)
	points := make([][2]float32, 0, maxItems)
	hasMore := false
	d.of(index).Nearby(
		rtree.BoxDist[float32, int32](target, target, nil),
		func(point, _ [2]float32, id int32, _ float32) bool {
			// Skip up to cursor
			if i < cursor {
				i++
				return true
			}

			// Stop if past max
			if i-cursor > maxItems-1 {
				hasMore = true
				return false
			}

			ids = append(ids, id)
			points = append(points, point)

			i++
			return true
		},
	)
	return indexPage{hasNext: hasMore, nextCursor: i, items: ids, itemPoints: points}, nil

}

func (d *inMemoryIndex) of(ty IndexType) *indexRTree {
	switch ty {
	case SubjectIndex:
		return &d.subject
	case ViewpointIndex:
		return &d.viewpoint
	default:
		panic("invalid index type")
	}
}

func sanityCheckIndex(index indexContents) {
	size := len(index.ID)
	if size == 0 {
		panic("empty index")
	}
	if len(index.SubjectLng) != size || len(index.SubjectLat) != size || len(index.ViewpointLng) != size || len(index.ViewpointLat) != size {
		panic("invalid index")
	}
}

func isZeroPoint(point [2]float32) bool {
	return math.Abs(float64(point[0])) < 0.01 && math.Abs(float64(point[1])) < 0.01
}
