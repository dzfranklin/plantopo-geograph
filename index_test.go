package geograph

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"slices"
	"testing"
)

func TestIndex(t *testing.T) {
	// Points:
	//
	//    | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10
	//  1 | 1
	//  2 |     2
	//  3 |         3
	//  4 |             4
	//  5 |                 5
	//  6 |                     6
	//  7 |                         7
	//  8 |                             8
	//  9 |                                 9
	// 10 |                                     10

	subject := loadIndex(IndexContents{
		ID:           []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		SubjectLng:   []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		SubjectLat:   []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		ViewpointLng: []float32{1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5},
		ViewpointLat: []float32{1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5},
	})

	t.Run("within/entire world", func(t *testing.T) {
		page, err := subject.within(Point(-180, -90), Point(180, 90), SubjectIndex, 10, 0)
		require.NoError(t, err)
		assert.Len(t, page.items, 10)
		assert.False(t, page.hasNext)
	})

	t.Run("within/reversed bounds is empty", func(t *testing.T) {
		page, err := subject.within(Point(180, 90), Point(-180, -90), SubjectIndex, 10, 0)
		require.NoError(t, err)
		assert.Len(t, page.items, 0)
		assert.False(t, page.hasNext)
	})

	t.Run("within/less than page", func(t *testing.T) {
		page, err := subject.within(Point(1.5, 1.5), Point(3.5, 3.5), SubjectIndex, 10, 0)
		require.NoError(t, err)
		require.Equal(t, page.items, []int32{2, 3})
		require.False(t, page.hasNext)
	})

	t.Run("within/maxItems=1", func(t *testing.T) {
		page, err := subject.within(Point(-180, -90), Point(180, 90), SubjectIndex, 1, 0)
		require.NoError(t, err)
		assert.Len(t, page.items, 1)
		assert.True(t, page.hasNext)
	})

	t.Run("within/paginate", func(t *testing.T) {
		minPt := Point(-180, -90)
		maxPt := Point(180, 90)

		var got []int32

		page, err := subject.within(minPt, maxPt, SubjectIndex, 2, 0)
		require.NoError(t, err)
		assert.Len(t, page.items, 2)
		assert.True(t, page.hasNext)
		got = append(got, page.items...)

		page, err = subject.within(minPt, maxPt, SubjectIndex, 2, page.nextCursor)
		require.NoError(t, err)
		assert.Len(t, page.items, 2)
		assert.True(t, page.hasNext)
		got = append(got, page.items...)

		page, err = subject.within(minPt, maxPt, SubjectIndex, 2, page.nextCursor)
		require.NoError(t, err)
		assert.Len(t, page.items, 2)
		assert.True(t, page.hasNext)
		got = append(got, page.items...)

		page, err = subject.within(minPt, maxPt, SubjectIndex, 2, page.nextCursor)
		require.NoError(t, err)
		assert.Len(t, page.items, 2)
		assert.True(t, page.hasNext)
		got = append(got, page.items...)

		page, err = subject.within(minPt, maxPt, SubjectIndex, 2, page.nextCursor)
		require.NoError(t, err)
		assert.Len(t, page.items, 2)
		assert.False(t, page.hasNext)
		got = append(got, page.items...)

		slices.Sort(got)
		assert.Equal(t, []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, got)
	})

	t.Run("near/entire world", func(t *testing.T) {
		target := Point(1, 1)
		page, err := subject.near(target, ViewpointIndex, 1000, 0)
		require.NoError(t, err)
		assert.Equal(t, []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, page.items)
		assert.False(t, page.hasNext)
	})

	t.Run("near/paginate", func(t *testing.T) {
		target := Point(1, 1)

		page, err := subject.near(target, ViewpointIndex, 4, 0)
		require.NoError(t, err)
		assert.Equal(t, []int32{1, 2, 3, 4}, page.items)
		assert.True(t, page.hasNext)

		page, err = subject.near(target, ViewpointIndex, 4, page.nextCursor)
		require.NoError(t, err)
		assert.Equal(t, []int32{5, 6, 7, 8}, page.items)
		assert.True(t, page.hasNext)

		page, err = subject.near(target, ViewpointIndex, 4, page.nextCursor)
		require.NoError(t, err)
		assert.Equal(t, []int32{9, 10}, page.items)
		assert.False(t, page.hasNext)
	})
}

func TestIndexIgnoresZero(t *testing.T) {
	subject := loadIndex(IndexContents{
		ID:           []int32{1, 2, 3},
		SubjectLng:   []float32{0, 0.001, 0},
		SubjectLat:   []float32{0, 0.001, 1},
		ViewpointLng: []float32{0, 0.001, 0},
		ViewpointLat: []float32{0, 0.001, 1},
	})

	minPt, maxPt := Point(-180, -90), Point(180, 90)

	page, err := subject.within(minPt, maxPt, SubjectIndex, 100, 0)
	require.NoError(t, err)
	require.Len(t, page.items, 1)

	page, err = subject.within(minPt, maxPt, ViewpointIndex, 100, 0)
	require.NoError(t, err)
	require.Len(t, page.items, 1)
}
