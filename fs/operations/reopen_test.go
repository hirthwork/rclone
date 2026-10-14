package operations

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/rclone/rclone/lib/pool"
	"github.com/rclone/rclone/lib/random"
	"github.com/rclone/rclone/lib/readers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// check interfaces
var (
	_ io.ReadSeekCloser      = (*ReOpen)(nil)
	_ io.ReaderAt            = (*ReOpen)(nil)
	_ pool.DelayAccountinger = (*ReOpen)(nil)
)

var errorTestError = errors.New("test error")

// this is a wrapper for a mockobject with a custom Open function
//
// breaks indicate the number of bytes to read before returning an
// error and garbage the number of bytes of garbage to return just
// before each error
type reOpenTestObject struct {
	fs.Object
	t           *testing.T
	wantStart   int64
	breaks      []int64
	garbage     []int64
	unknownSize bool
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
//
// This will break after reading the number of bytes in breaks
func (o *reOpenTestObject) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	// Lots of backends do this - make sure it works as it modifies options
	fs.FixRangeOption(options, o.Size())
	gotHash := false
	gotRange := false
	startPos := int64(0)
	for _, option := range options {
		switch x := option.(type) {
		case *fs.HashesOption:
			gotHash = true
		case *fs.RangeOption:
			gotRange = true
			startPos = x.Start
			if o.unknownSize {
				assert.Equal(o.t, int64(-1), x.End)
			}
		case *fs.SeekOption:
			startPos = x.Offset
		}
	}
	assert.Equal(o.t, o.wantStart, startPos)
	// Check if ranging, mustn't have hash if offset != 0
	if gotHash && gotRange {
		assert.Equal(o.t, int64(0), startPos)
	}
	rc, err := o.Object.Open(ctx, options...)
	if err != nil {
		return nil, err
	}
	if len(o.breaks) > 0 {
		// Pop a breakpoint off
		N := o.breaks[0]
		o.breaks = o.breaks[1:]
		// and the amount of garbage to send before the error
		var G int64
		if len(o.garbage) > 0 {
			G = o.garbage[0]
			o.garbage = o.garbage[1:]
		}
		// The stream is reopened reopenRewind bytes before the error
		// but not before where it was opened
		o.wantStart += max(N+G-reopenRewind, 0)
		// If 0 then return an error immediately
		if N == 0 {
			return nil, errorTestError
		}
		// Read N bytes then G bytes of garbage then an error
		garbage := bytes.Repeat([]byte{0xFF}, int(G))
		r := io.MultiReader(&io.LimitedReader{R: rc, N: N}, bytes.NewReader(garbage), readers.ErrorReader{Err: errorTestError})
		// Wrap with Close in a new readCloser
		rc = readCloser{Reader: r, Closer: rc}
	}
	return rc, nil
}

func TestReOpen(t *testing.T) {
	for _, testName := range []string{"Normal", "WithRangeOption", "WithSeekOption", "UnknownSize"} {
		t.Run(testName, func(t *testing.T) {
			// Contents for the mock object
			var (
				reOpenTestcontents = []byte("0123456789")
				expectedRead       = reOpenTestcontents
				rangeOption        *fs.RangeOption
				seekOption         *fs.SeekOption
				unknownSize        = false
			)
			switch testName {
			case "Normal":
			case "WithRangeOption":
				rangeOption = &fs.RangeOption{Start: 1, End: 7} // range is inclusive
				expectedRead = reOpenTestcontents[1:8]
			case "WithSeekOption":
				seekOption = &fs.SeekOption{Offset: 2}
				expectedRead = reOpenTestcontents[2:]
			case "UnknownSize":
				rangeOption = &fs.RangeOption{Start: 1, End: -1}
				expectedRead = reOpenTestcontents[1:]
				unknownSize = true
			default:
				panic("bad test name")
			}

			// Start the test with the given breaks
			testReOpen := func(breaks []int64, maxRetries int) (*ReOpen, *reOpenTestObject, error) {
				srcOrig := mockobject.New("potato").WithContent(reOpenTestcontents, mockobject.SeekModeNone)
				srcOrig.SetUnknownSize(unknownSize)
				src := &reOpenTestObject{
					Object:      srcOrig,
					t:           t,
					breaks:      breaks,
					unknownSize: unknownSize,
				}
				opts := []fs.OpenOption{}
				if rangeOption == nil && seekOption == nil {
					opts = append(opts, &fs.HashesOption{Hashes: hash.NewHashSet(hash.MD5)})
				}
				if rangeOption != nil {
					opts = append(opts, rangeOption)
					src.wantStart = rangeOption.Start
				}
				if seekOption != nil {
					opts = append(opts, seekOption)
					src.wantStart = seekOption.Offset
				}
				rc, err := NewReOpen(context.Background(), src, maxRetries, opts...)
				return rc, src, err
			}

			// Reset the start after a seek, taking into account the offset
			setWantStart := func(src *reOpenTestObject, x int64) {
				src.wantStart = x
				if rangeOption != nil {
					src.wantStart += rangeOption.Start
				} else if seekOption != nil {
					src.wantStart += seekOption.Offset
				}
			}

			t.Run("Basics", func(t *testing.T) {
				// open
				h, _, err := testReOpen(nil, 10)
				assert.NoError(t, err)

				// Check contents read correctly
				got, err := io.ReadAll(h)
				assert.NoError(t, err)
				assert.Equal(t, expectedRead, got)

				// Check read after end
				var buf = make([]byte, 1)
				n, err := h.Read(buf)
				assert.Equal(t, 0, n)
				assert.Equal(t, io.EOF, err)

				// Rewind the stream
				_, err = h.Seek(0, io.SeekStart)
				require.NoError(t, err)

				// Check contents read correctly
				got, err = io.ReadAll(h)
				assert.NoError(t, err)
				assert.Equal(t, expectedRead, got)

				// Check close
				assert.NoError(t, h.Close())

				// Check double close
				assert.Equal(t, errFileClosed, h.Close())

				// Check read after close
				n, err = h.Read(buf)
				assert.Equal(t, 0, n)
				assert.Equal(t, errFileClosed, err)
			})

			t.Run("ErrorAtStart", func(t *testing.T) {
				// open with immediate breaking
				h, _, err := testReOpen([]int64{0}, 10)
				assert.Equal(t, errorTestError, err)
				assert.Nil(t, h)
			})

			t.Run("WithErrors", func(t *testing.T) {
				// open with a few break points but less than the max
				h, _, err := testReOpen([]int64{2, 1, 3}, 10)
				assert.NoError(t, err)

				// check contents
				got, err := io.ReadAll(h)
				assert.NoError(t, err)
				assert.Equal(t, expectedRead, got)

				// check close
				assert.NoError(t, h.Close())
			})

			t.Run("TooManyErrors", func(t *testing.T) {
				// open with a few break points but >= the max
				h, _, err := testReOpen([]int64{2, 1, 3}, 3)
				assert.NoError(t, err)

				// check contents - the data read before each error is
				// discarded in case it is garbage
				got, err := io.ReadAll(h)
				assert.Equal(t, errorTestError, err)
				assert.Equal(t, expectedRead[:0], got)

				// check old error is returned
				var buf = make([]byte, 1)
				n, err := h.Read(buf)
				assert.Equal(t, 0, n)
				assert.Equal(t, errTooManyTries, err)

				// Check close
				assert.Equal(t, errFileClosed, h.Close())
			})

			t.Run("ReadAt", func(t *testing.T) {
				// open
				h, src, err := testReOpen([]int64{2, 1, 3}, 10)
				assert.NoError(t, err)

				buf := make([]byte, 5)

				// Read at 0
				n, err := h.ReadAt(buf, 0)
				require.NoError(t, err)
				assert.Equal(t, 5, n)
				assert.Equal(t, expectedRead[:n], buf[:n])

				// Read at 1
				setWantStart(src, 1)
				n, err = h.ReadAt(buf[:3], 1)
				require.NoError(t, err)
				assert.Equal(t, 3, n)
				assert.Equal(t, expectedRead[1:n+1], buf[:n])

				// check position unchanged
				pos, err := h.Seek(0, io.SeekCurrent)
				require.NoError(t, err)
				assert.Equal(t, int64(0), pos)

				// check close
				assert.NoError(t, h.Close())
				_, err = h.Seek(0, io.SeekCurrent)
				assert.Equal(t, errFileClosed, err)
			})

			t.Run("Seek", func(t *testing.T) {
				// open
				h, src, err := testReOpen([]int64{2, 1, 3}, 10)
				assert.NoError(t, err)

				// Seek to end
				pos, err := h.Seek(int64(len(expectedRead)), io.SeekStart)
				assert.NoError(t, err)
				assert.Equal(t, int64(len(expectedRead)), pos)

				// Seek to start
				pos, err = h.Seek(0, io.SeekStart)
				assert.NoError(t, err)
				assert.Equal(t, int64(0), pos)

				// Should not allow seek past end
				pos, err = h.Seek(int64(len(expectedRead))+1, io.SeekCurrent)
				if !unknownSize {
					assert.Equal(t, errSeekPastEnd, err)
					assert.Equal(t, len(expectedRead), int(pos))
				} else {
					assert.Equal(t, nil, err)
					assert.Equal(t, len(expectedRead)+1, int(pos))

					// Seek back to start to get tests in sync
					pos, err = h.Seek(0, io.SeekStart)
					assert.NoError(t, err)
					assert.Equal(t, int64(0), pos)
				}

				// Should not allow seek to negative position start
				pos, err = h.Seek(-1, io.SeekCurrent)
				assert.Equal(t, errNegativeSeek, err)
				assert.Equal(t, 0, int(pos))

				// Should not allow seek with invalid whence
				pos, err = h.Seek(0, 3)
				assert.Equal(t, errInvalidWhence, err)
				assert.Equal(t, 0, int(pos))

				// check read
				dst := make([]byte, 5)
				n, err := h.Read(dst)
				assert.Nil(t, err)
				assert.Equal(t, 5, n)
				assert.Equal(t, expectedRead[:5], dst)

				// Test io.SeekCurrent
				pos, err = h.Seek(-3, io.SeekCurrent)
				assert.Nil(t, err)
				assert.Equal(t, 2, int(pos))

				// check read
				setWantStart(src, 2)
				n, err = h.Read(dst)
				assert.Nil(t, err)
				assert.Equal(t, 5, n)
				assert.Equal(t, expectedRead[2:7], dst)

				pos, err = h.Seek(-2, io.SeekCurrent)
				assert.Nil(t, err)
				assert.Equal(t, 5, int(pos))

				// Test io.SeekEnd
				pos, err = h.Seek(-3, io.SeekEnd)
				if !unknownSize {
					assert.Nil(t, err)
					assert.Equal(t, len(expectedRead)-3, int(pos))
				} else {
					assert.Equal(t, errBadEndSeek, err)
					assert.Equal(t, 0, int(pos))

					// sync
					pos, err = h.Seek(1, io.SeekCurrent)
					assert.Nil(t, err)
					assert.Equal(t, 6, int(pos))
				}

				// check read
				dst = make([]byte, 3)
				setWantStart(src, int64(len(expectedRead)-3))
				n, err = h.Read(dst)
				assert.Nil(t, err)
				assert.Equal(t, 3, n)
				assert.Equal(t, expectedRead[len(expectedRead)-3:], dst)

				// check close
				assert.NoError(t, h.Close())
				_, err = h.Seek(0, io.SeekCurrent)
				assert.Equal(t, errFileClosed, err)
			})

			t.Run("AccountRead", func(t *testing.T) {
				// Use breaks to check discarded data isn't accounted
				h, _, err := testReOpen([]int64{2, 1, 3}, 10)
				assert.NoError(t, err)

				var total int
				h.SetAccounting(func(n int) error {
					total += n
					return nil
				})

				dst := make([]byte, 3)
				n, err := h.Read(dst)
				assert.Equal(t, 3, n)
				assert.NoError(t, err)
				assert.Equal(t, 3, total)
			})

			t.Run("AccountReadDelay", func(t *testing.T) {
				h, _, err := testReOpen(nil, 10)
				assert.NoError(t, err)

				var total int
				h.SetAccounting(func(n int) error {
					total += n
					return nil
				})

				rewind := func() {
					_, err := h.Seek(0, io.SeekStart)
					require.NoError(t, err)
				}

				h.DelayAccounting(3)

				dst := make([]byte, 16)

				n, err := h.Read(dst)
				assert.Equal(t, len(expectedRead), n)
				assert.Equal(t, io.EOF, err)
				assert.Equal(t, 0, total)
				rewind()

				n, err = h.Read(dst)
				assert.Equal(t, len(expectedRead), n)
				assert.Equal(t, io.EOF, err)
				assert.Equal(t, 0, total)
				rewind()

				n, err = h.Read(dst)
				assert.Equal(t, len(expectedRead), n)
				assert.Equal(t, io.EOF, err)
				assert.Equal(t, len(expectedRead), total)
				rewind()

				n, err = h.Read(dst)
				assert.Equal(t, len(expectedRead), n)
				assert.Equal(t, io.EOF, err)
				assert.Equal(t, 2*len(expectedRead), total)
				rewind()
			})

			t.Run("AccountReadError", func(t *testing.T) {
				// Test accounting errors
				h, _, err := testReOpen(nil, 10)
				assert.NoError(t, err)

				h.SetAccounting(func(n int) error {
					return errorTestError
				})

				dst := make([]byte, 3)
				n, err := h.Read(dst)
				assert.Equal(t, 3, n)
				assert.Equal(t, errorTestError, err)
			})
		})
	}
}

// readReOpenInChunks reads all of h in chunks of bufSize bytes,
// checking the position reported by Seek matches the data returned
func readReOpenInChunks(t *testing.T, h *ReOpen, bufSize int) []byte {
	var got []byte
	buf := make([]byte, bufSize)
	for {
		n, err := h.Read(buf)
		got = append(got, buf[:n]...)
		pos, seekErr := h.Seek(0, io.SeekCurrent)
		require.NoError(t, seekErr)
		assert.Equal(t, int64(len(got)), pos)
		if err == io.EOF {
			return got
		}
		require.NoError(t, err)
	}
}

// Check garbage sent just before the stream breaks is discarded and
// the stream reopened reopenRewind bytes before the break, whatever
// size of buffer the data is read with.
func TestReOpenGarbage(t *testing.T) {
	contents := []byte(random.String(3*reopenRewind + 123))
	for _, test := range []struct {
		name    string
		breaks  []int64
		garbage []int64
		rng     *fs.RangeOption
	}{
		{name: "NoGarbage", breaks: []int64{5000, 100, 6000}},
		{name: "MaxGarbage", breaks: []int64{5000, 100, 6000}, garbage: []int64{reopenRewind, reopenRewind, reopenRewind}},
		{name: "SomeGarbage", breaks: []int64{5000, 100, 6000, 500}, garbage: []int64{1, 50, 4000, 0}},
		{name: "RangeGarbage", breaks: []int64{5000, 100}, garbage: []int64{reopenRewind, 50}, rng: &fs.RangeOption{Start: 1000, End: int64(len(contents)) - 1001}},
	} {
		for _, bufSize := range []int{1, 100, reopenRewind, 2 * len(contents)} {
			t.Run(fmt.Sprintf("%s/%d", test.name, bufSize), func(t *testing.T) {
				src := &reOpenTestObject{
					Object:  mockobject.New("potato").WithContent(contents, mockobject.SeekModeNone),
					t:       t,
					breaks:  slices.Clone(test.breaks),
					garbage: slices.Clone(test.garbage),
				}
				// Hashes can only be requested when reading from the start
				want := contents
				opts := []fs.OpenOption{&fs.HashesOption{Hashes: hash.NewHashSet(hash.MD5)}}
				if test.rng != nil {
					opts = []fs.OpenOption{test.rng}
					src.wantStart = test.rng.Start
					want = contents[test.rng.Start : test.rng.End+1]
				}
				h, err := NewReOpen(context.Background(), src, 10, opts...)
				require.NoError(t, err)
				got := readReOpenInChunks(t, h, bufSize)
				assert.Equal(t, want, got)
				assert.Empty(t, src.breaks, "not all breaks were hit")
				assert.NoError(t, h.Close())
			})
		}
	}

	// Check only the data more than reopenRewind bytes before the
	// break is returned when the retries run out
	t.Run("TooManyErrors", func(t *testing.T) {
		src := &reOpenTestObject{
			Object:  mockobject.New("potato").WithContent(contents, mockobject.SeekModeNone),
			t:       t,
			breaks:  []int64{5000},
			garbage: []int64{100},
		}
		h, err := NewReOpen(context.Background(), src, 1)
		require.NoError(t, err)
		got, err := io.ReadAll(h)
		assert.Equal(t, errorTestError, err)
		assert.Equal(t, contents[:5100-reopenRewind], got)
	})

	// Check an error after all the data has been read is returned
	// rather than retried as it can't be a truncation
	t.Run("ErrorAtEnd", func(t *testing.T) {
		src := &reOpenTestObject{
			Object: mockobject.New("potato").WithContent(contents, mockobject.SeekModeNone),
			t:      t,
			breaks: []int64{int64(len(contents))},
		}
		h, err := NewReOpen(context.Background(), src, 10)
		require.NoError(t, err)
		got, err := io.ReadAll(h)
		assert.Equal(t, errorTestError, err)
		assert.Equal(t, contents[:len(got)], got)
	})
}
