package graphql

import (
	"context"
	"sync"
	"time"
)

// BatchFunc là hàm mà Developer phải tự implement.
// Nó nhận vào một danh sách các ID, truy vấn DB một lần duy nhất, và trả về một Map map[ID]Data.
type BatchFunc[K comparable, V any] func(ctx context.Context, keys []K) (map[K]V, error)

// ConfigDL cấu hình cho một DataLoader
type ConfigDL struct {
	Wait     time.Duration // Thời gian chờ tối đa để gom (batching) các request (Mặc định: 16ms)
	MaxBatch int           // Số lượng ID tối đa trong 1 lần query DB (Mặc định: 100)
}

// DataLoader là công cụ chống N+1 bằng cách gom nhóm các request tải dữ liệu riêng lẻ thành 1 request tổng.
type DataLoader[K comparable, V any] struct {
	batchFn  BatchFunc[K, V]
	wait     time.Duration
	maxBatch int

	mu    sync.Mutex
	cache map[K]V
	batch *batch[K, V]
}

type batch[K comparable, V any] struct {
	keys    []K
	done    chan struct{}
	results map[K]V
	err     error
}

// NewDataLoader khởi tạo một DataLoader mới
func NewDataLoader[K comparable, V any](batchFn BatchFunc[K, V], opts ...ConfigDL) *DataLoader[K, V] {
	wait := 16 * time.Millisecond
	maxBatch := 100
	if len(opts) > 0 {
		if opts[0].Wait > 0 {
			wait = opts[0].Wait
		}
		if opts[0].MaxBatch > 0 {
			maxBatch = opts[0].MaxBatch
		}
	}
	return &DataLoader[K, V]{
		batchFn:  batchFn,
		wait:     wait,
		maxBatch: maxBatch,
		cache:    make(map[K]V),
	}
}

// Load tải dữ liệu cho 1 key. Nếu key đã có trong cache, trả về ngay.
// Nếu không, nó sẽ chờ vài mili-giây để gom chung với các Load() khác.
func (l *DataLoader[K, V]) Load(ctx context.Context, key K) (V, error) {
	l.mu.Lock()

	// 1. Kiểm tra L1 Cache (chỉ sống trong request hiện tại)
	if val, ok := l.cache[key]; ok {
		l.mu.Unlock()
		return val, nil
	}

	// 2. Tạo hoặc tham gia vào một Batch đang gom
	var b *batch[K, V]
	if l.batch == nil || len(l.batch.keys) >= l.maxBatch {
		b = &batch[K, V]{
			done:    make(chan struct{}),
			results: make(map[K]V),
		}
		l.batch = b
		go l.dispatchBatch(ctx, b)
	} else {
		b = l.batch
	}

	b.keys = append(b.keys, key)
	l.mu.Unlock()

	// 3. Block goroutine này để đợi Batch gọi DB xong
	<-b.done

	if b.err != nil {
		var zero V
		return zero, b.err
	}

	val, ok := b.results[key]
	if !ok {
		// Key không tồn tại trong DB, trả về giá trị Zero và nil error
		var zero V
		return zero, nil
	}

	return val, nil
}

// LoadAll hỗ trợ tải nhiều key cùng lúc
func (l *DataLoader[K, V]) LoadAll(ctx context.Context, keys []K) ([]V, []error) {
	results := make([]V, len(keys))
	errors := make([]error, len(keys))
	
	var wg sync.WaitGroup
	wg.Add(len(keys))
	
	for i, key := range keys {
		go func(i int, key K) {
			defer wg.Done()
			results[i], errors[i] = l.Load(ctx, key)
		}(i, key)
	}
	
	wg.Wait()
	return results, errors
}

func (l *DataLoader[K, V]) dispatchBatch(ctx context.Context, b *batch[K, V]) {
	// Chờ thêm một khoảng thời gian (wait) để gom thêm các ID khác
	time.Sleep(l.wait)

	l.mu.Lock()
	if l.batch == b {
		l.batch = nil // Reset batch để các Load sau tạo batch mới
	}
	l.mu.Unlock()

	// Lọc trùng ID để tránh query thừa
	keys := make([]K, 0, len(b.keys))
	seen := make(map[K]bool)
	for _, k := range b.keys {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}

	// Thực thi Batch Function do Dev cung cấp (Gọi DB/Redis)
	res, err := l.batchFn(ctx, keys)
	b.results = res
	b.err = err

	// Đưa kết quả vào Cache
	if err == nil && res != nil {
		l.mu.Lock()
		for k, v := range res {
			l.cache[k] = v
		}
		l.mu.Unlock()
	}

	// Đánh thức tất cả các goroutine đang đợi <-b.done
	close(b.done)
}
