package main

import (
	"context"
	"fmt"
	"time"

	"github.com/thanhbvha/go-common/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func main() {
	ctx := context.Background()

	// 1. Khởi tạo cấu hình Telemetry
	cfg := telemetry.Config{
		ServiceName:    "demo-telemetry-service",
		ServiceVersion: "v1.0.0",
		Environment:    "development",
		Endpoint:       "localhost:4317", // Địa chỉ mặc định của OpenTelemetry Collector hoặc Jaeger/Signoz
		EnableTracing:  true,
		EnableMetrics:  true,
	}

	// 2. Khởi tạo module
	tel, err := telemetry.Init(ctx, cfg)
	if err != nil {
		fmt.Printf("[Lỗi] Không thể khởi tạo telemetry: %v\n", err)
	} else {
		// Đảm bảo flush toàn bộ dữ liệu trước khi chương trình kết thúc
		defer tel.Shutdown(ctx)
	}

	fmt.Println("--- Telemetry Example ---")

	// 3. Sử dụng Metrics (Bộ đếm)
	requestCounter := telemetry.MustCounter("api_requests_total", "Tổng số lượt gọi API")

	// 4. Sử dụng Tracing (Giám sát luồng chạy)
	fmt.Println("Đang xử lý request...")
	
	// Khởi tạo một Trace mới
	ctx, span := telemetry.StartSpan(ctx, "HandleCheckoutAPI")
	
	// Gắn tag (attribute) để sau này dễ search trên Dashboard (ví dụ: tìm tất cả lỗi của user_123)
	telemetry.SetAttributes(span, attribute.String("user.id", "user_123"), attribute.String("order.id", "ORD-8888"))
	
	// Giả lập thời gian chạy logic
	time.Sleep(150 * time.Millisecond) 
	
	// Gắn thêm 1 metric đếm số lượng
	requestCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "success")))
	
	// Kết thúc Trace
	span.End()

	fmt.Printf("Xử lý thành công! TraceID của bạn là: %s\n", span.SpanContext().TraceID().String())
	fmt.Println("Nếu anh có cài đặt Jaeger ở cổng 4317, hãy mở UI lên và tìm TraceID này!")
}
