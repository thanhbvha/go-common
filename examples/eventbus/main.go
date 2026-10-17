package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"time"

	"github.com/thanhbvha/go-common/eventbus"

	// Import all drivers to register them
	_ "github.com/thanhbvha/go-common/eventbus/driver/kafka"
	_ "github.com/thanhbvha/go-common/eventbus/driver/memory"
	_ "github.com/thanhbvha/go-common/eventbus/driver/nats"
	_ "github.com/thanhbvha/go-common/eventbus/driver/redis"
	redisopts "github.com/thanhbvha/go-common/eventbus/driver/redis"

	"github.com/thanhbvha/go-common/kafka"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/nats"
	"github.com/thanhbvha/go-common/redis"
	"github.com/thanhbvha/go-common/utils/graceful"
)

// Example Payload
type UserCreatedEvent struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

func main() {
	driverName := flag.String("driver", "redis", "Tên driver muốn sử dụng (memory, redis, nats, kafka)")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize Logger
	logOpts := logger.DefaultOptions()
	logOpts.Level = 0 // Info
	logOpts.StdOut = true
	sysLogger := logger.New(logOpts)
	logger.SetDefault(sysLogger)
	defer logger.Close()

	// 2. Tắt hệ thống Graceful
	sd := graceful.NewShutdown(15 * time.Second)

	// 3. Setup Driver (Cấu hình Connection cho từng loại Broker)
	switch *driverName {
	case "redis":
		sysLogger.Info("Khởi tạo Redis Connection...")
		redisCfg := redis.DefaultConfig()
		redisCfg.Host = "127.0.0.1"
		redisCfg.Port = 6379
		redisClient := redis.MustConnect(ctx, redisCfg)
		redis.SetDefault(redisClient) // Cắm vào biến toàn cục để Eventbus lấy ra xài

		sd.Register(func(ctx context.Context) error {
			sysLogger.Info("🛑 Đóng kết nối Redis...")
			return redisClient.Close()
		})

	case "nats":
		sysLogger.Info("Khởi tạo NATS Connection...")
		natsCfg := nats.DefaultConfig()
		natsCfg.URLs = []string{"nats://127.0.0.1:4222"}
		natsClient := nats.MustConnect(ctx, natsCfg)
		nats.SetDefault(natsClient)

		sd.Register(func(ctx context.Context) error {
			sysLogger.Info("🛑 Đóng kết nối NATS...")
			return natsClient.Close()
		})

	case "kafka":
		sysLogger.Info("Khởi tạo Kafka Connection...")
		kafkaCfg := kafka.DefaultConfig()
		kafkaCfg.Brokers = []string{"127.0.0.1:9092"}
		kafkaClient := kafka.MustConnect(ctx, kafkaCfg)
		kafka.SetDefault(kafkaClient)

		sd.Register(func(ctx context.Context) error {
			sysLogger.Info("🛑 Đóng kết nối Kafka...")
			kafkaClient.Close()
			return nil
		})

	case "memory":
		sysLogger.Info("Sử dụng Memory Driver (Không cần external connection)")
	default:
		sysLogger.Error("Driver không được hỗ trợ", "driver", *driverName)
		os.Exit(1)
	}

	// 4. Initialize the EventBus
	bus, err := eventbus.Open(
		eventbus.Config{
			Driver:        *driverName,
			Namespace:     "demo",
			Concurrency:   5,
			MaxDeliveries: 6,
			Logger:        sysLogger,
		},
	)
	if err != nil {
		sysLogger.Error("Failed to open eventbus", "err", err)
		os.Exit(1)
	}

	// ---------------------------------------------------------
	// KỊCH BẢN 1: WORK-QUEUE (1 Event -> 1 Worker xử lý)
	// Đảm bảo event được xử lý độc quyền (không trùng lặp)
	// ---------------------------------------------------------
	bus.Register("send_welcome_email",
		eventbus.Handle(func(ctx context.Context, job eventbus.Job, evt UserCreatedEvent) error {
			sysLogger.Info("[Work-Queue] Sending welcome email...", "email", evt.Email)

			// Giả lập lỗi mạng rớt -> Cần retry
			if time.Now().UnixNano()%2 == 0 {
				sysLogger.Warn("[Work-Queue] Network error, will retry later!")
				// Báo lỗi để EventBus tự động retry theo RetryPolicy (Exponential Backoff)
				return errors.New("smtp connection timeout")
			}

			if evt.Email == "" {
				return eventbus.MarkPermanent(errors.New("invalid email format")) // Sai định dạng -> Vứt thẳng vào DLQ
			}

			sysLogger.Info("[Work-Queue] Email sent successfully", "email", evt.Email)
			return nil
		}),
		eventbus.WithConcurrency(20), // Override Global Concurrency (5) -> Local (20)
		eventbus.WithMaxDeliveries(5),
		redisopts.WithBatchSize(20), // Kéo 20 tin nhắn mỗi lần cho khớp với Concurrency
		redisopts.WithBlock(2*time.Second),
	)

	// ---------------------------------------------------------
	// KỊCH BẢN 2: FAN-OUT (PUB/SUB) (1 Event -> N Handler xử lý)
	// Mỗi Handler sẽ nhận được 1 bản sao của Event để xử lý độc lập
	// ---------------------------------------------------------

	// Handler 1: Gửi Notification
	bus.Subscribe("user.registered", "notification_service",
		eventbus.Handle(func(ctx context.Context, job eventbus.Job, evt UserCreatedEvent) error {
			sysLogger.Info("[Pub/Sub - Handler 1] Pushing notification", "user_id", evt.UserID)
			return nil
		}),
		eventbus.WithConcurrency(3),
	)

	// Handler 2: Cập nhật CRM
	bus.Subscribe("user.registered", "crm_service",
		eventbus.Handle(func(ctx context.Context, job eventbus.Job, evt UserCreatedEvent) error {
			sysLogger.Info("[Pub/Sub - Handler 2] Syncing User to CRM system", "user_id", evt.UserID)
			return nil
		}),
	)

	// 3. Khởi động EventBus (Bắt đầu lắng nghe các Queue và Subscription)
	sysLogger.Info("🚀 Starting EventBus...")
	if err := bus.Start(ctx); err != nil {
		sysLogger.Error("Failed to start eventbus", "err", err)
		os.Exit(1)
	}

	// 4. Test chức năng Publish (Gửi Event)
	go func() {
		// Đợi Bus start xong
		time.Sleep(1 * time.Second)

		evt := UserCreatedEvent{UserID: "u123", Email: "demo@example.com"}

		// A. Gửi vào Work-Queue ngay lập tức
		sysLogger.Info(">>> Publishing to Work-Queue (send_welcome_email)")
		err := bus.Enqueue(ctx, "send_welcome_email", evt)
		if err != nil {
			sysLogger.Error("Publish error", "err", err)
		}

		// B. Publish dạng Pub/Sub Fan-out
		sysLogger.Info(">>> Publishing to Pub/Sub (user.registered)")
		err = bus.PublishEvent(ctx, "user.registered", evt)
		if err != nil {
			sysLogger.Error("Publish error", "err", err)
		}

		// C. Gửi Job Delay (Hẹn giờ chạy sau 5 giây)
		sysLogger.Info(">>> Publishing Delayed Job (send_welcome_email in 5s)")
		evt.Email = "emaildelay@mail.com"
		err = bus.Enqueue(ctx, "send_welcome_email", evt, eventbus.WithDelay(5*time.Second))
		if err != nil {
			sysLogger.Error("PublishDelay error", "err", err)
		}
	}()

	// 5. Chờ tín hiệu tắt từ Hệ Điều Hành (Graceful Shutdown)
	sd.Register(func(ctx context.Context) error {
		sysLogger.Info("🛑 Shutting down EventBus...")
		return bus.Stop(ctx)
	})

	sd.Wait()
	sysLogger.Info("Goodbye!")
}
