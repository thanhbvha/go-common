# Kế hoạch refactor & tối ưu module `websocket` để đạt production

> Ngày: 2026-11-07 · Phạm vi: `websocket/**` (core, pool, limiter, pubsub/{redis,nats}, routing/{,redis,nats}, adapter/{nethttp,fiber,gin,echo}, integration/eventbus).
> Trạng thái hiện tại theo `docs/PRODUCTION.md`: **Integration candidate**. Phần còn thiếu theo tài liệu đó: *Linux/TLS capacity, broker outage và slow-client soak*.
> Cách review: đọc code toàn bộ module (không đọc `*_test.go` kỹ, không đọc `examples/` ngoài vài lệnh gọi). **Chưa chạy** test, race hay benchmark. Mục nào ghi *[xác minh]* thì cần viết test để xác nhận trước khi sửa.

---

## Theo dõi tiến độ (cập nhật 2026-11-09)

Quy ước: ✅ xong và đã được review · 🟡 xong code, chờ review · 🔶 làm một phần · ⬜ chưa làm.
Toàn bộ thay đổi hiện **chưa commit**. Quy trình: làm xong một step thì dừng để review rồi mới sang step kế.

| Step | Nội dung | Trạng thái |
|---|---|---|
| 1 | S6: nối `wshttp.Gate` vào 4 adapter (kiểm tra trước authenticate, rate limit theo IP, `Retry-After`) | ✅ đã review |
| 2 | S5: `NewProductionManager`, pubsub mặc định `StayUnready`, `Deprecated` singleton, `ValidateProduction` bắt `AuthRequired` cho cluster | ✅ đã review |
| 3 | S3: deprecate phòng legacy, sửa tài liệu và ví dụ, test hai-user-hai-shard | ✅ đã review |
| 4 | S8 còn lại (`/livez`, `/readyz`) và S7 (chỉ sửa `Ready` chập chờn) | 🟡 xong code, chờ review |
| 5 | Phase 0 còn thiếu: test R4, R10-loopback, race `-count=10`, test tích hợp Redis/NATS, Graft build | 🟡 xong, chờ review; baseline toàn repo còn lỗi `internal/perftest` |
| 6 | Phase 2 theo mục 6b | 🔶 R3/R4/R7/R10(loopback)/R11/R12/R13/O3 xong phần lõi, chờ review; còn lại xem "Lịch sử triển khai và việc tiếp theo" |

> **Step 6, trạng thái 🔶.** Đã làm (race `-count=3` xanh toàn `websocket/...`, `go vet` và `go build ./...` sạch):
> - **R4:** `Shard.admitting` đếm admission đang giữ shard; `cleanupEmptyShards` bỏ qua shard còn admission; `acquireShard` thử lại nếu shard vừa bị gỡ. Test đỏ → xanh 10/10, đã bỏ build tag `ws_repro`.
> - **R10 loopback:** handler pubsub của `core.Manager` bỏ qua tin có `NodeID` của chính node (giống đường Redis/NATS thật). Test đỏ → xanh 10/10, đã bỏ build tag.
> - **R3 (xong phần theo 6b):** `cleanupEmptyShards` nay `Unsubscribe` channel của shard. Redis/NATS `Subscribe/Unsubscribe` quyết định dưới `psm.mu` nhưng gọi broker **ngoài khoá**, rồi commit lại dưới khoá (kiểm tra `desired` còn hiệu lực, đóng subscription thừa nếu bị race). Thêm `WithExecutor(partitions, queueSize)` cho cả hai backend (mặc định vẫn 4×256) và `pubsub.DropLog` giới hạn log từ chối dispatch 1 dòng/giây kèm số đã nén. Test: `pubsub/droplog_test.go`, `{redis,nats}/executor_option_test.go`. **Chưa làm:** gộp nhiều channel Redis vào một `PubSub` (vẫn mỗi channel một kết nối, tối đa ~`MaxShards`+1).
> - **R7 (xong phần lõi):** `SlowConsumerDropThreshold` mặc định 1 → 3 (chỉ đếm lần rớt liên tiếp, reset khi gửi được). `requestClose(code, reason)` nay để **writer** gửi close frame có mã trước khi đóng socket: 1013 khi queue đầy (slow consumer), 1008 khi vi phạm lặp lại; đường drain-deadline dùng code 0 (đóng ngay, không gửi frame vì writer có thể đang kẹt). Có timer `WriteWait` làm chốt chặn nếu writer không thoát. `ManagerConfig.MaxViolations` (mặc định 20): rate-limit/malformed/payload quá lớn/validate lỗi liên tiếp thì đóng 1008; một event hợp lệ hoàn chỉnh reset chuỗi. Sửa `TestConnectionQueueFullIsNonBlocking`; test mới trong `core/step6_test.go`. **Chưa làm:** error envelope gửi lại client cho từng vi phạm (hiện chỉ đóng khi lặp lại), close frame cho các đường `Close()` trực tiếp khác ("Authentication expired", "Session superseded", "Shard stopped") vẫn là 1006.
> - **R11:** bridge chỉ retry khi còn `QueueFull/Unavailable/Unknown` (Router đã tự re-resolve `Stale/NotFound`); `PublishCommand` nhận payload rỗng thành `{}`.
> - **R12:** executor log panic qua `wsrecovery`, vẫn giữ counter.
> - **R13:** inbound không phải UTF-8 bị tính là malformed; broadcast legacy liên node từ chối payload không phải UTF-8 (có log).
> - **O3:** xoá `pool.WorkerPool`, `pool.BufferPool` (cùng test), `ConnectionLimiter.Cleanup/cleanupTick/lastCleanup`. Không còn caller.
> - Test mới: `core/step6_test.go`.
>
> **Chưa làm trong Step 6:** xem mục "Lịch sử triển khai và việc tiếp theo" ngay dưới đây.

---

## Lịch sử triển khai và việc tiếp theo (cập nhật 2026-11-24)

### Lịch sử

Toàn bộ nằm trên nhánh `websocket`. Step 1–5 là các thay đổi đã có từ trước và đã review. Step 6 được làm thành ba đợt liên tiếp, mỗi đợt dừng lại để review:

| Đợt | Nội dung | Kiểm chứng |
|---|---|---|
| 6a | R4 (`Shard.admitting` + `acquireShard`), R10-loopback (handler pubsub của `core.Manager` bỏ tin cùng `NodeID`), R11, R12, R13, O3, `SlowConsumerDropThreshold` 1 → 3, `cleanupEmptyShards` gọi `Unsubscribe`. Bỏ build tag `ws_repro` của hai test tái hiện | Hai test đỏ xanh 10/10 với `-race` |
| 6b | R3: `Subscribe/Unsubscribe` của Redis/NATS gọi broker **ngoài** `psm.mu` rồi commit lại dưới khoá; `WithExecutor(partitions, queueSize)`; `pubsub.DropLog` (1 dòng log/giây) | Test mới `droplog_test.go`, `executor_option_test.go`; race `-count=5` |
| 6c | R7 lõi: close frame có mã do writer gửi (1013 slow consumer, 1008 vi phạm lặp lại); `ManagerConfig.MaxViolations` (mặc định 20); `readPump` không tự `Close` khi đã có `requestClose` | Test mới trong `core/step6_test.go`; race `-count=5` cho `core` |

Kết quả cuối: `go build ./...`, `go vet ./websocket/...`, `go test -race -count=3 ./websocket/...` đều xanh trên Windows. `go test ./...` toàn repo vẫn đỏ vì `internal/perftest.TestChildBrokerDrainBarrier` (ngoài phạm vi WebSocket, đã ghi ở Step 5).

**Thay đổi hành vi cần biết khi review:**
- `pool.WorkerPool`, `pool.BufferPool`, `ConnectionLimiter.Cleanup` bị xoá (breaking, đã chốt ở Q2).
- Client chậm bị ngắt sau 3 lần rớt liên tiếp (trước là 1); client vượt rate limit/gửi JSON sai liên tiếp 20 lần bị đóng bằng 1008 (trước đây bị bỏ qua lặng lẽ).
- Bridge EventBus không còn retry khi kết quả chỉ là `Stale/NotFound`.
- Tin inbound không phải UTF-8 bị coi là malformed; broadcast legacy liên node từ chối payload không phải UTF-8.

### Việc tiếp theo

Theo thứ tự ưu tiên đề xuất:

1. ✅ **Test tích hợp Redis/NATS thật (2026-11-26).** Docker Desktop đã bật; các container `local-*` có sẵn đều Exited/Created nên dựng fixture riêng (Redis 7, NATS `-js`, port ngẫu nhiên; thêm container `golang:1.26` + `redis-server` 8.0.2 cho các test tự bật `redis-server`), chạy xong đã xoá hết. Với `-race -tags=integration`: `pubsub/{redis,nats}` xanh (`-count=5`), `routing`, `routing/{redis,nats}` (contract), `integration/eventbus` (Redis routing, Redis Streams, NATS JetStream) và `examples/websocket/cluster` (legacy-redis, legacy-nats, redis-nats, nats-redis) xanh. **Chưa chạy:** các case Kafka (`kafka-redis`, `kafka-nats`, `TestKafkaMixedDelivery`), `TestPerformanceWebSocket`, chaos mất broker.
2. 🟡 **R7 còn lại, một phần (2026-11-26):** `Authentication expired` và `DisconnectIdentity` nay gửi close 1008; `Session superseded` gửi `core.CloseSessionSuperseded` (4000), qua `requestClose` nên writer gửi frame. Test mới `TestDisconnectIdentitySendsPolicyFrame`. **Đã làm thêm (2026-11-26), theo hướng "adapter từ chối trước Upgrade":** `Manager.CheckAdmission(identity, shardID)` (kiểm quota user/account/tenant, manager đang nhận, shard còn chỗ; không giữ chỗ, chỉ tư vấn) và `wshttp.Gate.AdmitIdentity`; cả 4 adapter gọi ngay sau authenticate, trước `ConnectionKey`/`Upgrade`, trả **503 + `Retry-After`**. Tách `quotaKeysLocked` dùng chung với `reserveIdentityConnection`. Test: `nethttp.TestQuotaRejectionIsHTTP503BeforeUpgrade` (kết nối thứ hai của cùng user nhận 503 thật qua `Dial`). Race `-count=3` xanh cho `adapter/...` và `core`. Kiểm tra này chỉ giảm chứ không loại hẳn đường đóng 1006: hai request đồng thời vượt qua pre-check thì request thua vẫn bị `HandleIdentityConnection` đóng sau Upgrade.
> **Hoàn tất R7 (2026-11-26):**
> - Test `TestQuotaRejectionIsHTTP503BeforeUpgrade` có cho cả 4 adapter (Gin/Echo/Fiber dựng server thật và `Dial`).
> - **Close frame cho đường từ chối trước khi pump chạy:** `Connection.reject(code, reason)` giành `runOnce` để chiếm quyền ghi (nếu pump đã chạy thì uỷ cho writer), gửi close frame rồi đóng socket; `rejectAndWait` cho caller cần socket đã đóng khi trả về. `Manager.closeRaw` làm việc tương tự cho socket chưa có wrapper. Mã: "Shard stopped" → 1001, "Shard at max capacity"/"registration rejected"/"Session registration failed"/"server capacity reached"/"shard capacity reached" → 1013, lỗi cấp wrapper → 1011.
> - **Error envelope:** mỗi vi phạm gửi `{"type":"error","data":{"code":"rate_limited|malformed|payload_too_large|invalid_payload"}}`, tối đa 1 bản/giây/connection (`sendQuiet`, không tính vào slow-consumer, queue đầy thì bỏ). Chỉ gửi mã, không gửi chi tiết lỗi. Hành vi đổi: client sai định dạng nay nhận thêm frame `error`; ví dụ `v2/TestV2RejectsClientSelectedIdentityFields` được sửa để đọc frame này.
> - Test mới: `TestRejectBeforePumpsSendsCloseFrame`, `TestShardAtCapacityRejectsWithTryAgainLater`, `TestViolationSendsRateLimitedErrorEnvelope`.
> - **Tài liệu giao thức client (2026-11-26):** `websocket/CLIENT_PROTOCOL.md` (liên kết từ `websocket/README.md`) liệt kê HTTP status trước Upgrade, bảng close code (1001/1008/1011/1013/4000) kèm hành động của client, các mã `error` envelope và quy tắc reconnect. Đã đối chiếu với code; chỉ là tài liệu, không có test tự động giữ nó khớp với code.
> - **R7: đóng (2026-11-26).** Giới hạn còn lại là của giao thức, không sửa ở server: close frame chỉ gửi best-effort (deadline `WriteWait`), client không đọc hoặc mạng chết vẫn thấy 1006. Hai đường `Close` do `readPump`/`writePump` kết thúc là dọn dẹp khi socket đã hỏng nên không gửi mã. Chưa có test riêng cho close 1001 ("Shard stopped" qua `reject`), 1011 (`closeRaw`) và 4000 (`Session superseded`); `MaxViolations=20` và `SlowConsumerDropThreshold=3` chờ chỉnh sau soak Phase 4.
3. ⏹ **R3 còn lại: đóng, không làm (2026-11-26).** Gộp channel Redis vào một `PubSub` chỉ ảnh hưởng đường legacy single-node/dev (ở `Distributed` pubsub là `NoopManager`, không có kết nối). Mỗi channel một kết nối, trần ~`MaxShards`+1 (1001) mỗi node; ví dụ dùng ≤64 shard. NATS không bị vấn đề này. Chỉ mở lại nếu soak Phase 4 đo thấy số kết nối Redis hoặc thời gian resubscribe là vấn đề. Cách giảm rủi ro không cần code: đặt `MaxShards` thấp (64–128) cho cấu hình dùng pubsub legacy. Các con số (trần client Redis 10.000, chi phí mỗi kết nối) là suy luận, chưa đo.
4. ✅ **R10 còn lại (2026-11-26):** đã **xoá hẳn** `node_status`: `Manager.sendNodeStatus` và handler log, `SendNodeStatus` ở interface `pubsub.Manager`/Redis/NATS/`NoopManager`, hằng `MessageTypeNodeStatus` và mục của nó trong `knownTypes` (tin `node_status` nay bị decoder từ chối như loại không biết). Lý do: chỉ publish một lần lúc khởi động, node nhận chỉ ghi log, không ai dùng; node discovery thực sự do lease của `routing.Gateway` đảm nhiệm. Build/vet sạch, race `-count=2` xanh cho `websocket/...` và `examples/websocket/...`. **Hoãn sang Phase 4:** đo khoảng mất tin khi resubscribe (cần chaos broker); phần tài liệu best-effort đã có ở "Delivery contract" và `CLIENT_PROTOCOL.md`.
5. ✅ **Xoá hẳn singleton và API deprecated (S5, 2026-11-26):** đã xoá `core.GetGlobalManager`, `core.SetAuthorizer`, `limiter.GetGlobalConnectionLimiter`, `pubsub.Set/GetGlobalManager`, `redispubsub`/`natspubsub` `GetGlobalManager` + `NewManager()`, và `HandleHealthCheck` của 4 adapter (route `/health` vẫn mount, trỏ vào `HandleReady`). Adapter để trống `Manager`/`ConnectionLimiter` thì tạo mới theo handler (`core.NewManager()`, `NewConnectionLimiter(100)`), không còn dùng chung singleton. Còn lại: registry sự kiện toàn cục `core.RegisterHandler`/`defaultEventRegistry`/`defaultObserver` (chưa gắn Deprecated).
6. ✅ **Test S8 connect-storm (2026-11-26):** `core.TestReadinessStaysStableDuringConnectStorm`: 32 worker × 40 vòng connect/ngắt xen kẽ qua `HandleIdentityConnection` trong khi một goroutine gọi `Readiness()` liên tục (hàng chục nghìn lần). Mặc định: 0 lần not-ready, bộ đếm hội tụ về 0. **Đã kiểm chứng test có sức phân biệt:** đặt `accountingGrace = 0` thì readiness chập chờn ~14–20 nghìn lần/16–23 nghìn probe (`AccountingValid:false`), đúng lỗi S8 gốc. Race `-count=5` xanh. Lưu ý: test dùng `mockConn` trong tiến trình, không có TLS/mạng thật; kịch bản skew kéo dài hơn 5s dưới tải không được kiểm.
7. ✅ **Phase 3 (2026-11-26), đã review; O5 hoãn theo 6b.**
> - **O1:** `core.Manager.Snapshot()` (kiểu có sẵn, không dựng map, không khoá accounting) và gói `websocket/metrics` (`Register(meter, Sources{Manager,Gateway,Router})`, OTel observable instrument nên không có goroutine lấy mẫu). Gauge: connections, shards, ready, executor queue, handlers running/quarantined, gateway sessions/rooms. Counter: opened/closed, message/byte in/out, slow_consumers, `drops{reason}`, drain.forced, subscription_errors, gateway lease_failures/rejected, router attempts/unknown/stale_retries. Nhãn chỉ là enum cố định. **Chưa làm:** sampling log và che `userID/clientIP` trong log; histogram mở rộng (chỉ có 6 bucket cũ trong `GetStats`); độ sâu queue theo phân vị.
> - **O2:** `Config.AdminToken` ở cả 4 adapter bảo vệ `HandleStats` và `HandleShardManagement` (Bearer, so sánh hằng thời gian, rỗng thì luôn 401). `/readyz` chỉ trả `{"status":...}`, không còn `checks` (lộ topology). Ví dụ `examples/websocket` truyền `AdminToken`. Breaking: client gọi stats qua adapter mà không có token nay nhận 401.
> - **O4:** `fiber.Server.Start()` bind cổng đồng bộ (`net.Listen`) rồi mới trả nil, thay cho `time.After(500ms)`; CORS không còn bật mặc định (`MiddlewareManager.AllowOrigins`, rỗng = không có CORS). Điều phối thứ tự với `Manager.Shutdown` chỉ ghi trong runbook (dùng `foundation/adapter/fiber.Component`), **chưa có helper mới**.
> - **O6:** `websocket/PRODUCTION_RUNBOOK.md` (sizing, LB/proxy, endpoint, alert, rollout/drain, ma trận durability, troubleshooting); các con số bộ nhớ là ước lượng.
> - Test mới: `metrics/otel_test.go`, `wshttp.TestAdminAllowed`, `TestAdminEndpointsRequireToken` (4 adapter), `TestReadyBodyDoesNotExposeTopology`, `fiber.TestStartReportsBusyPortAndServesWhenBound`. `go vet` và `go test -race -count=1 ./websocket/... ./examples/websocket/...` xanh.
8. 🟡 **Phase 4 (2026-11-26), một phần, chờ review.** Kết quả đầy đủ: `websocket/benchmarks/soak-20261126.md`.
> - **goleak** ở `core`, `pubsub`, `routing`. Tìm ra rò thật: `requestClose` trên connection chưa chạy pump chờ `WriteWait` một writer không bao giờ có (nay thoát khi `closedDone`); 6 test không dọn executor/connection đã sửa.
> - **Fuzz mới:** `routing.FuzzRPCReceive`, `FuzzProcessMessage` (Redis, NATS), đã nối vào `test-fuzz.sh`; mỗi cái 30 s, 2–3 triệu lần, không panic. `crossNodePayload` chỉ bọc `Payload()` đã được fuzz `DecodeCrossNodeMessage` phủ.
> - **Bảo mật:** test e2e token hết hạn khi đang kết nối (đóng 1008), identity hết hạn lúc upgrade, quota per-user dưới upgrade đồng thời. **Sửa lỗi thật:** identity đã hết hạn trước đây vẫn được đăng ký; nay `CheckAdmission` trả `ErrIdentityExpired` (401 trước Upgrade) và `HandleIdentityConnection` đóng 1008.
> - **Linux/TLS (Docker, 2 gateway + nginx TLS + Redis):** 10.000 kết nối idle, 0 lỗi, ~17,5 KB/conn; soak 30 phút 9.000.000/9.000.000 tin, goroutine phẳng, 0 readiness chập chờn; Redis dừng 30 s và 5 phút: không rớt socket, hết outage ready lại sau 1,6 s và 10,8 s, không restart node. Harness: `examples/websocket/soak/run.py`.
> - **Chưa đạt/chưa làm:** p99 fan-out 277–587 ms so với mục tiêu 150 ms (chưa tách nguyên nhân, máy dùng chung); chưa đo số tin mất trong lúc broker chết; chưa kill -9 node, reconnect storm 100%, NATS outage, soak 24 h, 5.000 kết nối có tải. Vì vậy **chưa nâng `docs/PRODUCTION.md` lên Production**.

**Rủi ro mở:** ngưỡng `MaxViolations=20` và `SlowConsumerDropThreshold=3` là số chọn theo suy luận, chưa đo với client thật; cần chỉnh sau soak.

Các mục đã làm ở phiên trước (chưa đánh dấu review): S1, S2a, S2b, S4, S8 (một phần), R8, R10 (handler `Chat`).

---

## 0. Đính chính so với review trước

Sau khi đọc `routing/*`, `pubsub/nats`, các adapter và `eventbus`, một số nhận định cũ cần chỉnh:

| Nhận định cũ | Chỉnh lại |
|---|---|
| Rò rỉ subscription khi `cleanupEmptyShards` | Vẫn đúng (không `Unsubscribe`), nhưng **bị chặn bởi `MaxShards` (≤1000)** nên là lãng phí có giới hạn, không phải rò vô hạn. Hạ xuống P1 (R3). |
| `Unregister` đồng bộ chặn đường broadcast tối đa 5s | Chỉ xảy ra khi bật `Distributed`. Ở chế độ đó fan-out đi qua `routing.Gateway`, không đi qua `shard.SendTo*`. Tác động thực tế nằm ở `cleanup()` tuần tự (R9). Hạ xuống P1. |
| Endpoint stats/shard/health không có xác thực | `fiber.Server.SetupRoutes` **chỉ mount `/ws` và `/health`**, các endpoint vận hành để ứng dụng tự gắn (có ghi chú trong code). Hạ xuống P2: chỉ cần tài liệu và middleware bảo vệ tuỳ chọn (O2). |
| Một PubSub Redis cho mỗi channel | Vẫn đúng (`pubsub/redis/manager.go:157`), nhưng channel ≤ `MaxShards`+1. Gộp vào R3. |
| Lỗi mới, chưa nêu ở review trước | S1 (Gin/Echo IP spoof), S3 (room theo shard hash), S4 (cạn permit của HandlerSupervisor), S7 (renew vượt chu kỳ), S8 (readiness chập chờn). |

---

## 1. Kiến trúc hiện tại (để thống nhất thuật ngữ)

Module có **hai đường fan-out song song**, đây là gốc của nhiều vấn đề:

1. **Đường legacy:** `Manager → Shard(hash(userID)) → chatRooms/userSessions` cộng với `pubsub.Manager` (Redis/NATS Core, best-effort, channel `shard:<id>`).
2. **Đường routing (tenant-scoped, opt-in):** `core.Manager{Distributed: *routing.Gateway}` → `SessionRegistry` (Redis lease) + `DeliveryTransport` (RPC qua Redis/NATS) → `routing.Router` ← `integration/eventbus.Bridge`. Khi `Distributed != nil`, `pubsub` bị thay bằng `NoopManager` (`manager.go:91-94`).

Đường 2 được thiết kế chặt hơn hẳn (generation fence, lease, dedupe, tenant key). Đường 1 có nhiều lỗi ngữ nghĩa (S3, R3, R10). **Quyết định kiến trúc quan trọng nhất của kế hoạch này:** coi đường 1 là *chế độ single-node / dev*, và chuẩn hoá đường 2 làm đường production duy nhất cho cluster (xem câu hỏi Q1).

---

## 2. Sổ đăng ký phát hiện

Mức độ: **P0** = chặn production · **P1** = cần có trước khi chịu tải thật · **P2** = hoàn thiện.

### P0: bảo mật & đúng đắn

**S1. Gin/Echo: `ConnectionKey` mặc định giả mạo được**
- `adapter/gin/handler.go:71` dùng `c.ClientIP()`. Gin mặc định tin mọi proxy nên đọc `X-Forwarded-For`.
- `adapter/echo/handler.go:71` dùng `c.RealIP()`, mặc định đọc `X-Forwarded-For` / `X-Real-IP` khi chưa cấu hình `IPExtractor`.
- Trong khi `nethttp` dùng `httpkit.ClientIP` (peer trực tiếp), và Fiber có test chống cấu hình proxy không an toàn (`fiber/handler_test.go:102`).
- Hậu quả: kẻ tấn công đổi header là vượt giới hạn "100 conn/key" (`limiter`), và log/metadata ghi IP giả. Đây còn **mâu thuẫn trực tiếp với `SECURITY.md`**.
- *[xác minh]* bằng test gửi `X-Forwarded-For` giả.

**S2. Thông điệp từ broker không được xác thực, không giới hạn, không kiểm tra schema**
- `pubsub/redis/manager.go:241-274` và `pubsub/nats/manager.go:221-254`: không giới hạn kích thước trước khi decode, parse hai lần (`Valid` rồi `Unmarshal`), tin hoàn toàn `ShardID/UserID/RoomID`, không kiểm tra `Version`/`Type`.
- `core/manager.go:193-206`: `crossNodePayload` trả `nil` khi thiếu `Data["message"]`, handler vẫn phát frame rỗng tới client.
- `routing/rpc.go:93-135`: command giao tin nhận từ bất kỳ publisher nào có quyền trên channel; chỉ kiểm tra `TargetNode`. Ai publish được vào Redis/NATS đều có thể đẩy payload tuỳ ý tới mọi session của mọi tenant (kể cả giả mạo `result`).
- Vi phạm quy tắc "broker messages là untrusted" trong `SECURITY.md`.

**S3. Phòng (room) của đường legacy bị chia theo shard hash của user**
- `chatRooms` nằm trong `Shard` (`shard.go:57`), `JoinRoom` ghi vào shard của connection (`shard.go:244-251`), còn shard được chọn theo `hash(userID)` (`manager.go:431-434`, adapter gọi `GetShardID(userID)`).
- `SendToRoom` chỉ duyệt shard hiện tại, và pubsub `BroadcastRoomMessage(s.name, …)` chỉ publish vào channel của shard đó.
- Kết quả: hai user ở hai shard khác nhau cùng vào "room1" **không thấy tin của nhau**. Ví dụ `examples/websocket/main.go:294` dùng đúng kiểu này.
- Đường `Distributed` không bị ảnh hưởng vì khoá theo `tenant+room`.
- Đây gần như chắc chắn là lỗi chức năng. *[xác minh]* bằng test hai shard.

**S4. `HandlerSupervisor` bị cạn permit bởi handler không hợp tác**
- `core/handler_supervisor.go:73-110`: khi `ctx` hết hạn, `Run` trả về nhưng goroutine handler vẫn giữ permit cho tới khi tự kết thúc (chỉ đếm `quarantined`).
- Sức chứa mặc định là `NumCPU*4` (`config.go:79`) dùng chung toàn manager. Một số ít client gửi event kích hoạt handler chặn (DB, HTTP, lock) là đủ làm mọi event khác nhận `ErrHandlerCapacity`.
- Giới hạn `MaxPendingHandlers=64`/conn không đỡ được vì chỉ cần vài chục conn.
- Cần quota theo connection/identity trên supervisor và cơ chế phát hiện handler kẹt.

**S5. Mặc định chưa an toàn cho production**
- `normalizeManagerConfig` gán `allowAuthorizer{}` khi `Authorizer == nil` (`config.go:119`); `ValidateProduction` chỉ kiểm tra Authorizer và **không tự động chạy**.
- `pubsub/redis.NewManager()` và `NewManagerWithClient(nil)` rơi về standalone loopback với `Ready()==true` (`redis/manager.go:58-65, 85-88, 552-556`), nên có thể chạy "cluster" mà thực chất không có broker.
- Singleton toàn cục (`GetGlobalManager`, `SetAuthorizer`, `defaultEventRegistry`, `defaultObserver`, `limiter.GetGlobal*`) khuyến khích dùng sai; `pubsub.SetGlobalManager` ghi biến không có lock (`pubsub.go:114`).

**S6. Thứ tự kiểm soát khi upgrade chưa tối ưu**
- Cả 4 adapter: authenticate (thường tốn CPU/I/O) chạy **trước** rate limit; rate limit là **một bucket toàn cục** 1000 req/s (`limiter/rate_limiter.go:165`), nên một nguồn làm cạn bucket sẽ chặn mọi người dùng hợp lệ.
- Quota (`MaxConnections`, per-user/account/tenant) chỉ được kiểm tra sau khi đã `Upgrade` (`nethttp/handler.go:118` rồi `HandleIdentityConnection`), nên client nhận 101 rồi bị đóng không có mã lỗi.

**S7. Gateway `renew` có thể vượt chu kỳ làm node chập chờn "not ready"**
- `routing/gateway.go:579-612`: `renew` chạy tuần tự theo lô 256 session với `ctx` timeout bằng đúng `RenewInterval` (mặc định 10s). Nếu không hoàn tất, `leaseUntil` không được gia hạn rồi `Ready()` trả false (`gateway.go:118-120`) và `Handle` từ chối delivery.
- Với ~200k session và mỗi session một `EVAL`, cần ~800 pipeline mỗi 10s (~20k EVAL/s mỗi node, chưa tính room). Khi tải tăng hoặc Redis chậm, node rơi vào vòng lặp "không sẵn sàng → mất lease → client reconnect → tải tăng thêm".
- *[xác minh]* bằng benchmark `routing/redis/register_renew_benchmark_test.go` ở 100k/200k session.

**S8. Readiness chập chờn và tốn kém**
- `core/observability.go:167-183`: `Ready` yêu cầu `totalConnections == shardConnections`, nhưng hai bộ đếm cập nhật ở hai thời điểm khác nhau mà không có khoá chung (`shard.go:430` so với `releaseManagerConnection` ở `shard.go:440`; `manager.go:314-320`). Khi connect/disconnect dồn dập, `/health` có thể trả 503 thoáng qua và LB rút node.
- `Readiness()` gọi `GetStats()` dựng map lớn (mọi shard, histogram…) mỗi lần probe.

### P1: ổn định, hiệu năng, độ bền

**R1. Một goroutine mỗi shard xử lý toàn bộ event**
- `shard.go:455-465` → `HandleEvent` (`event.go:119-222`) làm `Valid` + `Unmarshal` + tra registry + **`Authorizer.CanPublish` đồng bộ** cho mọi conn của shard (tới 100k). Authorizer có I/O sẽ chặn cả shard (head-of-line).
- Hàng đợi `incomingMessage` (4096) dùng chung, rớt âm thầm khi đầy (`connection.go:334-338`); không có công bằng giữa các connection.
- `bytes.Clone(message)` ở `connection.go:333` thừa (gorilla/fiber trả slice mới).

**R2. Kế toán connection tuần tự hoá bởi một mutex và gọi `GetConfig()` mỗi lần**
- `manager.go:244-306`: mỗi connect lấy `accountingMutex` và gọi `GetConfig()` (copy struct lớn + `normalizeManagerConfig`) hai lần. Khi cả đàn client reconnect sau restart, đây là điểm nghẽn.

**R3. Pubsub (Redis/NATS)**
- Redis: mỗi channel một `goredis.PubSub` (một kết nối riêng) → tới ~1000 kết nối/node.
- `Subscribe`/`Unsubscribe` giữ `psm.mu` suốt thời gian gọi mạng (`redis/manager.go:148-165, 177-198`; NATS `nats/manager.go:155-186`): chặn `RegisterHandler`, `GetStats`, `processMessage` khi broker chậm.
- `cleanupEmptyShards` không `Unsubscribe` (`manager.go:608-624`), trong khi `RemoveShard` có.
- Executor nội bộ cố định 4 partition × 256 (`redis/manager.go:76,104`; NATS tương tự), không cấu hình được. Khi đầy chỉ log `ErrorAsync` cho từng message (bão log).

**R4. Race khi dọn shard rỗng *[xác minh]***
- `cleanupEmptyShards` xoá shard khỏi map rồi `Shutdown`; trong lúc đó `HandleIdentityConnection` có thể vừa lấy shard đó (`manager.go:509`) và `Register` trả "Shard stopped", connection bị từ chối oan. Cửa sổ nhỏ nhưng đúng vào lúc hàng loạt reconnect.

**R5. Gateway/RPC: các điểm nghẽn mở rộng**
- `Gateway.Handle` giữ `g.mu` khi sao chép toàn bộ thành viên phòng (`gateway.go:439-457`): O(kích thước phòng) trong mutex toàn cục, chặn `Register/Remove/JoinRoom`.
- `deliveryCache` (100k) bị một tin gửi phòng lớn đẩy sạch (mỗi session một entry), làm dedupe mất tác dụng cho retry.
- `RPCTransport.Deliver` giới hạn cứng 1024 pending mỗi node (`rpc.go:142`), trong khi `Router.send` mở tới 32 worker/lần gửi; tải cao sẽ nhận `ErrUnavailable`.
- Callback nhận message chạy tuần tự trong goroutine của subscription (Redis `sub.Channel()` có buffer hữu hạn, NATS có pending limit), một lần giao phòng lớn làm trễ mọi command sau nó.

**R6. Redis registry**
- Khoá `prefix:{sha256(tenant)}:…` nhét mọi key của một tenant vào cùng một hash slot (`registry.go:36`): tenant lớn tạo hot-slot trên Redis Cluster.
- `resolveLegacyRoomScript` decode JSON toàn bộ thành viên phòng bằng `cjson` (O(N) trong Lua) và là đường **mặc định** (`roomNodeIndex=false`).
- `register`/`renew` dùng `cjson.decode` cho từng field; hash user bị giới hạn 1000 nhưng `HGETALL` khi đầy vẫn O(1000) mỗi lần.

**R7. Chính sách với client chậm/xấu**
- `SlowConsumerDropThreshold=1` (`config.go:81`): một lần đầy queue là bị đóng. Với broadcast bursty thì quá gắt.
- Đóng connection **không gửi close frame** trừ nhánh drain; client thấy mã 1006. Tin bị rate limit/sai định dạng/đầy queue chỉ tăng counter, không báo lỗi, không ngắt client lạm dụng (`connection.go:329-338`, `event.go:126-135`).

**R8. Thiếu validate cấu hình**
- Không kiểm tra: `PingPeriod < PongWait`, `WriteWait>0`, `MaxMessageBytes <= ByteBurst` (nếu vi phạm, `AllowN` không bao giờ cho qua, `rate_limiter.go:53`), `MaxPendingHandlers` so với `MaxHandlerGoroutines`, giới hạn `MaxShardConnections ≤ MaxConnections`.
- Mặc định đáng bàn: `maxMessageSize=1024`, quota per-user/account/tenant = 0 (không giới hạn), `defaultSendQueueSize=256`.

**R9. `cleanup()` tuần tự có I/O từ xa**
- `shard.go:559-588` rồi `Unregister` rồi `removeConnection` gọi `gateway.Remove` với timeout 5s (`shard.go:435-439`). Khi Redis chậm, dọn N connection chết mất N×5s và chặn vòng quét kế tiếp.

**R10. Chất lượng pubsub legacy**
- `core/manager.go:158-191` không đăng ký handler cho `MessageTypeChat` nên `BroadcastChatMessage` luôn sinh `WarnAsync` (bão log).
- `sendNodeStatus` chỉ gọi một lần lúc khởi tạo (`manager.go:124`), không có heartbeat/node discovery.
- Chế độ loopback (client nil) không bỏ qua message cùng `NodeID` như đường Redis thật (`redis/manager.go:252` so với `287-304`) nên `BroadcastGlobal` có thể giao hai lần cho client cục bộ *[xác minh]*.
- Khoảng trống khi resubscribe (100ms–5s backoff) làm mất tin: đúng thiết kế best-effort nhưng chưa có tài liệu/số liệu.

**R11. Bridge EventBus**
- `bridge.go:172-174`: mọi kết quả khác `Enqueued` (kể cả `NotFound`/`Stale` do session vừa đóng) đều bị coi là retryable, gây retry cả message tới đến khi vào DLQ dù phần lớn đã giao. Nên phân loại: `QueueFull/Unavailable/Unknown` retry; `NotFound/Stale` chỉ retry sau khi re-resolve.
- `PublishCommand` từ chối payload rỗng (`json.Valid(nil)=false`), nên event không có `data` luôn lỗi.

**R12. Panic bị nuốt không log trong executor**
- `pool/partitioned_executor.go:99-103` chỉ đếm `panicked`; `event.go:188-193` re-panic sau khi đếm. Không có stack/ngữ cảnh ở chỗ này (các nơi khác dùng `wsrecovery`).

**R13. Payload nhị phân/không phải UTF-8**
- `shard.go:665-695`: `string(message)` đưa vào `map[string]interface{}` rồi JSON làm hỏng dữ liệu không hợp lệ UTF-8; mọi frame gửi ra luôn là `TextMessage`.

**R14. Chi phí tài nguyên mỗi connection**
- Mỗi connection: 2 pump + 1 goroutine chờ (`connection.go:255-261`) + 1 goroutine của adapter = 4 goroutine; queue gửi 256 slot; hai `RateLimiter`; map room. Ước lượng (chưa đo): ~15–20 KB/conn nên 200k conn cần vài GB. Cần số đo thật.

### P2: quan sát, vận hành, dọn dẹp

- **O1. Observability:** chỉ có counter atomic và histogram 6 bucket (`observability.go`); chưa có exporter Prometheus/OTel, thiếu gauge (độ sâu queue, số room, goroutine), thiếu metric cho `routing` (đã có `GatewayStats`/`RouterStats` nhưng chưa nối ra ngoài). Log chứa `userID/clientIP` (PII); `node_status` log nguyên `data`.
- **O2. Endpoint vận hành:** `HandleStats/HandleShardManagement/HandleHealthCheck` có ở cả 4 adapter nhưng không có bảo vệ tích hợp; `/health` trộn liveness và readiness; thiếu `Retry-After` khi từ chối.
- **O3. Mã chết:** `pool.WorkerPool`, `pool.BufferPool` và các hàm global của chúng chỉ xuất hiện trong test và định nghĩa; `ConnectionLimiter.Cleanup`/`cleanupTick` không bao giờ được gọi.
- **O4. Fiber server:** `Start()` coi là thành công sau `time.After(500ms)` (`fiber/server.go:110-115`) thay vì chờ listener thật sự sẵn sàng; `Shutdown()` không điều phối với `Manager.Shutdown` (thứ tự drain); `middleware.go` đặt CORS `AllowOrigins:"*"`.
- **O5. Xác thực dài hạn:** chỉ có `ExpiresAt` và `DisconnectIdentity`; chưa có refresh/revocation định kỳ; token qua query string lọt vào log proxy.
- **O6. Tài liệu/runbook:** thiếu sizing (`ulimit`, bộ nhớ/conn), cấu hình LB/proxy timeout, ma trận best-effort vs durable, thứ tự rollout/drain, hướng dẫn migration sang `WithRoomNodeIndex`.
- **O7. Kiểm thử:** thiếu soak, chaos broker, goroutine-leak (`goleak`), fuzz cho `crossNodePayload`/`processMessage`/`rpc.receive`.

### Điểm tốt cần giữ nguyên
- Vòng đời connection chặt: `Close` idempotent, drain FIFO, writer duy nhất, `recover` ở mọi goroutine, shutdown theo thứ tự và có deadline.
- Routing có generation fence, lease, key theo tenant, dedupe, hành vi huỷ theo context, sức chứa registry có giới hạn.
- Bộ test đã khá rộng (race, shutdown, room I/O, fuzz, benchmark + tài liệu `websocket/BENCHMARKS.md`).

---

## 3. Lộ trình thực hiện

Nguyên tắc: mỗi PR nhỏ, có test đỏ trước rồi sửa, chạy `go test ./websocket/... && go vet ./...` và race check theo `AGENTS.md`. Sau mỗi PR đổi source: build Graft theo hướng dẫn trong `AGENTS.md`.

### Phase 0: lưới an toàn (2–3 ngày)
| # | Việc | Kết quả |
|---|---|---|
| 0.1 | Chạy baseline: `go test ./...`, `go vet ./...`, race check `-count=10` cho `core/pool/pubsub/adapter`, và tích hợp Redis/NATS (xem lệnh trong `AGENTS.md`) | Biên bản baseline, danh sách test flaky/skip |
| 0.2 | Viết **test tái hiện** (đỏ) cho S1, S3, S4, R4, S8, R10-loopback | Mỗi phát hiện *[xác minh]* có test chứng minh |
| 0.3 | Đo baseline: benchmark 100k/200k session cho `routing/redis` (renew), connect-storm cho `core` | Số liệu để so sánh sau tối ưu (S7, R2, R5) |
| 0.4 | Chốt câu hỏi Q1–Q4 (mục 6) | Quyết định kiến trúc |

**Step 5 — baseline (2026-11-24):**

- **R4:** chặn `SessionRegistry.Register` sau khi `HandleIdentityConnection` đã chọn shard, chạy dọn shard rồi cho đăng ký tiếp. Kết quả 10/10 lần: shard đã dừng, admission trả `failed to register connection`. Đây là lỗi thứ tự xử lý, không phải báo cáo data race từ Go.
- **R10-loopback:** một `BroadcastGlobal` gửi **2 bản** cho cùng connection ở cả Redis và NATS `AllowStandalone`, 10/10 lần mỗi backend (đợi executor xử lý xong). Bản đầu gửi trực tiếp, bản sau đến từ handler loopback.
- Hai test đỏ chạy riêng bằng `go test -race -tags=ws_repro -run '^(TestShardCleanupDoesNotRejectInFlightAdmission|TestBroadcastGlobalDeliversOnceInStandalone)$' -count=10 ./websocket/core`. *(Đã sửa ở Step 6: build tag `ws_repro` đã bỏ, hai test chạy trong CI mặc định.)*
- **Windows / Go 1.27.3:** `go test -count=1 ./websocket/...`, `go vet ./...`, `go vet -tags=ws_repro ./websocket/core` và race `-count=10` theo `.github/workflows/websocket.yml` đều đạt. `go test ./...` chưa xanh: `internal/perftest.TestChildBrokerDrainBarrier` ngoài phạm vi WebSocket lỗi 10/10 khi chạy riêng (`callback=true`, `BacklogMeasured=true`, `BrokerDrain=0`).
- **WSL / Redis 7.0.15 + NATS 2.15.0 chạy tạm:** hai test pub/sub ban đầu gửi `notification` thiếu `ShardID`, nên bộ kiểm tra schema từ chối. Sau khi sửa dữ liệu test và kiểm tra cả payload nhận được, mỗi backend đạt 10/10 với `-race -tags=integration`. Redis routing contract, Redis Streams/NATS JetStream bridge và cả 4 cấu hình Redis/NATS của cluster example đều đạt với `-race -tags=integration`. Không chạy các case Kafka, benchmark tải hoặc soak.
- **Graft/Trail:** guarded `thothan trail build` thành công sau khi thêm các test tái hiện.

### Phase 1: bảo mật & đúng đắn (P0), ~1.5–2 tuần
| ID | Thay đổi | File chính | Kiểm chứng |
|---|---|---|---|
| S1 | Tạo `wshttp.ClientIP`-style helper dùng chung; Gin/Echo mặc định dùng peer trực tiếp (như `httpkit.ClientIP`), yêu cầu cấu hình trusted proxy tường minh (Gin `SetTrustedProxies`, Echo `IPExtractor`), thêm test XFF giả cho cả bốn adapter | `adapter/gin|echo/handler.go`, `adapter/internal/wshttp` | Test spoof đỏ→xanh |
| S2a | Chuẩn hoá `CrossNodeMessage` nhận vào: giới hạn byte trước decode, **một** lần `Unmarshal`, kiểm `Version`/`Type`/độ dài `ShardID/UserID/RoomID`, bỏ message thiếu `Data.message`; thêm cấu hình `MaxMessageBytes` cho pubsub | `pubsub/{redis,nats}/manager.go`, `core/manager.go:193` | Fuzz + test message hỏng/quá lớn |
| S2b | Xác thực nguồn: ký HMAC-SHA256 (khoá cấp qua cấu hình/biến môi trường, hỗ trợ xoay khoá `kid`) cho `CrossNodeMessage` và `rpcEnvelope`; từ chối khi chữ ký sai. Song song vẫn khuyến nghị ACL broker theo node | `pubsub/pubsub.go`, `routing/rpc.go` | Test giả mạo command/result bị từ chối |
| S3 ✅ | Theo Q1: (A) **khuyến nghị**: đánh dấu phòng legacy là *chỉ cùng shard*, deprecate `Shard.JoinRoom/SendToRoom` cho cluster, chuyển tài liệu và ví dụ sang `Gateway`+`Router`; hoặc (B) sửa thật: chọn shard cho phòng theo `hash(tenant,room)` độc lập với shard user | `core/shard.go`, `manager.go`, `examples/websocket` | Test hai-user-hai-shard |
| S4 | `HandlerSupervisor`: thêm quota theo connection/identity và theo `eventType`; metric `quarantined`; khi `quarantined` vượt ngưỡng thì đóng/hạn chế connection vi phạm; tài liệu rõ "handler phải tôn trọng `ctx`" | `core/handler_supervisor.go`, `event.go` | Test handler chặn vô hạn không làm cạn handler khác |
| S5 ✅ | `ManagerConfig.Validate()` mới (R8) và `ValidateProduction()` mở rộng; thêm hàm tạo `core.NewProductionManager(cfg)` gọi bắt buộc; Redis/NATS pubsub mặc định `StayUnready` (không im lặng standalone); khoá/đánh dấu `Deprecated` cho singleton; `SetGlobalManager` dùng `sync` | `core/config.go`, `pubsub/*` | Test: thiếu authorizer/broker → lỗi khởi tạo |
| S6 ✅ | Thứ tự mới trong adapter: (1) `IsAccepting`, (2) giới hạn theo IP, (3) pre-check `CanAcceptConnection`/quota từ identity chưa-đủ → **503 + `Retry-After`**, (4) authenticate, (5) quota theo identity, (6) Upgrade. Rate limit theo IP (token bucket theo key, có TTL dọn) thay bucket toàn cục. Gom logic vào một helper dùng chung để 4 adapter không lệch nhau | `adapter/*`, `limiter/` | Test thứ tự + test không lộ chi tiết lỗi |
| S7 🟡 | Gateway renew: chạy theo lô **song song có giới hạn**, deadline độc lập chu kỳ (`LeaseTTL`-based), jitter, chỉ đánh dấu `Ready=false` khi *lease thực sự hết hạn* (không phải khi một vòng chậm); cân nhắc lease mức node (một key heartbeat/node) cộng lease session lười | `routing/gateway.go`, `routing/redis/registry.go` | Benchmark 200k session: renew < 50% `RenewInterval` |
| S8 🟡 | `Readiness()` nhẹ: không gọi `GetStats()`; chỉ đọc atomic; bỏ so sánh `total==shard` khỏi readiness (chuyển thành metric/cảnh báo, hoặc dùng accounting thống nhất dưới một nguồn sự thật). Tách `livez`/`readyz` | `core/observability.go`, adapters | Test connect-storm không làm readiness chập chờn |

> **S6, trạng thái ✅ (đã review).** Đã làm: `wshttp.Gate` (lifecycle, sức chứa node, rate limit theo IP) chạy trước `authenticate` ở cả nethttp, Fiber, Gin, Echo; từ chối trả 503/429 kèm `Retry-After`; `Config.RateLimiter` đổi thành `IPRateLimiter *limiter.KeyedRateLimiter` (breaking, theo Q2); xoá `limiter.GetGlobalRateLimiter`; cập nhật ví dụ và README. Khoá giới hạn là IP của peer trực tiếp, `X-Forwarded-For` không tránh được (có test cho cả 4 adapter). Test chứng minh authenticator không bị gọi khi request bị chặn.
> **Chưa làm trong S6:** quota theo identity (user/account/tenant) vẫn kiểm tra sau `Upgrade`, vì cần identity đã xác thực. Pre-check mức node (`CanAcceptConnection`) đã có. Phần còn lại gắn với Step 2.

> **S5, trạng thái ✅ (đã review).** Đã làm:
> - `ManagerConfig.Validate()` (phần R8, từ phiên trước) và `ValidateProduction()` mở rộng: từ chối `PubSub` thật khi không có `Distributed` (theo Q1), từ chối `PubSub` đặt cạnh `Distributed`, và **từ chối cluster `Distributed` mà transport chưa ở chế độ `pubsub.AuthRequired`** (`Gateway.RequiresAuthentication`). `AuthPassive` và `AuthOptional` đều bị từ chối vì vẫn nhận tin giả.
> - `ManagerConfig.AllowUnauthenticatedBroker` (mặc định false): lối thoát tường minh khi broker đã là ranh giới tin cậy bằng ACL cho từng node cộng TLS/mTLS (Q3 lớp 1).
> - `core.NewProductionManager(cfg)`: validate rồi mới khởi tạo. Ví dụ `v2` và `cluster` đã chuyển sang dùng nó.
> - Pubsub Redis/NATS: `NewManager` và `NewManagerWithClient(nil)` giờ mặc định `StayUnready` (không im lặng chạy standalone). Standalone chỉ bật khi gọi `NewManagerWithClientAndPolicy(nil, pubsub.AllowStandalone)`. `NewManagerWithClient` nhận thêm `...Option`.
> - Singleton pubsub (`pubsub.Set/GetGlobalManager`, `redispubsub`/`natspubsub` `GetGlobalManager` và `NewManager`) ban đầu đánh dấu `Deprecated`, **sau đó đã xoá hẳn (2026-11-26)**.
> - Ví dụ `v2` yêu cầu `WS_V2_BROKER_KEY`, ví dụ `cluster` yêu cầu `WS_EXAMPLE_BROKER_KEY` (≥ 32 byte). Đã cập nhật README và `SECURITY.md`.
> - Test mới: bảng 5 trường hợp xác thực broker cho `ValidateProduction`, `NewProductionManager` từ chối/chấp nhận, pubsub fail-closed khi không có broker, ví dụ `v2` yêu cầu khoá mạnh.
>
> **Phần còn lại của S5 (đã làm 2026-11-26):** `core.GetGlobalManager`, `core.SetAuthorizer`, `limiter.GetGlobalConnectionLimiter` đã xoá hẳn (không chỉ deprecate); `pool.GetGlobal*` đã mất cùng O3. Test `TestSecurityRejectsUnauthorizedRoomAndEvent` nay gắn authorizer qua coordinator giả. `go build`, `go vet`, `go test -race -count=1` cho `websocket/...` và `examples/websocket/...` xanh.

> **S3, trạng thái ✅ (đã review).** Chọn phương án (A) theo Q1: không sửa hash phòng. Đã làm:
> - **Test mô tả hành vi** (`websocket/core/room_scope_test.go`) chứng minh nhận định của plan là đúng: hai user ở hai shard khác nhau cùng vào một phòng thì `Shard.SendToRoom` không tới được nhau, `GetRoomSize` chỉ đếm theo shard, và tin phòng từ node khác được định tuyến theo **tên shard của người gửi** nên chỉ tới thành viên ở shard trùng tên. Đây là test "đóng đinh" hành vi hiện tại, không phải test sửa lỗi: nếu sau này đổi thiết kế thì phải cố ý sửa test.
> - **`Deprecated`** (kèm lý do và hướng thay thế `routing.Router`): `Shard.SendToRoom`, `Shard.BroadcastRoomMessage`, `Shard.BroadcastUserNotification`. Doc của `JoinRoom` và `GetRoomSize` ghi rõ phạm vi theo shard; `JoinRoom` được giữ vì nó cũng ghi phiên vào registry mà `Router` đọc.
> - **Chặn cấu hình** đã có từ Step 2 (`ValidateProduction` từ chối `PubSub` thật khi không có `Distributed`).
> - **Tài liệu:** thêm mục "Legacy shard rooms are single-node only" vào `websocket/README.md`; ví dụ `examples/websocket` (README và chú thích trong `main.go`) nêu rõ giới hạn và trỏ sang `v2`/`cluster`.
>
> **Phát hiện khi làm S3:** ví dụ `examples/websocket` (đường legacy) có handler `chat.send` thực chất **không chuyển tin giữa hai user khác nhau**, kể cả trên một node, vì adapter chọn shard theo `hash(user_id)` với tối đa 64 shard. Tôi chỉ ghi tài liệu, chưa đổi hành vi ví dụ.
> **Chưa làm trong S3:** chưa xoá hẳn API phòng legacy (còn dùng trong ví dụ và test); chưa thêm `core.Manager.SendToRoom` tổng hợp theo mọi shard (không cần theo Q1).

> **S7, trạng thái 🟡 (xong code, chờ review).** Theo mục 6b chỉ sửa lỗi `Ready` chập chờn, không tối ưu quy mô lớn.
> - **Nguyên nhân gốc (đã chứng minh bằng test đỏ trước khi sửa):** `renew()` giới hạn mỗi vòng bằng đúng `RenewInterval` (`context.WithTimeout(g.ctx, RenewInterval)`). Một vòng chậm hơn lịch nhưng vẫn thành công bị huỷ, `leaseUntil` không được gia hạn, và `Ready()` rơi về false khi lease cũ hết hạn, dù registry chỉ chậm chứ không hỏng.
> - **Sửa:** ngân sách một vòng là `LeaseTTL - RenewInterval` (`Gateway.renewBudget`). Một vòng bắt đầu ở T và thành công thì lease mới là T+LeaseTTL, nên vòng chạy tới `LeaseTTL - RenewInterval` vẫn còn đúng một chu kỳ trước khi lease cũ hết. Mặc định (TTL 30s, interval 10s) đổi ngân sách từ 10s lên 20s.
> - **Test mới** (`routing/gateway_renew_budget_test.go`): vòng chậm 2.5x chu kỳ vẫn gia hạn readiness; registry không bao giờ trả lời thì vòng bị chặn đúng ngân sách và `Ready` giảm về false; `cancel` gateway huỷ vòng đang chạy dưới 250ms (đảm bảo ngân sách dài hơn không làm chậm `Close`).
> - **Chưa làm trong S7 (đã hoãn theo 6b):** renew song song theo lô, lease mức node, jitter. Benchmark 100k/200k session chưa chạy, nên chưa có số đo cải thiện ở quy mô lớn.

> **S8, trạng thái 🟡 (xong code, chờ review).** Phần readiness nhẹ và có thời gian ân hạn đã làm ở phiên trước. Phần còn lại làm ở bước này:
> - **Tách liveness và readiness** ở cả 4 adapter: `HandleLive` (luôn 200 khi tiến trình trả lời được, không đọc manager hay broker) và `HandleReady` (503 khi đang drain hoặc executor/broker không sẵn sàng). Logic dùng chung ở `wshttp.LiveStatus/ReadyStatus`. `HandleHealthCheck` ban đầu giữ làm bí danh của `HandleReady`, **sau đó đã xoá (2026-11-26)**; route `/health` trỏ thẳng vào `HandleReady`.
> - **Lý do tách:** nếu một endpoint dùng cho cả hai, khi Redis/NATS sập thì mọi node đều fail probe và orchestrator khởi động lại các tiến trình khỏe, đứt toàn bộ WebSocket, thay vì chỉ rút khỏi load balancer. Node đang drain cũng phải còn "sống" để drain xong.
> - `fiber.Server.SetupRoutes` mount thêm `/livez` và `/readyz`; `/health` giữ làm bí danh của `/readyz`. Ví dụ `examples/websocket` (4 stack) và `cluster` cũng mount cả ba. README ví dụ giải thích cách chọn probe.
> - **Test mới:** `wshttp` (liveness vô điều kiện, readiness theo manager, node đã dừng thì not-ready nhưng vẫn live), và mỗi adapter có test probe, trong đó có kiểm tra bí danh `/health` giữ nghĩa readiness.
> - **Chưa làm trong S8:** chưa có test connect-storm chứng minh readiness không chập chờn (test phía thời gian ân hạn của phiên trước chỉ là test đơn vị với bộ đếm giả). `/readyz` vẫn lộ chi tiết `checks` (executor, broker) cho bất kỳ ai gọi được, bảo vệ nó thuộc mục O2 ở Phase 3.

**Tiêu chí hoàn thành Phase 1:** toàn bộ test đỏ của Phase 0 chuyển xanh; `go vet` sạch; race `-count=10` sạch; tài liệu `SECURITY`/README cập nhật.

### Phase 2: ổn định & hiệu năng (P1), ~2–3 tuần
| ID | Thay đổi | Ghi chú |
|---|---|---|
| R1 | Parse **một lần** (`Unmarshal` trực tiếp, bỏ `Valid`) ngay trong `readPump` (song song theo connection) thay vì trong vòng shard; chuyển `CanPublish` vào worker (có timeout); hàng đợi công bằng theo connection; bỏ `bytes.Clone` thừa; phản hồi lỗi/đóng khi vi phạm lặp lại (R7) | Cần đo trước/sau bằng `core/benchmark_test.go` |
| R2 | Cache `ManagerConfig` đã normalize (bất biến sau khởi tạo); tổng conn dùng atomic; quota map chia shard theo hash khoá; kiểm tra trần bằng CAS | Benchmark connect-storm |
| R3 | Redis: dùng **một** `PubSub` nhiều channel (hoặc ít channel cố định theo `hash(shard)%N`); `Subscribe/Unsubscribe` chuẩn bị ngoài khoá, commit trong khoá; `cleanupEmptyShards` gọi `Unsubscribe`; executor có thể cấu hình; giới hạn tần suất log khi từ chối | Test + `integration` Redis/NATS |
| R4 | Shard có trạng thái `retiring` dưới `shardMutex` + đếm tham chiếu; `HandleIdentityConnection` retry một lần nếu shard vừa bị dọn | Test race |
| R5 | `Gateway.Handle`: dùng `RWMutex`/copy-on-write cho `rooms`; phân mảnh `g.mu`; dedupe key theo `message` + bitmap/Bloom cho phòng lớn hoặc cache riêng cho room fan-out; `RPCTransport.pending` cấu hình được + bounded worker xử lý callback | Benchmark `gateway_benchmark_test.go` |
| R6 | Cho phép **bucket hoá khoá** theo tenant (`{tenant:bucket}`) để tránh hot-slot; chuyển mặc định sang `WithRoomNodeIndex` kèm hướng dẫn migration đã có trong code; giảm `cjson` ở đường nóng | Benchmark `routing/redis` |
| R7 | `SlowConsumerDropThreshold` mặc định > 1 và có cửa sổ thời gian; gửi **close frame** có mã (1008/1013/1001) trên mọi đường đóng; error envelope chuẩn cho rate-limit/malformed; ngắt sau N vi phạm | Test tích hợp client giả |
| R9 | `cleanup()` song song có budget chung (như `shutdown` đã làm); `gateway.Remove` ra hàng đợi nền có giới hạn thay vì inline | Test Redis chậm |
| R10 | Đăng ký handler `Chat`; heartbeat node định kỳ hoặc bỏ `sendNodeStatus`; loopback bỏ qua self (hoặc tài liệu hoá); tài liệu best-effort + số đo khoảng mất tin khi reconnect | |
| R11 | Phân loại retry theo `Status`; cho phép payload rỗng bằng `null`/`{}` | `integration/eventbus` |
| R12 | Log panic của executor bằng `wsrecovery`; giữ counter | `pool/partitioned_executor.go` |
| R13 | Dùng `json.RawMessage`/base64 cho payload legacy; hỗ trợ `BinaryMessage` có chủ đích | Breaking nhẹ, cần Q2 |
| R14 | Đo bộ nhớ/goroutine per conn; gộp goroutine chờ vào pump; cân nhắc `SendQueueSize` nhỏ hơn cùng mở rộng động; không bật nén theo mặc định | Pprof |
| O3 | Xoá hoặc `internal` hoá `WorkerPool/BufferPool`; xoá `ConnectionLimiter.Cleanup` không dùng | Giảm bề mặt API (breaking, cần Q2) |

### Phase 3: quan sát & vận hành (P2), ~1 tuần
- **O1:** gói `websocket/metrics` (hoặc adapter `Observer`) xuất Prometheus/OTel: gauge `connections`, `rooms`, độ sâu queue theo phân vị, `slow_consumers_total`, `drops_total{reason}`, `handler_quarantined`, `pubsub_reconnects_total`, `gateway_lease_failures_total`, `router_unknown_total`, histogram mở rộng. Log: sampling + băm/che `userID/clientIP`; bỏ log `node_status` nguyên văn.
- **O2:** middleware bảo vệ endpoint vận hành dùng chung; `/livez` tối giản, `/readyz` có chi tiết chỉ trả cho mạng nội bộ.
- **O4:** `Start()` chờ listener thật sự; helper tích hợp `foundation.Component` (Start không chặn, `Run` chặn, shutdown theo thứ tự: ngừng nhận → drain manager → đóng gateway/transport → đóng server); bỏ CORS `*` mặc định.
- **O5:** ticket ngắn hạn/subprotocol thay token trong query; hook revoke định kỳ qua `DisconnectIdentity`.
- **O6:** viết `websocket/docs/PRODUCTION_RUNBOOK.md`: sizing, `ulimit`, timeout LB, rollout/drain, ma trận durability, sự cố thường gặp.

### Phase 4: chứng minh sẵn sàng production, ~1–2 tuần
- **Soak & tải (Linux, TLS):** ≥ mục tiêu conn/node (Q4) trong ≥ 24h; connect-storm 100% reconnect trong 60s; slow-client 5–10% client chậm; broker outage (Redis/NATS dừng 30s, 5 phút, failover); node kill -9.
- **Chaos & rò rỉ:** `goleak` ở `core`, `pubsub`, `routing`; fuzz `processMessage`, `rpc.receive`, `crossNodePayload`.
- **Bảo mật:** kiểm thử spoof header, giả mạo broker, quota bypass, origin, token hết hạn khi đang kết nối.
- **Cập nhật `docs/PRODUCTION.md`:** nâng trạng thái `websocket` từ *Integration candidate* lên *Production* kèm số đo, giới hạn đã biết, SLO.

---

## 4. Tiêu chí chấp nhận sản xuất (Definition of Done)

1. Toàn bộ P0 đóng, mỗi mục có test hồi quy; không còn mục *[xác minh]* chưa kết luận.
2. `go test ./...` và `go vet ./...` xanh; race `-count=10` xanh; test tích hợp Redis/NATS xanh trong CI có broker.
3. Soak 24h: không rò goroutine/bộ nhớ (tăng < 5%), p99 độ trễ event và fan-out trong SLO đã chọn, không có readiness chập chờn, `slow_consumers` và `drops` nằm trong ngưỡng.
4. Broker outage: node tự khôi phục subscription/lease không cần restart; số tin mất đo được và có tài liệu.
5. Mặc định an toàn: không thể khởi tạo "production manager" thiếu authorizer, thiếu broker hợp lệ, hoặc dùng IP giả mạo được.
6. Dashboard + cảnh báo + runbook có sẵn; `docs/PRODUCTION.md` cập nhật.

---

## 5. Rủi ro & chiến lược tương thích

- **Breaking change:** đổi mặc định ConnectionKey (S1), pubsub mặc định `StayUnready` (S5), xoá/ẩn pool (O3), thay encoding payload (R13). Đề xuất: giữ cờ `Legacy…` một phiên bản, ghi `Deprecated` + `CHANGELOG`, ghi rõ trong `docs/V2.md`.
- **Hiệu năng vs an toàn:** HMAC (S2b) thêm CPU mỗi message: đo trước, ưu tiên ký phần header + hash payload.
- **Rollout cluster:** các thay đổi wire (HMAC, `CrossNodeMessage`) cần giai đoạn "chấp nhận cả hai" (verify-optional → verify-required) để rolling update không rớt tin.
- **Phạm vi lớn:** Phase 2 chạm vào lõi `core`; ưu tiên PR nhỏ + benchmark so sánh để tránh hồi quy hiệu năng.
- **Giới hạn của đánh giá:** chưa chạy test/benchmark/race, ước lượng bộ nhớ và tải Redis ở trên là suy luận từ code.

---

## 6. Quyết định đã chốt (2026-11-07)

Mục tiêu sản phẩm: **chat realtime ~1.000 user đồng thời; game online dạng lobby / turn-based**. RPG/MMO tính sau.

### Q1: ĐÃ CHỐT
Đường legacy (`Shard`+`pubsub`) chỉ dùng cho **single-node / dev**. Cluster **bắt buộc** đi `routing.Gateway` + `routing.Router`.
Hệ quả: `Shard.JoinRoom/SendToRoom` cho cluster bị deprecate; `ValidateProduction` từ chối cấu hình có `PubSub` thật mà không có `Distributed`; S3 xử lý bằng cách tài liệu hoá + chặn cấu hình, không sửa hash phòng.

### Q2: ĐÃ CHỐT
Cho phép breaking change (branch chưa public). Không giữ cờ `Legacy…`; xoá thẳng API/mặc định không an toàn (xoá `pool.WorkerPool/BufferPool`, đổi mặc định ConnectionKey, pubsub mặc định `StayUnready`).

### Q3: KHUYẾN NGHỊ (áp dụng)
**Lớp 1 (bắt buộc, hạ tầng): ACL + TLS ở broker.** Redis ACL user riêng cho websocket, chỉ cấp quyền trên prefix `websocket*`/channel của module; NATS user + permissions publish/subscribe theo subject; bật TLS, không tắt verify. Đây là rào chắn chính vì với 1k user bạn kiểm soát được toàn bộ node.
**Lớp 2 (nhẹ, trong code): HMAC-SHA256 tuỳ chọn** cho `CrossNodeMessage` và `rpcEnvelope`, khoá lấy từ cấu hình/biến môi trường, có `kid` để xoay khoá; chế độ `Optional` → `Required`. Mặc định `Required` khi `ValidateProduction` chạy trên cấu hình cluster.
**Luôn bật (không phụ thuộc khoá):** giới hạn kích thước trước decode, kiểm `Version/Type`, độ dài các ID, bỏ message thiếu payload.
Lý do: HMAC đắt hơn đáng kể so với lợi ích nếu ACL đã chặt; nhưng nó rẻ ở quy mô 1k user và chặn được trường hợp broker dùng chung nhiều dịch vụ.

### Q4: KHUYẾN NGHỊ (áp dụng): quy mô & SLO

| Hạng mục | Mục tiêu thiết kế | Ghi chú |
|---|---|---|
| Kết nối đồng thời | **1.000 thường xuyên; kiểm chứng tải tới 5.000/node** (hệ số an toàn 5×) | 1 node đủ; chạy **2 node** để có HA + rolling deploy |
| Topology | 2 node ứng dụng + 1 Redis (có replica) hoặc 1 NATS; LB có sticky không bắt buộc | Redis đơn là đủ, không cần Cluster ở quy mô này |
| Tin chat | tối đa 1–2 KB/tin; ≤ 5 tin/s/user; phòng ≤ 200 thành viên | `MaxMessageBytes` 4 KB, `MessagesPerSecond` 10, burst 20 |
| Game lobby/turn-based | phòng ≤ 8–16 người; ≤ 2 hành động/s/người; trạng thái do server quyết định | Không cần tick-rate; dùng đường `Router.SendToRoom` |
| p99 fan-out nội node | **< 50 ms** | đo trong test tải |
| p99 fan-out liên node (qua Redis/NATS) | **< 150 ms** | |
| Mất tin | chat/lobby: **best-effort có đo đếm**, mất khi broker gián đoạn là chấp nhận được; tin quan trọng (kết quả ván, thanh toán, thông báo bắt buộc) đi **EventBus** (durable) | cần client resume bằng `last_seq` cho phòng chat nếu muốn không mất tin |
| Reconnect storm | 100% client reconnect trong 60 s sau khi 1 node chết, không làm sập node còn lại | |
| Khả dụng | Rolling deploy không làm client thấy lỗi quá 1 lần reconnect | drain + close frame 1001 |
| Bộ nhớ/conn | ghi nhận số đo thực; mục tiêu < 32 KB/conn | |

**Hệ quả cho kế hoạch:** các mục tối ưu cho 100k–200k conn (S7 ở quy mô lớn, R2 shard hoá accounting, R5 copy-on-write, R6 bucket hoá khoá, R14) **hạ xuống "để sau"**, chỉ làm phần đúng đắn/bảo mật + phần rẻ. Ưu tiên: S1, S2, S3(chặn), S4, S5, S6, S8, R3 (bản rẻ), R4, R7, R8, R10, R11, R12, O1–O6 cốt lõi.

### Q5: KHUYẾN NGHỊ (áp dụng)
- **Frame nhị phân: KHÔNG hỗ trợ ở giai đoạn này.** Chat và game lobby/turn-based dùng JSON text là đủ, dễ debug, dễ kiểm tra giới hạn. Sửa R13 theo hướng đúng-đắn: payload phải là JSON hợp lệ/UTF-8, từ chối phần còn lại (thay vì làm hỏng âm thầm). Khi cần binary (RPG, voice) sẽ thiết kế giao thức riêng (msgpack/protobuf, subprotocol).
- **Nén permessage-deflate: KHÔNG bật mặc định.** Tin chat nhỏ, nén tốn CPU và bộ nhớ mỗi connection, và làm tăng rủi ro "compression bomb". Chỉ cân nhắc sau khi đo băng thông thực tế; nếu bật phải kèm giới hạn kích thước sau giải nén.
- **Tín hiệu phụ trợ nên có:** envelope có `seq` theo phòng (để client phát hiện mất tin/sắp xếp) và `ping` ứng dụng nếu cần đo RTT.

---

## 6b. Phạm vi triển khai thực tế theo quyết định trên

| Phase | Làm ngay | Hoãn |
|---|---|---|
| 0 | Baseline (xanh), test tái hiện S1, S3, S4, R4, S8 | benchmark 200k session |
| 1 | S1, S2a, S2b (HMAC tuỳ chọn), S3 (chặn cấu hình), S4, S5, S6, S8 | S7 tối ưu quy mô lớn (chỉ sửa lỗi `Ready` chập chờn) |
| 2 | R3 (unsubscribe + giữ khoá ngắn), R4, R7, R8, R10, R11, R12, R13 (từ chối non-UTF8), O3 | R1 đầy đủ, R2, R5, R6, R14 |
| 3 | O1 (metrics), O2, O4, O6 | O5 |
| 4 | Soak 1k–5k conn, chaos broker, goleak | tải 100k+ |

---

## 7. Ước lượng tổng

| Phase | Thời lượng | Phụ thuộc |
|---|---|---|
| 0 | 2–3 ngày | — |
| 1 | 1.5–2 tuần (rút gọn theo mục 6b) | Q1, Q3 đã chốt |
| 2 | 2–3 tuần (rút gọn theo mục 6b) | Phase 1, Q2, Q4 đã chốt |
| 3 | ~1 tuần | song song được với cuối Phase 2 |
| 4 | 1–2 tuần | môi trường Linux/TLS + Redis/NATS |

Tổng cộng khoảng 6–8 tuần cho một người, có thể rút ngắn nếu chia Phase 2 theo tầng (core / pubsub / routing).
