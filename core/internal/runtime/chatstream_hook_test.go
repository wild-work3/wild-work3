// 拦截器链的进程级回归：验的是**调用时序**，不是算法。
//
// 为什么值得用真进程测：W3-25/§2-E 的红线是「SSE 逐事件透传，不得为逆序处理而缓存整段响应」，
// 而旧实现把 RunHooks 放在渠道调用**之后**——链内部的流水线写得再对，整体仍退化成「先收集再回放」。
// 后果实测（temp/test_hook_long.py）：401 帧的响应首字延迟 1286ms、超过 queue 容量 256 帧后
// 反压一路堵回上游，客户端 45s 收 0 帧，直到 ChatTimeout=300s。这类缺陷只有「真起进程 + 真流式」才暴露。
//
// 手法：测试二进制**自扮演插件**。Manager 把 os.Args[0] 当插件 exe 拉起（`call --data <dir>`），
// TestMain 见第一个位置参数是 "call" 就切进 fakePluginMain；角色靠 --data 末段目录名区分
// （PluginDataDir = <core data>/plugins/<id>）；场景参数走环境变量（plugin.Start 用 os.Environ()
// 追加，父进程 t.Setenv 的值天然被子进程继承）。不依赖 dist 产物、不碰网络。
package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"wild-work3/main/protocol/chat"
	"wild-work3/main/protocol/jsonl"
	"wild-work3/main/protocol/meta"
	"wild-work3/main/protocol/plugin"
	"wild-work3/main/protocol/rpc"
)

const (
	fakeChan = "fakechan" // 假渠道插件：按 env 吐 N 帧 text.delta
	fakeHook = "fakehook" // 假拦截器：原样转发，可按 env 在第 K 帧后带错退出
)

func TestMain(m *testing.M) {
	// `go test` 永远不会把 "call" 当第一个参数，故不会误入插件分支。
	if len(os.Args) > 1 && os.Args[1] == plugin.ModeCall {
		os.Exit(fakePluginMain())
	}
	os.Exit(m.Run())
}

// ---------- 假插件（子进程侧） ----------

func fakePluginMain() int {
	dec, enc := jsonl.NewDecoder(os.Stdin), jsonl.NewEncoder(os.Stdout)
	var m jsonl.Msg
	if err := dec.Decode(&m); err != nil {
		fmt.Fprintf(os.Stderr, "[fake] 读 invoke 失败: %v\n", err)
		return 1
	}
	switch m.Method {
	case "chat.generate":
		return fakeChannelStream(m.ID, enc, m.Params)
	case "hook.stream":
		return fakeHookStream(m.ID, dec, enc)
	case "request.prepare":
		return fakePrepare(m.ID, enc, m.Params)
	}
	return fakeResult(enc, m.ID, jsonl.Errorf("not_found", "channel", "假插件不支持 %s", m.Method))
}

// fakePrepare 模拟声明 request.prepare 的拦截器。
//   - WW3_FAKE_PREPARE_REJECT=1 → 返回错误（拒绝请求）
//   - 否则：解析入站 request，往 messages 前插一条 system（"注入的管理区"），返回改写后的完整 request。
func fakePrepare(id string, enc *jsonl.Encoder, params jsonl.RawMessage) int {
	if os.Getenv("WW3_FAKE_PREPARE_REJECT") == "1" {
		return fakeResult(enc, id, jsonl.Errorf("prompt_too_long", "request", "上下文超限（假 prepare 拒绝）"))
	}
	var p struct {
		Request json.RawMessage `json:"request"`
	}
	if err := json.Unmarshal(params, &p); err != nil || len(p.Request) == 0 {
		return fakeResult(enc, id, jsonl.Errorf("server", "request", "prepare 解析 params 失败"))
	}
	var req chat.GenerateRequest
	if err := json.Unmarshal(p.Request, &req); err != nil {
		return fakeResult(enc, id, jsonl.Errorf("server", "request", "prepare 解析 request 失败: %v", err))
	}
	// 注入一条 system 到最前
	req.Messages = append([]chat.ChatMessage{{Role: "system", Content: "注入的管理区"}}, req.Messages...)
	b, _ := json.Marshal(map[string]any{"request": req})
	return fakeResult(enc, id, nil, b)
}

func boolPtr(b bool) *bool { return &b }

// fakeChannelStream 吐 WW3_FAKE_N 帧 text.delta（每帧间隔 WW3_FAKE_GAP_MS）后正常收尾。
// 若设置了 WW3_FAKE_EXPECT_SYSTEM=1，则校验入站 messages 首条是否是 system（验证 prepare 注入）。
func fakeChannelStream(id string, enc *jsonl.Encoder, params jsonl.RawMessage) int {
	// 校验 prepare 注入：入站 messages 首条应是 system
	if os.Getenv("WW3_FAKE_EXPECT_SYSTEM") == "1" {
		var p struct {
			Messages []chat.ChatMessage `json:"messages"`
		}
		if err := json.Unmarshal(params, &p); err != nil || len(p.Messages) == 0 || p.Messages[0].Role != "system" {
			return fakeResult(enc, id, jsonl.Errorf("server", "channel", "prepare 注入校验失败：首条消息不是 system"))
		}
	}
	n, gap := fakeEnvInt("WW3_FAKE_N", 8), fakeEnvInt("WW3_FAKE_GAP_MS", 0)
	for i := 0; i < n; i++ {
		data, _ := json.Marshal(map[string]any{"text": fmt.Sprintf("p%d", i)})
		if err := enc.Encode(&jsonl.Msg{V: 1, ID: id, Op: jsonl.OpEvent, Event: "text.delta", Data: data}); err != nil {
			return 0 // 调用方已关管道（拦截器命中后 core 提前终止上游）：静默退出即可
		}
		if gap > 0 {
			time.Sleep(time.Duration(gap) * time.Millisecond)
		}
	}
	return fakeResult(enc, id, nil)
}

// fakeHookStream 原样转发每一帧；WW3_FAKE_HOOK_KILL_AFTER>0 时在转发第 K 帧后带 reasoning_loop 退出。
func fakeHookStream(id string, dec *jsonl.Decoder, enc *jsonl.Encoder) int {
	killAfter, sent := fakeEnvInt("WW3_FAKE_HOOK_KILL_AFTER", 0), 0
	for {
		var in jsonl.Msg
		if err := dec.Decode(&in); err != nil {
			return 0 // 上游收尾（core 关了 stdin）
		}
		if in.Op != jsonl.OpPush && in.Op != jsonl.OpEvent {
			continue
		}
		if in.Event == "upstream.end" {
			return fakeResult(enc, id, nil) // 结束帧不转发下游（W3-25）
		}
		if err := enc.Encode(&jsonl.Msg{V: 1, ID: id, Op: jsonl.OpEvent, Event: in.Event, Data: in.Data}); err != nil {
			return 0
		}
		if sent++; killAfter > 0 && sent >= killAfter {
			return fakeResult(enc, id, jsonl.Errorf("reasoning_loop", "model", "思考链死循环（假拦截器命中）"))
		}
	}
}

func fakeResult(enc *jsonl.Encoder, id string, e *jsonl.ErrObj, results ...jsonl.RawMessage) int {
	ok := e == nil
	msg := &jsonl.Msg{V: 1, ID: id, Op: jsonl.OpResult, OK: &ok}
	if ok {
		if len(results) > 0 && len(results[0]) > 0 {
			msg.Result = results[0]
		} else {
			msg.Result = jsonl.RawMessage(`{"finish_reason":"stop"}`)
		}
	} else {
		msg.Error = e
	}
	if err := enc.Encode(msg); err != nil {
		return 1
	}
	return 0
}

func fakeEnvInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

// ---------- 测试侧夹具 ----------

// newFakeManager 建一个把测试二进制当两个插件用的 Manager（渠道 + 拦截器各一）。
func newFakeManager(t *testing.T) *Manager {
	t.Helper()
	dataDir := t.TempDir()
	m := New(t.TempDir(), dataDir)
	for _, id := range []string{fakeChan, fakeHook} {
		kind := "channel"
		if id == fakeHook {
			kind = "hook"
		}
		m.Register(&Plugin{
			Met: meta.Manifest{
				ID:   id,
				Kind: kind,
				// 与真插件一致：无常驻，控制面/会话面都是 call（W3-20/W3-25）
				Execution: meta.Execution{Control: plugin.ModeCall, Conversation: plugin.ModeCall, MaxWorkers: 4},
			},
			Exe:    os.Args[0],
			Dir:    filepath.Join(dataDir, id),
			Source: "official",
		})
	}
	return m
}

func fakeHookSpecs() []HookSpec {
	return []HookSpec{{ID: "h1", PluginID: fakeHook, Order: 10, Config: map[string]any{"enabled": true}}}
}

// recorder 收集下游事件到达时刻（OnEvent 在 rpc readLoop 里被调，需加锁）。
type recorder struct {
	mu    sync.Mutex
	n     int
	first time.Duration
}

func (r *recorder) since(start time.Time) func(string, json.RawMessage) error {
	return func(name string, data json.RawMessage) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.n++
		if r.n == 1 {
			r.first = time.Since(start)
		}
		return nil
	}
}

func (r *recorder) snapshot() (int, time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n, r.first
}

func fakeReq() *chat.GenerateRequest {
	return &chat.GenerateRequest{Model: "fakechan/m", Messages: []chat.ChatMessage{{Role: "user", Content: "hi"}}}
}

// ---------- 用例 ----------

// TestChatStreamHooksArePipelined 验「链与渠道并行、事件边到边穿链」：
// 上游 40 帧 × 20ms ≈ 800ms，若首帧在 400ms 内到达就是流水线；若退化成先收集再回放，
// 首帧必然 ≥800ms（整段生成时长）。
func TestChatStreamHooksArePipelined(t *testing.T) {
	m := newFakeManager(t)
	t.Setenv("WW3_FAKE_N", "40")
	t.Setenv("WW3_FAKE_GAP_MS", "20")

	rec := &recorder{}
	start := time.Now()
	_, err := m.ChatStream(context.Background(), fakeReq(), ChatOptions{
		PluginID: fakeChan, ChannelID: "fake-1", Hooks: fakeHookSpecs(), OnEvent: rec.since(start),
	})
	total := time.Since(start)
	if err != nil {
		t.Fatalf("ChatStream 失败: %v", err)
	}
	n, first := rec.snapshot()
	if n != 40 {
		t.Fatalf("下游事件数 = %d，期望 40", n)
	}
	if first > 400*time.Millisecond {
		t.Fatalf("首帧延迟 %v（整段 %v）——拦截器链不是流水线，事件被缓存到渠道结束后才回放", first, total)
	}
	if total < 700*time.Millisecond {
		t.Fatalf("整段仅 %v：假渠道的 20ms/帧节奏没生效，本用例的时序前提不成立", total)
	}
}

// TestChatStreamHooksNoStallOverQueueCap 验「事件数超过 queue 容量（256）不死锁」：
// 旧实现里 RunHooks 跑在渠道之后，桶一满 emit 就永久阻塞，反压把上游也堵死，
// 只能等 ChatTimeout（300s）。这里给 20s 上限，正常路径应在 1~2s 内跑完 600 帧。
func TestChatStreamHooksNoStallOverQueueCap(t *testing.T) {
	m := newFakeManager(t)
	t.Setenv("WW3_FAKE_N", "600") // > queue cap 256
	t.Setenv("WW3_FAKE_GAP_MS", "0")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rec := &recorder{}
	start := time.Now()
	_, err := m.ChatStream(ctx, fakeReq(), ChatOptions{
		PluginID: fakeChan, ChannelID: "fake-1", Hooks: fakeHookSpecs(), OnEvent: rec.since(start),
	})
	if err != nil {
		t.Fatalf("ChatStream 失败（600 帧 > 队列容量 256，疑似停摆）: %v", err)
	}
	if n, _ := rec.snapshot(); n != 600 {
		t.Fatalf("下游事件数 = %d，期望 600", n)
	}
}

// TestChatStreamHookErrorStopsUpstream 验「链上命中即终止」的两半：
// ① 下游拿到的是带 kind 的真因（reasoning_loop），不是 "closed"/EPIPE 这类传输层噪声；
// ② 渠道 worker 被提前回收，上游不再白跑完整段（8s 的流应在 3s 内结束）。
func TestChatStreamHookErrorStopsUpstream(t *testing.T) {
	m := newFakeManager(t)
	t.Setenv("WW3_FAKE_N", "400") // 整段需 8s
	t.Setenv("WW3_FAKE_GAP_MS", "20")
	t.Setenv("WW3_FAKE_HOOK_KILL_AFTER", "5") // 第 5 帧后命中

	rec := &recorder{}
	start := time.Now()
	_, err := m.ChatStream(context.Background(), fakeReq(), ChatOptions{
		PluginID: fakeChan, ChannelID: "fake-1", Hooks: fakeHookSpecs(), OnEvent: rec.since(start),
	})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("期望拦截器命中后返回错误，实际 err=nil")
	}
	if got := rpc.KindOf(err); got != "reasoning_loop" {
		t.Fatalf("错误 kind = %q（err=%v），期望 reasoning_loop：命中原因被传输层错误盖掉了", got, err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("耗时 %v：渠道 worker 未被提前终止，上游仍在白跑完整段生成", elapsed)
	}
	if n, _ := rec.snapshot(); n > 10 {
		t.Fatalf("下游收到 %d 帧，期望 ≤10（命中后应立即停止转发）", n)
	}
}

// TestChatStreamNoHooksPassthrough 守住「无拦截器 = 直通、零额外跳转」这条基线（W3-25）。
func TestChatStreamNoHooksPassthrough(t *testing.T) {
	m := newFakeManager(t)
	t.Setenv("WW3_FAKE_N", "30")
	t.Setenv("WW3_FAKE_GAP_MS", "0")

	rec := &recorder{}
	start := time.Now()
	if _, err := m.ChatStream(context.Background(), fakeReq(), ChatOptions{
		PluginID: fakeChan, ChannelID: "fake-1", OnEvent: rec.since(start),
	}); err != nil {
		t.Fatalf("ChatStream 失败: %v", err)
	}
	if n, _ := rec.snapshot(); n != 30 {
		t.Fatalf("下游事件数 = %d，期望 30", n)
	}
}

// ---------- request.prepare 阶段回归 ----------

// prepareHook 是第二个假拦截器：声明 request.prepare，不参与 hook.stream。
const prepareHook = "fakeprepare"

// fakePrepareHookSpecs 返回带 Prepare 标志的拦截器链（模拟 route.hookSpecs 的填充结果）。
func fakePrepareHookSpecs() []HookSpec {
	return []HookSpec{{ID: "p1", PluginID: prepareHook, Order: 5, Config: map[string]any{}, Prepare: true}}
}

// TestChatStreamPrepareRewritesRequest 验 request.prepare 的核心语义：
// ① 声明了 prepare 的 hook 在渠道调用**之前**收到 request；
// ② 改写后的 messages 被渠道 worker 看到（渠道侧校验首条消息是否是 system）。
func TestChatStreamPrepareRewritesRequest(t *testing.T) {
	m := newFakeManager(t)
	m.Register(&Plugin{
		Met: meta.Manifest{
			ID:           prepareHook,
			Kind:         "hook",
			Execution:    meta.Execution{Control: plugin.ModeCall, Conversation: plugin.ModeCall, MaxWorkers: 4},
			Capabilities: meta.Capabilities{Conversation: []string{"hook.stream", "request.prepare"}},
		},
		Exe:    os.Args[0],
		Dir:    filepath.Join(t.TempDir(), prepareHook),
		Source: "official",
	})
	// 假渠道校验入站 messages 首条是否是 system（prepare 注入的结果）
	t.Setenv("WW3_FAKE_EXPECT_SYSTEM", "1")
	t.Setenv("WW3_FAKE_N", "1")
	t.Setenv("WW3_FAKE_GAP_MS", "0")

	if _, err := m.ChatStream(context.Background(), fakeReq(), ChatOptions{
		PluginID: fakeChan, ChannelID: "fake-1", Hooks: fakePrepareHookSpecs(), OnEvent: func(string, json.RawMessage) error { return nil },
	}); err != nil {
		t.Fatalf("ChatStream 失败（prepare 注入后渠道校验未过）: %v", err)
	}
}

// TestChatStreamPrepareRejectsRequest 验 prepare 的 fail-closed 语义：
// prepare 返回错误 = 拒绝请求，渠道 worker 不启动。
func TestChatStreamPrepareRejectsRequest(t *testing.T) {
	m := newFakeManager(t)
	m.Register(&Plugin{
		Met: meta.Manifest{
			ID:           prepareHook,
			Kind:         "hook",
			Execution:    meta.Execution{Control: plugin.ModeCall, Conversation: plugin.ModeCall, MaxWorkers: 4},
			Capabilities: meta.Capabilities{Conversation: []string{"hook.stream", "request.prepare"}},
		},
		Exe:    os.Args[0],
		Dir:    filepath.Join(t.TempDir(), prepareHook),
		Source: "official",
	})
	t.Setenv("WW3_FAKE_PREPARE_REJECT", "1") // 让 prepare 返回错误
	t.Setenv("WW3_FAKE_N", "5")

	rec := &recorder{}
	_, err := m.ChatStream(context.Background(), fakeReq(), ChatOptions{
		PluginID: fakeChan, ChannelID: "fake-1", Hooks: fakePrepareHookSpecs(), OnEvent: rec.since(time.Now()),
	})
	if err == nil {
		t.Fatal("期望 prepare 拒绝后返回错误，实际 err=nil")
	}
	// 拒绝原因应保留 hook 的 kind（prompt_too_long），供 route 按 errkind 分类，
	// 不能被包成"结果解析失败"这类传输层噪声。
	if got := rpc.KindOf(err); got != "prompt_too_long" {
		t.Fatalf("错误 kind = %q（err=%v），期望 prompt_too_long：hook 的拒绝原因被盖掉了", got, err)
	}
	// 渠道 worker 不应启动：下游事件数为 0
	if n, _ := rec.snapshot(); n != 0 {
		t.Fatalf("下游事件数 = %d，期望 0（prepare 拒绝后渠道不应启动）", n)
	}
}

// TestChatStreamPrepareBackwardCompat 验向后兼容：未声明 prepare 的 hook 不受影响。
func TestChatStreamPrepareBackwardCompat(t *testing.T) {
	m := newFakeManager(t)
	t.Setenv("WW3_FAKE_N", "10")
	t.Setenv("WW3_FAKE_GAP_MS", "0")

	rec := &recorder{}
	// 用普通的 fakeHookSpecs（Prepare 默认 false）——prepare 阶段应被跳过
	if _, err := m.ChatStream(context.Background(), fakeReq(), ChatOptions{
		PluginID: fakeChan, ChannelID: "fake-1", Hooks: fakeHookSpecs(), OnEvent: rec.since(time.Now()),
	}); err != nil {
		t.Fatalf("ChatStream 失败: %v", err)
	}
	if n, _ := rec.snapshot(); n != 10 {
		t.Fatalf("下游事件数 = %d，期望 10（未声明 prepare 的 hook 不应影响正常流）", n)
	}
}
