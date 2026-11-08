package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"wild-work3/main/core/internal/api"
	"wild-work3/main/core/internal/config"
	"wild-work3/main/core/internal/pool"
	"wild-work3/main/core/internal/route"
	"wild-work3/main/core/internal/runtime"
	"wild-work3/main/core/internal/scheduler"
	"wild-work3/main/core/internal/stat"
	"wild-work3/main/protocol/chat"
	"wild-work3/main/protocol/cli"
	"wild-work3/main/protocol/errkind"
	"wild-work3/main/protocol/jsonl"
	"wild-work3/main/protocol/rpc"
	"wild-work3/main/rules"
)

const coreVersion = "1.0.0"

// 进程级日志（nil 时静默）。stdout 只允许输出协议消息，日志一律走这里/stderr。
var (
	logMu sync.RWMutex
	gLog  *cli.Logger
)

func setLog(l *cli.Logger) { logMu.Lock(); gLog = l; logMu.Unlock() }

func lg() *cli.Logger {
	logMu.RLock()
	defer logMu.RUnlock()
	return gLog
}

// application 汇集 core 的运行态依赖（多模式下共用）。
type application struct {
	dataDir    string
	pluginsDir string
	configFile string // 统一配置文件路径（--config 指定；空 = 默认 <data>/config.json）
	listen     string
	hooks      string // 拦截器链覆盖（--hooks：none / id1,id2；空 = 从配置读）
	gwListen   string // gateway 对外监听（只用于「打开错端口」提示，空=未知）
	log        *cli.Logger

	store  *config.Store
	pool   *pool.Pool
	led    *stat.Book
	rt     *runtime.Manager
	router *route.Router
	srv    *api.Server
	sched  *scheduler.Scheduler
}

// init 加载配置/流水/插件（serve 与 stdio 模式共用；CLI 只读模式也用）。
func (a *application) init() error {
	var st *config.Store
	var err error
	if a.configFile != "" {
		st, err = config.OpenFile(a.configFile)
	} else {
		st, err = config.Open(a.dataDir)
	}
	if err != nil {
		return fmt.Errorf("加载配置失败: %w", err)
	}
	// 请求体摘要开关（诊断用，默认关，旧版 stat.SetBodyTrace 同口径）：
	// 进程级全局生效，面板改配置时由 api 层再同步。
	stat.SetBodyTrace(st.Get().BodyLog)
	// 监听地址优先级：命令行 > 配置文件 > 默认值。
	// 命令行只是本次进程的临时覆盖，**不写回配置**（旧实现会把 --listen 持久化，
	// 与 gateway 侧语义不一致，且让「临时调试一次」永久改了用户配置）。
	if a.listen != "" && a.listen != st.Get().Listen {
		a.log.Infof("命令行 --listen=%s 覆盖配置值 %s（不落盘）", a.listen, st.Get().Listen)
	}
	// 错端口防呆：core 知道 gateway 的对外地址后，浏览器直连 core 端口时才能给出可点的正确地址。
	api.SetGatewayListen(a.gwListen)
	led, err := stat.Open(filepath.Join(a.dataDir, "stat"))
	if err != nil {
		return fmt.Errorf("打开流水失败: %w", err)
	}
	cfg := st.Get()
	p := pool.New(time.Duration(nz(cfg.CoolSecondsSoft, 60))*time.Second, time.Duration(nz(cfg.CoolSecondsHard, 43200))*time.Second)
	// 连续错误计数冷却（旧版 err_threshold / err_cooldown）：默认 3 次 / 10m。
	p.Configure(cfg.Cooling())
	// 冷却/错误计数跨重启保留（旧版 state.json）：先恢复运行时状态，再用配置里的账号身份覆盖。
	if err := p.SetPersist(filepath.Join(a.dataDir, "pool-state.json")); err != nil {
		a.log.Errorf("账号池状态恢复失败（忽略，从空池开始）: %v", err)
	}
	for _, ac := range cfg.Accounts {
		// ExpireAt 是「额度最早到期日」（路由排序键），不是 token 过期时刻；
		// 这里先置空，等插件上报 resource_detail 后由 SetCreditDetail 写入。
		p.Upsert(pool.Entry{UID: ac.UID, ChannelID: ac.ChannelID, Nickname: ac.Nickname, Credits: ac.Credits, Unit: ac.Unit})
	}
	// 单账号渠道启动自愈：这类渠道任何账号级惩罚都等于整条渠道下线（无号可换），
	// 上个进程留下的脏冷却不该继续锁死（旧版 pool.ClearPenalty 同口径）。
	for _, ch := range cfg.Channels {
		p.ClearChannelPenalty(ch.ID)
	}
	rt := runtime.New(a.pluginsDir, a.dataDir)
	rt.SetLogger(a.log) // 拦截器链的降级/失败告警落 core.log
	if err := rt.Discover(); err != nil {
		a.log.Errorf("扫描插件目录失败: %v", err)
	}
	for _, pl := range rt.Plugins() {
		a.log.Infof("已加载插件: %s v%s (%s) exe=%s", pl.Met.ID, pl.Met.Version, pl.Met.Kind, pl.Exe)
	}
	// 必须先补渠道再启动 syncAccounts，否则首次安装的凭据没有渠道可供扫描。
	if err := seedDefaultChannels(st, rt); err != nil {
		return fmt.Errorf("补建插件默认渠道失败: %w", err)
	}
	router := &route.Router{Store: st, Pool: p, RT: rt, HooksOverride: a.hooks}
	router.Logf = a.log.Infof
	// 规则引擎（Pass B）：channel 作用域规则在 core 侧每个候选渠道调用前施加。
	// 规则定义权威仍来自统一配置 config.json 的 rules 段（默认规则内嵌 main/rules），
	// 只是执行时机必须在「最终渠道已知」之后（gateway 路由前不知道会落到哪个渠道）。
	if rs, rerr := st.Rules(); rerr != nil {
		a.log.Errorf("规则加载失败（Pass B 已禁用，请检查 config.json 的 rules 段）: %v", rerr)
	} else if eng, eerr := rules.New(rs); eerr != nil {
		a.log.Errorf("规则编译失败（Pass B 已禁用）: %v", eerr)
	} else {
		router.Rules = eng
	}
	// 401 自愈：收到 SessionDead 时就地刷新凭据并重试一次（旧版 refreshIfSessionDead）。
	// 复用控制面 `accounts.refresh`（插件负责刷新 + 原子落盘）；失败返回 false 交给冷却。
	router.SelfHeal = func(ctx context.Context, channelID, acctUID string) bool {
		ch := st.Channel(channelID)
		if ch == nil || !ch.Enabled {
			return false
		}
		// 未声明 accounts.refresh 的渠道无法就地刷新（如纯 API Key 渠道）
		if pl := rt.Plugin(ch.PluginID); pl == nil || !pl.Met.Capabilities.HasControl("accounts.refresh") {
			return false
		}
		uid := acctUID
		for _, ac := range st.Get().Accounts {
			if ac.UID == acctUID {
				uid = ac.RemoteUID
				if uid == "" {
					uid = acctUID
				}
				break
			}
		}
		tctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		_, err := rt.Call(tctx, ch.PluginID, "accounts.refresh",
			map[string]any{"uid": uid, "config": ch.Config})
		if err != nil {
			a.log.Infof("401 自愈刷新失败 channel=%s uid=%s: %v", channelID, acctUID, err)
			return false
		}
		return true
	}
	a.store, a.led, a.pool, a.rt, a.router = st, led, p, rt, router
	a.srv = &api.Server{Store: st, Pool: p, RT: rt, Stat: led, Router: router,
		Ver: map[string]string{"core": coreVersion, "ipc": "1"}}
	return nil
}

// seedDefaultChannels 只补缺失的实例 ID；已有渠道（包括用户改名、禁用和自定义配置）原样保留。
//
// 「已存在」还要看 DefaultID 印记（W3-45）：用户把内建渠道改名后，配置里不再有 def.ID，
// 若只按 ID 去重就会重建一份，同一个上游凭空多出一个实例。首次补建时把 def.ID 写进 DefaultID，
// 之后无论怎么改名都能认出来。
func seedDefaultChannels(st *config.Store, rt *runtime.Manager) error {
	return st.Update(func(c *config.Config) error {
		seen := make(map[string]bool, len(c.Channels))
		seeded := make(map[string]bool, len(c.Channels)) // 插件已预置过的 default 标识
		for _, ch := range c.Channels {
			seen[ch.ID] = true
			if ch.DefaultID != "" {
				seeded[ch.PluginID+"\x00"+ch.DefaultID] = true
			}
		}
		// 回填印记：安装树摆盘/旧版配置里的内建渠道没有 DefaultID，
		// 而「ID 恰好等于某插件声明的 def.ID」就是尚未改名的铁证，可在启动时就补上。
		for _, pl := range rt.Plugins() {
			for _, def := range pl.Met.DefaultChannels {
				id := strings.TrimSpace(def.ID)
				if id == "" || seeded[pl.Met.ID+"\x00"+id] {
					continue
				}
				for _, ch := range c.Channels {
					if ch.ID == id && ch.PluginID == pl.Met.ID {
						ch.DefaultID = id
						seeded[pl.Met.ID+"\x00"+id] = true
					}
				}
			}
		}
		for _, pl := range rt.Plugins() {
			if pl.Met.Kind != "channel" {
				continue
			}
			for _, def := range pl.Met.DefaultChannels {
				id := strings.TrimSpace(def.ID)
				if id == "" || seen[id] {
					continue
				}
				if seeded[pl.Met.ID+"\x00"+id] {
					continue // 该内建实例已被用户改名为别的 ID，不重建
				}
				seen[id] = true
				seeded[pl.Met.ID+"\x00"+id] = true
				c.Channels = append(c.Channels, &config.ChannelConfig{
					ID: id, PluginID: pl.Met.ID, DisplayName: def.Name,
					Enabled: true, Config: def.Config, DefaultID: id,
				})
			}
		}
		return nil
	})
}

// listenAddr 实际监听地址：命令行 --listen 覆盖配置值。
func (a *application) listenAddr() string {
	if a.listen != "" {
		return a.listen
	}
	return a.store.Get().Listen
}

// runServe 生产形态：HTTP 管理面 + stdio IPC + 定时调度并发。
func (a *application) runServe() {
	if err := a.init(); err != nil {
		fatal("%v", err)
	}
	defer a.led.Close()
	a.syncAccounts()
	a.startScheduler()

	httpSrv := &http.Server{Addr: a.listenAddr(), Handler: a.srv.Handler()}
	go func() {
		a.log.Infof("管理面监听 %s", a.listenAddr())
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			a.log.Errorf("HTTP 服务退出: %v", err)
		}
	}()

	shutdown := func(reason string) {
		a.log.Infof("退出：%s", reason)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(ctx)
		a.stopPlugins()
		os.Exit(0)
	}
	api.SetQuit(func() { shutdown("收到 API 退出请求") })

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() { <-sigCh; shutdown("收到信号") }()

	a.serveIPC()
	shutdown("stdin 关闭（父进程已退出）")
}

// runStdioOnly 只跑 stdio IPC（gateway 的默认调用方式，也供 core CLI 自驱动）。
func (a *application) runStdioOnly() {
	if err := a.init(); err != nil {
		fatal("%v", err)
	}
	defer a.led.Close()
	a.syncAccounts()
	a.startScheduler()
	a.serveIPC()
	a.stopPlugins()
}

// startScheduler 启动定时任务（签到/保活/积分刷新）。
//
// gateway 用 `core serve` 拉起 core，stdio 由它继承，因此定时任务必须在
// serve 里跑；stdio-only 模式（CLI 自驱动）也起一份，保证调试时行为一致。
// 多个 core 进程同时跑同一 data 目录会导致重复签到，故 CLI 一次性命令
// （chat/models/channels/usage）不起调度器。
func (a *application) startScheduler() {
	sched := scheduler.New(a.store, a.pool, a.log, a.srv)
	a.sched = sched
	go sched.Run(context.Background())
	a.log.Infof("定时任务就绪：%s", sched.Describe())
}

// syncAccounts 启动时从插件磁盘导入既有凭据（旧版 import_local 语义）。
// 放在后台并带超时：任何插件卡住都不能拖慢 core 启动。
func (a *application) syncAccounts() {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if n := a.srv.SyncAccounts(ctx); n > 0 {
			a.log.Infof("启动账号同步完成：导入/更新 %d 个账号", n)
		}
	}()
}

// serveIPC 会话面：gateway → core 的统一流式契约。
func (a *application) serveIPC() {
	a.log.Infof("IPC 就绪（stdio JSONL），版本 %s", coreVersion)
	ipc := rpc.NewServer(os.Stdin, os.Stdout)
	ipc.Handle("initialize", func(ctx context.Context, p jsonl.RawMessage, ev *rpc.EventWriter) (jsonl.RawMessage, *jsonl.ErrObj) {
		return mustJSON(map[string]any{"protocol": 1, "role": "core", "version": coreVersion}), nil
	})
	ipc.Handle("capabilities.list", func(ctx context.Context, p jsonl.RawMessage, ev *rpc.EventWriter) (jsonl.RawMessage, *jsonl.ErrObj) {
		return mustJSON(map[string]any{"control": []string{"initialize", "capabilities.list", "ping", "health.check", "shutdown"},
			"conversation": []string{"chat.generate"}}), nil
	})
	ipc.Handle("ping", func(ctx context.Context, p jsonl.RawMessage, ev *rpc.EventWriter) (jsonl.RawMessage, *jsonl.ErrObj) {
		return mustJSON("pong"), nil
	})
	ipc.Handle("health.check", func(ctx context.Context, p jsonl.RawMessage, ev *rpc.EventWriter) (jsonl.RawMessage, *jsonl.ErrObj) {
		return mustJSON(map[string]any{"state": "ok", "plugins": len(a.rt.Plugins())}), nil
	})
	ipc.Handle("chat.generate", a.handleIPCGenerate)
	ipc.Handle("shutdown", func(ctx context.Context, p jsonl.RawMessage, ev *rpc.EventWriter) (jsonl.RawMessage, *jsonl.ErrObj) {
		return mustJSON(map[string]any{"ok": true}), nil
	})
	if err := ipc.Serve(context.Background()); err != nil {
		a.log.Errorf("IPC 退出: %v", err)
	}
}

// handleIPCGenerate 一次对话：路由 → 插件 worker → 事件 → 记账。
func (a *application) handleIPCGenerate(ctx context.Context, raw jsonl.RawMessage, ev *rpc.EventWriter) (jsonl.RawMessage, *jsonl.ErrObj) {
	var req chat.GenerateRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, jsonl.Errorf(errkind.BadParams, errkind.ScopeRequest, "请求解析失败: %v", err)
	}
	start := time.Now()
	a.log.Infof("chat.generate model=%s stream=%v msgs=%d tools=%d conv=%s", req.Model, req.Stream, len(req.Messages), len(req.Tools), req.ConversationID)
	// 请求体构成摘要（诊断用，默认关；旧版 stat.TraceBody 同口径）：
	// 只记条数/角色分布/体量 Top10 + 每条前 200 字，用来定位「上下文为何膨胀」。
	// env 用原始 params 字节（就是客户端请求的归一化体，未经任何改写）。
	if a.led != nil && stat.BodyTraceEnabled() {
		a.led.TraceBody("", req.Model, raw)
	}

	var generated strings.Builder
	res, err := a.router.Execute(ctx, &req, &ipcHandler{ev: ev, fn: func(name string, data json.RawMessage) error {
		var fields map[string]any
		if len(data) > 0 {
			_ = json.Unmarshal(data, &fields)
		}
		if fields == nil {
			fields = map[string]any{}
		}
		// 上游 usage 先由 runtime 字段级合并；core 成功收口时只向下游发一帧 finalized usage，
		// 保证客户端与 stat 记账使用同一口径，也避免消费者误把多帧 usage 相加。
		if name == "usage" {
			return nil
		}
		// completed/incomplete 是插件内部终结帧；core 必须在 finalized usage 之后才向 gateway 收尾，
		// 否则 SSE 客户端会先收到 [DONE]，最终 usage 丢失。
		if name == "completed" || name == "incomplete" {
			return nil
		}
		if name == "text.delta" || name == "reasoning.delta" {
			if s, _ := fields["content"].(string); s != "" {
				generated.WriteString(s)
			}
		} else if name == "tool_call.delta" {
			if s, _ := fields["name"].(string); s != "" {
				generated.WriteString(s)
			}
			if s, _ := fields["arguments_fragment"].(string); s != "" {
				generated.WriteString(s)
			}
		}
		return ev.Event(name, fields)
	}})

	rec := stat.UsageRecord{ConversationID: req.ConversationID, Model: req.Model}
	if res != nil {
		rec.ChannelID, rec.AccountUID, rec.OK = res.ChannelID, res.AccountUID, true
		rec.Model = req.Model
		// 降级留痕（旧版 stats.Fallbacks 内存环形 200 条）：面板「统计」tab 的降级记录区块。
		if res.FallbackFrom != "" {
			a.srv.AddFallback(res.FallbackFrom, req.Model, res.ChannelID)
		}
		res.Usage = chat.FinalizeUsage(&req, res.Usage, generated.String())
		rec.PromptTokens, rec.CompletionTokens, rec.TotalTokens = res.Usage.PromptTokens, res.Usage.CompletionTokens, res.Usage.TotalTokens
		rec.PromptReported, rec.CompletionReported, rec.TotalReported = res.Usage.PromptReported, res.Usage.CompletionReported, res.Usage.TotalReported
		rec.PromptEstimated, rec.CompletionEstimated, rec.TotalEstimated = res.Usage.PromptEstimated, res.Usage.CompletionEstimated, res.Usage.TotalEstimated
		rec.TimeMs, rec.FirstTokenMs = res.Usage.TimeMs, res.Usage.FirstTokenMs
		if res.Usage.Cache != nil {
			rec.CachedTokens, rec.CachedReported = res.Usage.Cache.Cached, res.Usage.Cache.CachedReported
			rec.CacheWriteTokens, rec.CacheWriteReported = res.Usage.Cache.Write, res.Usage.Cache.WriteReported
		}
		_ = ev.Event("usage", res.Usage)
	} else {
		rec.TimeMs = time.Since(start).Milliseconds()
		if err != nil {
			rec.ErrKind = rpc.KindOf(err)
		}
	}
	a.led.AppendUsage(rec)
	// 结构化请求日志（旧版 stats 的「请求日志」区块）：面板表格的数据源，
	// 与文本日志（api.Logf）分开——这里只记事实，不记正文。
	api.AddRequestLog(api.RequestLogEntry{
		Model: req.Model, Channel: rec.ChannelID, Account: rec.AccountUID, OK: rec.OK,
		ErrKind: rec.ErrKind, Prompt: rec.PromptTokens, Completion: rec.CompletionTokens,
		Total: rec.TotalTokens, TimeMs: rec.TimeMs, FirstMs: rec.FirstTokenMs,
	})
	if !rec.OK {
		// 失败落盘（W3-46）：内存环形重启即清，而「昨天到底怎么挂的」只能在磁盘上找。
		// 明细进 usage 流水已够，这里额外记 ErrKind 与截断后的上游摘要，便于单表排查。
		a.led.AppendFail(stat.FailRecord{
			RequestID: rec.RequestID, ConversationID: rec.ConversationID,
			ChannelID: rec.ChannelID, AccountUID: rec.AccountUID, Model: rec.Model,
			ErrKind: rec.ErrKind, Message: failSummary(err), TimeMs: rec.TimeMs, FirstTokenMs: rec.FirstTokenMs,
		})
	}

	if err != nil {
		kind := errkind.Server
		if k := rpc.KindOf(err); k != "" {
			kind = errkind.Kind(k)
		}
		scope := scopeOf(kind)
		msg := cleanErrMsg(err) // 去掉 rpc.RPCError 自带的 "[kind]scope: " 前缀，避免二次拼接
		// 上游明示的重试等待（排队/限流）透传给下游，客户端据此退避（旧版 Queued.retryAfter）。
		retryAfter := rpc.RetryAfterOf(err).Milliseconds()
		a.log.Errorf("chat.generate 失败 model=%s kind=%s scope=%s 耗时=%dms: %s", req.Model, kind, scope, time.Since(start).Milliseconds(), msg)
		out := map[string]any{"kind": kind, "scope": scope, "message": msg, "retryable": errkind.Rotatable(kind)}
		if retryAfter > 0 {
			out["retry_after_ms"] = retryAfter
		}
		_ = ev.Event("error", out)
		return nil, &jsonl.ErrObj{Kind: kind, Scope: scope, Message: msg, RetryAfterMs: retryAfter}
	}
	a.log.Infof("chat.generate 完成 model=%s channel=%s acct=%s 尝试=%d tokens=%d/%d 耗时=%dms",
		req.Model, res.ChannelID, res.AccountUID, res.Attempts, rec.PromptTokens, rec.CompletionTokens, time.Since(start).Milliseconds())
	// 降级必留痕（规则 3）：把原模型回给网关，由它发 X-Model-Fallback 头。
	out := map[string]any{"ok": true, "channel": res.ChannelID}
	if res.FallbackFrom != "" {
		a.log.Infof("model fallback from=%s to=%s channel=%s", res.FallbackFrom, req.Model, res.ChannelID)
		out["fallback_from"] = res.FallbackFrom
	}
	return mustJSON(out), nil
}

// stopPlugins 无需做任何事：渠道插件与拦截器插件都无常驻进程（W3-20 / W3-25），
// 每个调用进程都在自己的调用结束时被回收（渠道见 runtime.call 的 plugin.RunOnce，
// 拦截器见 runtime.startHook 的 c.Wait + defer c.Close）。保留此方法只为对称（与 startPlugins 配对）。
func (a *application) stopPlugins() {}

type handlerFunc func(name string, data json.RawMessage) error

func (f handlerFunc) OnEvent(name string, data json.RawMessage) error { return f(name, data) }

// ipcHandler 把事件转发到 IPC 事件流，并实现 route.fallbackNamer：
// 在候选模型切换时向网关插一条 `model.fallback`，让它能赶在首帧前改
// `X-Model-Fallback` 头与 chunk 的 model 字段（降级必留痕，规则 3）。
type ipcHandler struct {
	ev *rpc.EventWriter
	fn handlerFunc
}

func (h *ipcHandler) OnEvent(name string, data json.RawMessage) error { return h.fn(name, data) }

// OnFallback 只传通知，不写正文；to 是降级链里配置的模型全路径。
func (h *ipcHandler) OnFallback(from, to string) {
	_ = h.ev.Event("model.fallback", map[string]any{"from": from, "to": to})
}

func mustJSON(v any) jsonl.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// cleanErrMsg 取错误原始消息（RPCError.Error() 会带 "[kind]scope: " 前缀，转发出错时会重复拼接）。
func cleanErrMsg(err error) string {
	return rpc.MessageOf(err)
}

// failSummary 失败摘要（只用于落盘排查）：nil 错误返回空串，其余取裸消息。
// 不落凭据——上游错误体里即使回显 key，也会在 core 的 redact 环节被遮（§9 红线）。
func failSummary(err error) string {
	if err == nil {
		return ""
	}
	return cleanErrMsg(err)
}

func scopeOf(k errkind.Kind) errkind.Scope {
	switch k {
	case errkind.HardCredit, errkind.SoftRate, errkind.SessionDead, errkind.AccountFault, errkind.WafBlock:
		return errkind.ScopeAccount
	case errkind.ModelBlocked, errkind.NotFound:
		return errkind.ScopeModel
	case errkind.Server, errkind.Conn, errkind.Truncated, errkind.IdleTimeout:
		return errkind.ScopeChannel
	}
	return errkind.ScopeRequest
}

func nz(v, d int) int {
	if v == 0 {
		return d
	}
	return v
}

func fatal(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "[core] "+format+"\n", a...)
	os.Exit(1)
}

func defaultDataDir() string {
	if d := os.Getenv("WW3_DATA"); d != "" {
		return d
	}
	exe, _ := os.Executable()
	return filepath.Join(filepath.Dir(exe), "data")
}

func defaultPluginsDir() string {
	if d := os.Getenv("WW3_PLUGINS"); d != "" {
		return d
	}
	exe, _ := os.Executable()
	return filepath.Join(filepath.Dir(exe), "plugins")
}

var _ = flag.Parse
