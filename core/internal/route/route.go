// Package route 实现模型路由、账号选择、请求内重试/换号/降级（W3-09 / 旧版 serveRotation 语义）。
package route

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"wild-work3/main/core/internal/config"
	"wild-work3/main/core/internal/pool"
	"wild-work3/main/core/internal/runtime"
	"wild-work3/main/protocol/chat"
	"wild-work3/main/protocol/errkind"
	"wild-work3/main/protocol/rpc"
	"wild-work3/main/rules"
)

// Router 路由 + 调度。
type Router struct {
	Store *config.Store
	Pool  *pool.Pool
	RT    *runtime.Manager

	// SelfHeal 是可选的「401 自愈」钩子：收到 SessionDead 时先尝试刷新一次凭据，
	// 成功则允许同请求重试一次（旧版 dev/internal/app/app.go:2099 refreshIfSessionDead）。
	//
	// 为何不能只靠保活：保活是定时任务（默认 22 点），而本地 expiresAt 可能不准
	// （token 已在别处被轮换/上次刷新未落盘），上游已拒但本地仍显示有效 ——
	// 不就地刷新就会卡在 401 直到下次保活。
	//
	// 返回 true 表示已刷新（凭据已落盘），调用方应重试一次。
	SelfHeal func(ctx context.Context, channelID, acctUID string) bool

	// Logf 可选日志钩子（core 注入 cli.Logger；为空则不打日志）。
	Logf func(format string, a ...any)

	// Rules 可选规则引擎（Pass B：channel 作用域规则）。
	//
	// 每个候选渠道调用前从 pristine 请求重建，因此 fallback 到 B 时不会累积 A 的改写。
	// nil 或该渠道无规则时完全跳过（不做请求体往返）。
	Rules *rules.Engine
}

// logf 可选日志（未注入则丢弃）。
func (r *Router) logf(format string, a ...any) {
	if r.Logf != nil {
		r.Logf(format, a...)
	}
}

// hardDur 本次硬冷却时长：配了每日发奖窗口时截到窗口（R73），否则等于池子的 hard。
func (r *Router) hardDur() time.Duration {
	base := r.Pool.HardCooldown()
	if r.Store == nil {
		return base
	}
	return r.Store.Get().HardCooldownFor(r.Pool.Now(), base)
}

// Resolve 解析模型名到 (channel_id, upstream_model)，六级：
// ① 渠道 ID 前缀 → ② ModelRoutes 精确 → ③ 通配 → ④ 命名空间前缀（反查各渠道已刷新模型）
// → ⑤ 渠道 models 列表 → ⑥ 默认渠道。
func (r *Router) Resolve(model string) (channelID, upModel string, ok bool) {
	cfg := r.Store.Get()
	// 1) 模型名自带 channel/ 前缀
	if i := strings.Index(model, "/"); i > 0 {
		prefix := model[:i]
		for _, ch := range cfg.Channels {
			if ch.ID == prefix && ch.Enabled {
				return ch.ID, model[i+1:], true
			}
		}
	}
	// 2) 精确路由
	if cid, found := cfg.ModelRoutes[model]; found {
		return cid, model, true
	}
	// 3) 通配路由
	for k, cid := range cfg.ModelRoutes {
		if strings.HasSuffix(k, "*") && strings.HasPrefix(model, strings.TrimSuffix(k, "*")) {
			return cid, model, true
		}
	}
	// 4) 命名空间前缀：模型名形如 <ns>/<upstream>，而渠道 ID 未必等于 <ns>
	// （workbuddy 双区：一个插件产出 workbuddy/、workbuddyai/ 两个命名空间，
	//   渠道实例却叫 wb-cn / wb-global）。所以再拿各渠道已刷新到的模型清单
	//   反推命名空间；命中则把**完整模型名**下发给插件（插件自己剥前缀）。
	if i := strings.Index(model, "/"); i > 0 {
		ns := model[:i]
		for _, ch := range cfg.Channels {
			if !ch.Enabled {
				continue
			}
			for _, m := range ch.Models {
				if j := strings.Index(m, "/"); j > 0 && m[:j] == ns {
					return ch.ID, model, true
				}
			}
		}
	}
	// 5) 模型属于某渠道的 models 列表
	for _, ch := range cfg.Channels {
		if !ch.Enabled {
			continue
		}
		for _, m := range ch.Models {
			if m == model {
				return ch.ID, model, true
			}
		}
	}
	// 6) 默认渠道兼底。
	//
	// 必须同时校验存在且 Enabled：面板关掉一个渠道后，若兼底不看 Enabled，
	// 对话仍会打到该渠道（实测 `mock/x` 与裸名在 enabled=[] 时照样成功）。
	if cfg.DefaultChannel != "" {
		for _, ch := range cfg.Channels {
			if ch.ID == cfg.DefaultChannel && ch.Enabled {
				return ch.ID, model, true
			}
		}
	}
	return "", "", false
}

// Result 一次成功调用的产物（由调用方消费事件后填充）。
type Result struct {
	Usage        *chat.Usage
	FinishReason string
	ChannelID    string
	AccountUID   string
	Attempts     int
	// FallbackFrom 非空时表示本次请求发生了模型降级（原模型），供网关发 X-Model-Fallback
	FallbackFrom string
}

// Handler 事件消费者：返回 error 可提前中断本次尝试。
type Handler interface {
	OnEvent(name string, data json.RawMessage) error
}

// fallbackNamer 可选接口：handler 实现后，路由层在切换候选模型时通知它。
//
// 存在的理由：网关需要**在流开始前**就知道最终模型名，以便正确写
// `X-Model-Fallback` 头与 chunk 的 model 字段；而 core 内部要到整个请求结束
// 才在 result 里回 `fallback_from`，那时首帧早已发出。实现此接口的 handler
// 就能在事件流上插一条 `model.fallback`（不写正文），由网关消费。
//
// 未实现时（例如 CLI 直接调 router）只是少一条通知，不影响主流程。
type fallbackNamer interface {
	OnFallback(from, to string)
}

// Execute 执行一次对话：路由 → 选号 → 调用（失败按 ErrKind 重试/换号/降级）。
//
// 四类处置（旧版 dev/docs/模型降级链定义.md §5，按动作幅度排序）：
//
//	request              请求自身问题 → 零动作，原文透传
//	upstream-transparent 上游整体故障（5xx/传输层）→ 不换号、不冷却、不降级
//	account-transparent  上游已给确定性结论（waf/model_blocked）→ 冷却该号，不换号
//	account-rotate       账号可用性问题 → 换号（上限 = 候选账号数）
//	model                该渠道对该模型无承载能力 → 换号耗尽后换模型（降级链）
//
// 注意不可逆边界（W3-10）：一旦 handler 已收到任何响应事件，则不再重试/换号/降级。
func (r *Router) Execute(ctx context.Context, req *chat.GenerateRequest, h Handler) (*Result, error) {
	cfg := r.Store.Get()
	// 下游兼容层（模型名映射 / 思考强度注入 / max_tokens 封顶）在路由前统一应用；
	// 不修改调用方结构体，复制一份再改。
	local := *req
	r.applyCompat(cfg, &local)
	req = &local

	// 降级链按**原模型名**（客户端看到的那个）查：ModelFallback 的 key 是用户配置的模型，
	// 而 Resolve 可能已把它解析成上游模型名（剥了渠道前缀）。
	origModel := req.Model
	cands := r.candidates(cfg, origModel)

	var lastErr error
	attempts := 0
	for i, c := range cands {
		if i > 0 {
			// 先通知再试：网关要靠它赶在首帧前写 X-Model-Fallback 头与 model 字段。
			// 若本候选最终也失败，事件流里已经多了一条通知，但正文尚未开始，
			// 网关只改头/模型名不改正文，仍属于合法留痕（后续若失败会直接报错终止）。
			if n, ok := h.(fallbackNamer); ok {
				n.OnFallback(origModel, c.wanted)
			}
		}
		res, err := r.try(ctx, req, c, h, &attempts)
		if err == nil {
			if i > 0 {
				res.FallbackFrom = origModel // 降级必留痕（规则 3）
			}
			return res, nil
		}
		lastErr = err
		if !r.fallbackWorthy(kindOf(err)) {
			return nil, err // 内容类/5xx/登录态：换模型治不了本
		}
	}
	if lastErr == nil {
		return nil, &routeErr{kind: errkind.NotFound, msg: r.missHint(origModel)}
	}
	return nil, lastErr
}

// cand 一个 (渠道, 上游模型) 候选格子。
type cand struct {
	channelID string
	model     string // 发给插件的模型名
	wanted    string // 降级链里配置的模型全路径（留痕用，主候选为空）
}

// candidates 构建本次请求的候选链：主候选 + 降级链（旧版 pickModelFallback）。
//
// 降级链**只在主候选换号耗尽后**才走（成本递进：换号便宜、换模型贵）。
// 未配置规则时零开销：连容量探测都不做（边界 6）。
func (r *Router) candidates(cfg *config.Config, origModel string) []cand {
	channelID, upModel, ok := r.Resolve(origModel)
	if !ok {
		return nil // 交给 try 报 not_found
	}
	out := []cand{{channelID: channelID, model: upModel}}
	for _, to := range cfg.ModelFallback[origModel] {
		to = strings.TrimSpace(to)
		if to == "" {
			continue
		}
		cid, m, ok := r.Resolve(to)
		if !ok || (cid == channelID && m == upModel) {
			continue
		}
		// 同渠道备胎只在该渠道**对该备胎模型仍有承载能力**时才有意义：
		// 同渠道兄弟模型共享同一批账号，整渠道全灭时重打一遍必然二次撞墙。
		if cid == channelID && !r.Pool.HasCapacity(cid, m) {
			continue
		}
		out = append(out, cand{channelID: cid, model: m, wanted: to})
	}
	return out
}

// applyChannelRules 对单个候选请求施加 channel 作用域规则（Pass B）。
//
// 规则引擎的入口是 map[string]any（与 gateway 侧入站形态一致），而 GenerateRequest 是
// 结构化类型，故在此做一次 JSON 往返。仅当该渠道确实有规则时才走这里（HasChannelRules 预判），
// 正常请求零开销。往返失败（理论上不会）则原样返回，不阻断对话。
//
// 安全边界：channel_config / account_uid / extra 在规则生效前摘掉，避免字符串规则改写
// 渠道配置（如 base_url、API Key 字段）或内部调度字段（AGENTS W3-35 硬约束）。
func applyChannelRules(e *rules.Engine, req *chat.GenerateRequest, channelID string) {
	b, err := json.Marshal(req)
	if err != nil {
		return
	}
	var body map[string]any
	if err := json.Unmarshal(b, &body); err != nil {
		return
	}
	for _, k := range ruleOpaqueKeys {
		delete(body, k)
	}
	if !e.PostRoute(body, rules.Context{ChannelID: channelID}) {
		return
	}
	nb, err := json.Marshal(body)
	if err != nil {
		return
	}
	// 只回写规则可能触及的字段，避免往返把未识别字段丢掉（Extra 等保持原样）。
	var out chat.GenerateRequest
	if err := json.Unmarshal(nb, &out); err != nil {
		return
	}
	req.Messages = out.Messages
	req.Tools = out.Tools
	req.ToolChoice = out.ToolChoice
}

// ruleOpaqueKeys 规则不得读写的字段（渠道配置含凭据/基址，account_uid 是调度字段）。
var ruleOpaqueKeys = []string{"channel_config", "account_uid", "extra"}

// try 在一个候选格子上换号重试（上限 = 候选账号数，旧版已删除固定 MaxRotate）。
func (r *Router) try(ctx context.Context, req *chat.GenerateRequest, c cand, h Handler, attempts *int) (*Result, error) {
	ch := r.Store.Channel(c.channelID)
	if ch == nil {
		return nil, &routeErr{kind: errkind.NotFound, msg: "渠道不存在: " + c.channelID}
	}
	exclude := map[string]bool{}
	healed := map[string]bool{} // 401 自愈去重（每个账号只刷新一次）
	var lastErr error

	// 最多尝试「池内该渠道账号数 + 1」次：多 1 次留给 API Key 渠道（无账号池条目，
	// 首次 Pick 返回 nil 走渠道自身作为唯一账号），否则无账号渠道一次都试不上。
	limit := 1 + r.Pool.CountChannel(c.channelID)
	for attempt := 0; attempt < limit; attempt++ {
		*attempts = *attempts + 1
		// 粘性选号：同一渠道连续请求优先命中同一账号，达阈值或不可用才换（旧版 pickWithSticky）。
		ent := r.Pool.PickSticky(c.channelID, c.model, exclude)
		// API Key 渠道无账号池条目时用渠道本身作为唯一账号
		acctUID := ""
		if ent != nil {
			acctUID = ent.UID
		}

		emitted := false
		wrapped := &countHandler{h: h, emitted: &emitted}
		req2 := *req
		req2.Model = c.model
		req2.ChannelConfig = channelConfigJSON(ch)
		// Pass B：channel 作用域规则（W3-35）。从 pristine req 重建（req2 只改了 model/config），
		// 故 fallback 到下一渠道时不会累积上一个渠道的改写。
		if r.Rules != nil && r.Rules.HasChannelRules(ch.ID) {
			applyChannelRules(r.Rules, &req2, ch.ID)
		}
		// 多账号渠道（OAuth）需要知道 core 选定的是哪个逻辑账号；凭据仍由插件私有目录持有（W3-07）。
		// 发给插件的是**凭据标识**（RemoteUID），不是 core 内部主键（后者掺了渠道 ID）。
		req2.AccountUID = r.remoteUID(acctUID)

		usage, err := r.RT.ChatStream(ctx, &req2, runtime.ChatOptions{
			PluginID:  ch.PluginID,
			ChannelID: ch.ID, // 并发分组键：同一插件的不同区域实例不互相干扰
			OnEvent:   wrapped.OnEvent,
			Hooks:     r.hookSpecs(),
		})
		if err == nil {
			if acctUID != "" {
				r.Pool.NoteSuccess(acctUID, ch.ID)
			}
			return &Result{Usage: usage, ChannelID: c.channelID, AccountUID: acctUID, Attempts: *attempts}, nil
		}
		lastErr = err
		// 已发出部分响应 → 不可重放（W3-10）
		if emitted {
			return nil, err
		}
		r.Pool.ClearSticky(ch.ID) // 选中的号失败了，下次强制重选

		kind := kindOf(err)
		// 冷却/计数（按四类处置分派；只有 account-rotate 类的默认错误才累计 errCount）
		switch kind {
		case errkind.HardCredit:
			if acctUID != "" {
				// R73：配了每日发奖时刻时，硬冷却截到「下个发奖窗口+缓冲」而不跑满 12h，
				// 免得 19:07 撞 402 被锁到次日 07:07，而额度其实 00:20 就到了。
				hard := r.hardDur()
				// 单账号渠道（oczen/kilo 这类 API Key 渠道）：任何账号级惩罚都等于整条渠道
				// 下线，且无号可换 ⇒ 只冷却「号×该模型」，同号其他模型照常可用，下一次请求
				// 挑号阶段即「模型全冷」自动触发降级链（旧版 handler.go:837-860 同一例外）。
				if r.Pool.CountChannel(ch.ID) <= 1 {
					r.Pool.CooldownModel(acctUID, c.model, hard)
				} else {
					r.Pool.CooldownFor(acctUID, c.model, pool.CoolHard, hard)
				}
			}
		case errkind.SessionDead, errkind.Auth:
			if acctUID != "" {
				// 401 自愈：先刷新一次凭据再重试一次同账号（旧版 refreshIfSessionDead）。
				// 每个账号只自愈一次（healed 去重），避免 token 真死时无限循环。
				// 刷新成功要把 dead 标记清掉，否则下一次 PickSticky 仍会跳过它。
				if r.SelfHeal != nil && !healed[acctUID] {
					healed[acctUID] = true
					if r.SelfHeal(ctx, ch.ID, acctUID) {
						r.Pool.ClearSessionDead(acctUID)
						delete(exclude, acctUID) // 同号重试一次
						r.logf("401 自愈成功，重试同账号 channel=%s uid=%s model=%s", ch.ID, acctUID, c.model)
						continue
					}
				}
				r.Pool.Cooldown(acctUID, c.model, pool.CoolDead)
			}
		case errkind.SoftRate, errkind.Queued:
			// 429/排队：上游把「模型拥塞」和「账号被限」都归这一类，无法区分。
			// 只罚「号×模型」而不是整号：否则一个账号第二个模型也撞墙就会升级整号冷却，
			// 把可用容量从「1 个模型格子」缩成 0（旧版刻意接受的粒度）。
			// 排队类优先用上游报的 retry_after（旧版 Queued.Cooldown() 取 retryAfter），
			// 上游没说时退软冷却。
			if acctUID != "" {
				d := r.Pool.SoftCooldown()
				if ra := rpc.RetryAfterOf(err); ra > 0 {
					d = ra
				}
				r.Pool.CooldownModel(acctUID, c.model, d)
			}
		case errkind.ModelBlocked, errkind.WafBlock:
			// account-transparent：上游已给确定性结论，换号只会再撞一次 → 罚号但不换号。
			if acctUID != "" {
				r.Pool.CooldownModel(acctUID, c.model, time.Hour)
			}
			return nil, err
		case errkind.Server, errkind.Conn, errkind.Truncated, errkind.IdleTimeout:
			// upstream-transparent：5xx/传输层/截断与账号健康无关 → 不计错、不冷却、不换号。
			return nil, err
		case errkind.PromptTooLong, errkind.ImageInvalid, errkind.BadParams, errkind.ContentBlocked, errkind.Passthrough:
			// 请求级错误（IPC_CHAT §4.1）：上下文超限/图片非法/参数非法/内容拦截/形态拒绝，
			// 错误跟着请求体走，与账号健康无关 → 不罚号、不换号（换号必然复现）。
			// 拦截器 request.prepare 的 fail-closed 拒绝（如 EC 硬预算门）正是走 prompt_too_long。
			return nil, err
		case errkind.Client:
			// 请求内容类 4xx：错误跟着请求体走，**不计错**（否则客户端重试风暴几分钟内
			// 就能把全池账号记满冷却）；仅轮转以兼容「同模型不同账号权益差异」。
		default:
			if acctUID != "" {
				r.Pool.NoteError(acctUID)
			}
		}
		if !errkind.Rotatable(kind) {
			return nil, err
		}
		if acctUID != "" {
			exclude[acctUID] = true // 换号
		} else {
			break // 无账号池可换，交给降级链
		}
	}
	return nil, lastErr
}

// fallbackWorthy 报告该错误是否值得换模型（旧版 fallbackWorthy）。
//
// 只有额度/限流两类：内容类换模型必然复现，5xx/登录态换模型治不了本，
// WAF/无此模型是上游给的确定性结论。
func (r *Router) fallbackWorthy(k errkind.Kind) bool {
	switch k {
	case errkind.HardCredit, errkind.SoftRate, errkind.Queued:
		return true
	}
	return false
}

func nz(v, def int) int {
	if v == 0 {
		return def
	}
	return v
}

// applyCompat 应用下游兼容层（旧版 compat / reasoning_efforts 语义）：
//
//	① 模型名映射：先精确、再前缀通配（key 以 * 结尾）→ 重写为 "channel/model"
//	② 思考强度注入：仅当客户端 **未带** reasoning_effort（含空串）时，按模型名注入，'*' 为全局默认
//	③ max_tokens 封顶：仅向下压，不放大（0 = 不限）
func (r *Router) applyCompat(cfg *config.Config, req *chat.GenerateRequest) {
	if mm := cfg.Compat.ModelMap; len(mm) > 0 {
		if v, ok := mm[req.Model]; ok && v != "" {
			req.Model = v
		} else {
			for k, v := range mm {
				if v != "" && strings.HasSuffix(k, "*") && strings.HasPrefix(req.Model, strings.TrimSuffix(k, "*")) {
					req.Model = v
					break
				}
			}
		}
	}
	if req.ReasoningEffort == "" && len(cfg.ReasoningEfforts) > 0 {
		if v, ok := cfg.ReasoningEfforts[req.Model]; ok && v != "" {
			req.ReasoningEffort = v
		} else if v, ok := cfg.ReasoningEfforts["*"]; ok && v != "" {
			req.ReasoningEffort = v
		}
	}
	if capTok := cfg.Compat.MaxTokensCap; capTok > 0 && req.MaxTokens != nil && *req.MaxTokens > capTok {
		m := capTok
		req.MaxTokens = &m
	}
}

// hookSpecs 取本次请求要经过的拦截器链（W3-25）。
//
// 只纳入 Enabled 且插件已安装、且声明了 hook.stream 能力的实例——配置里残留一个已
// 卸载插件的 hook 条目不应该让全部对话挂掉（那是配置问题，不是请求问题），但会记日志。
func (r *Router) hookSpecs() []runtime.HookSpec {
	hooks := r.Store.Get().Hooks
	if len(hooks) == 0 {
		return nil
	}
	out := make([]runtime.HookSpec, 0, len(hooks))
	for _, h := range hooks {
		if h == nil || !h.Enabled || h.PluginID == "" {
			continue
		}
		p := r.RT.Plugin(h.PluginID)
		if p == nil {
			continue // 插件未安装：跳过（静默，避免每次请求刷日志）
		}
		if !p.Met.Capabilities.HasConversation("hook.stream") {
			continue // 未声明拦截器能力（例如误把渠道插件配上）
		}
		out = append(out, runtime.HookSpec{
			ID:       orDefault(h.ID, h.PluginID),
			PluginID: h.PluginID,
			Order:    h.Order,
			Config:   h.Config,
			FailOpen: h.FailOpen,
			// Prepare 从 manifest 能力声明填充：声明了 request.prepare 的层参与改写阶段。
			Prepare: p.Met.Capabilities.HasConversation("request.prepare"),
		})
	}
	return runtime.SortHooks(out)
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func channelConfigJSON(ch *config.ChannelConfig) json.RawMessage {
	if ch == nil {
		return nil
	}
	b, err := json.Marshal(ch.Config)
	if err != nil {
		return nil
	}
	return b
}

// kindOf 从错误提取 kind（复用 main/protocol/rpc.KindOf）。
func kindOf(err error) errkind.Kind {
	if k := rpc.KindOf(err); k != "" {
		return errkind.Kind(k)
	}
	return errkind.Server
}

// missHint 路由不中时的可行动错文（旧版 dev/internal/gateway/router.go 同款思路）。
//
// 「未找到模型路由: x」对人没用：看不出是前缀写错、渠道被禁用、还是该渠道从未刷过模型。
// 命名空间反查（第④级）依赖 `models/refresh`，没刷新过的渠道对新模型不路，这是最常见的错场景。
func (r *Router) missHint(model string) string {
	cfg := r.Store.Get()
	var enabled, disabled []string
	for _, ch := range cfg.Channels {
		if ch.Enabled {
			enabled = append(enabled, ch.ID)
		} else {
			disabled = append(disabled, ch.ID)
		}
	}
	join := func(ss []string) string {
		if len(ss) == 0 {
			return "（无）"
		}
		return strings.Join(ss, ", ")
	}
	if i := strings.Index(model, "/"); i > 0 {
		prefix := model[:i]
		for _, id := range disabled {
			if id == prefix {
				return fmt.Sprintf("渠道 %q 已禁用（模型 %q）；启用渠道: %s", prefix, model, join(enabled))
			}
		}
		return fmt.Sprintf("模型 %q 的渠道前缀 %q 未匹配任何启用渠道；启用渠道: %s（裸模型名可用 compat.model_map 做别名映射）",
			model, prefix, join(enabled))
	}
	if cfg.DefaultChannel != "" {
		// 兼底渠道被禁用/已删除：比「未设 default_channel」更该说清楚，否则用户看不懂为何配了兼底还 404
		for _, ch := range cfg.Channels {
			if ch.ID == cfg.DefaultChannel && !ch.Enabled {
				return fmt.Sprintf("没有启用渠道能服务模型 %q；兼底渠道 %q 已禁用；启用渠道: %s", model, cfg.DefaultChannel, join(enabled))
			}
		}
		return fmt.Sprintf("没有启用渠道能服务模型 %q；兼底渠道 %q 不存在或未安装；启用渠道: %s", model, cfg.DefaultChannel, join(enabled))
	}
	hint := fmt.Sprintf("没有启用渠道能服务模型 %q；启用渠道: %s", model, join(enabled))
	if len(enabled) > 0 {
		hint += "（带命名空间的模型需先对该渠道跑一次 models/refresh，路由靠已刷新清单反查）"
	}
	return hint + "；也未设置 default_channel 兼底"
}

// routeErr 携带 kind 的路由错误（如模型未找到 → not_found/model 作用域）。
type routeErr struct {
	kind errkind.Kind
	msg  string
}

func (e *routeErr) Error() string { return e.msg }

// Kind 实现分类接口（供 rpc.KindOf 提取）。
func (e *routeErr) Kind() string { return e.kind }

// countHandler 记录是否已发出响应事件（不可逆边界判定）。
type countHandler struct {
	h       Handler
	emitted *bool
}

func (c *countHandler) OnEvent(name string, data json.RawMessage) error {
	switch name {
	case "text.delta", "reasoning.delta", "tool_call.delta":
		*c.emitted = true
	}
	if c.h == nil {
		return nil
	}
	return c.h.OnEvent(name, data)
}

// remoteUID 把 core 内部账号主键（`<凭据标识>@<渠道ID>`）换回插件侧标识。
//
// 插件只认 auths/ 里的凭据标识（`--auth` 参数 / `account_uid`）；内部主键掺了
// 渠道 ID 用于 core 侧去重，回传插件时一律剥掉（PLUGIN_CLI §5.2）。
func (r *Router) remoteUID(key string) string {
	if key == "" {
		return ""
	}
	for _, a := range r.Store.Get().Accounts {
		if a.UID == key {
			if a.RemoteUID != "" {
				return a.RemoteUID
			}
			break
		}
	}
	// 没有账号表条目（API Key 渠道把渠道 ID 当账号）：`<id>@<id>` → `<id>`
	if i := strings.Index(key, "@"); i > 0 {
		return key[:i]
	}
	return key
}
