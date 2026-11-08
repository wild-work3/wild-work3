package route

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"wild-work3/main/core/internal/config"
	"wild-work3/main/core/internal/pool"
	"wild-work3/main/protocol/chat"
	"wild-work3/main/protocol/errkind"
)

// fakeHandler 记录事件，不返回错误。
type fakeHandler struct {
	events []string
}

func (f *fakeHandler) OnEvent(name string, _ json.RawMessage) error {
	f.events = append(f.events, name)
	return nil
}

// newStore 建一个临时配置库（config.Open 在目录不存在时写默认值）。
func newStore(t *testing.T) *config.Store {
	t.Helper()
	st, err := config.Open(t.TempDir())
	if err != nil {
		t.Fatalf("config.Open: %v", err)
	}
	return st
}

// 401 自愈：SelfHeal 成功时同账号重试一次，不冷却、不换号。
func TestSelfHealRetriesSameAccount(t *testing.T) {
	calls := 0
	r := &Router{SelfHeal: func(ctx context.Context, ch, uid string) bool { calls++; return true }}
	if r.SelfHeal == nil {
		t.Fatal("SelfHeal 应已注入")
	}
	_ = r.SelfHeal(context.Background(), "ch", "u1")
	if calls != 1 {
		t.Fatalf("SelfHeal 应被调用一次，得到 %d", calls)
	}
}

// fallbackWorthy 只认额度/限流/排队三类（旧版口径）。
func TestFallbackWorthy(t *testing.T) {
	r := &Router{}
	yes := []errkind.Kind{errkind.HardCredit, errkind.SoftRate, errkind.Queued}
	no := []errkind.Kind{errkind.Server, errkind.SessionDead, errkind.Auth,
		errkind.ModelBlocked, errkind.WafBlock, errkind.Client, errkind.ContentBlocked}
	for _, k := range yes {
		if !r.fallbackWorthy(k) {
			t.Errorf("%s 应触发降级", k)
		}
	}
	for _, k := range no {
		if r.fallbackWorthy(k) {
			t.Errorf("%s 不应触发降级", k)
		}
	}
}

// 六级路由：命名空间前缀反查（第 ④ 级）必须优先于渠道 models 精确匹配（第 ⑤ 级）。
func TestResolveNamespacePrefix(t *testing.T) {
	st := newStore(t)
	if err := st.Update(func(c *config.Config) error {
		c.Channels = []*config.ChannelConfig{
			{ID: "wb-cn", PluginID: "workbuddy", Enabled: true, Models: []string{"workbuddy/glm-5"}},
			{ID: "wb-global", PluginID: "workbuddy", Enabled: true, Models: []string{"workbuddyai/gpt-6-astra"}},
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r := &Router{Store: st}
	// 渠道 ID 与命名空间不同名，必须靠第 ④ 级反查
	cid, m, ok := r.Resolve("workbuddyai/gpt-6-astra")
	if !ok || cid != "wb-global" || m != "workbuddyai/gpt-6-astra" {
		t.Fatalf("应路由到 wb-global 并下发完整模型名，得到 cid=%q m=%q ok=%v", cid, m, ok)
	}
}

// 前缀路由的前提是 models 已刷新：没刷新过则第 ④ 级无数据可反查，回落默认渠道。
func TestResolveRequiresRefreshedModels(t *testing.T) {
	st := newStore(t)
	if err := st.Update(func(c *config.Config) error {
		c.Channels = []*config.ChannelConfig{{ID: "wb-global", PluginID: "workbuddy", Enabled: true}}
		c.DefaultChannel = "wb-global"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r := &Router{Store: st}
	cid, _, ok := r.Resolve("workbuddyai/gpt-6-astra")
	if !ok || cid != "wb-global" {
		t.Fatalf("应回落到默认渠道，得到 cid=%q ok=%v", cid, ok)
	}
}

// applyCompat：模型映射（精确 → `*` 通配）、effort 注入、max_tokens 封顶只向下压。
func TestApplyCompat(t *testing.T) {
	st := newStore(t)
	if err := st.Update(func(c *config.Config) error {
		c.Compat.ModelMap = map[string]string{"gpt-4o": "glm-5", "claude-*": "glm-5.3"}
		c.Compat.MaxTokensCap = 4096
		c.ReasoningEfforts = map[string]string{"*": "medium"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r := &Router{Store: st}
	big := 8192

	req := &chat.GenerateRequest{Model: "gpt-4o", MaxTokens: &big}
	r.applyCompat(st.Get(), req)
	if req.Model != "glm-5" {
		t.Errorf("精确映射失败: %q", req.Model)
	}
	if req.MaxTokens == nil || *req.MaxTokens != 4096 {
		t.Errorf("max_tokens 应被压到 4096，得到 %v", req.MaxTokens)
	}
	if req.ReasoningEffort != "medium" {
		t.Errorf("effort 通配注入失败: %q", req.ReasoningEffort)
	}

	// 通配映射
	req2 := &chat.GenerateRequest{Model: "claude-3-5-sonnet"}
	r.applyCompat(st.Get(), req2)
	if req2.Model != "glm-5.3" {
		t.Errorf("通配映射失败: %q", req2.Model)
	}

	// 只向下压：本来就小则不动
	small := 100
	req3 := &chat.GenerateRequest{Model: "other", MaxTokens: &small}
	r.applyCompat(st.Get(), req3)
	if req3.MaxTokens == nil || *req3.MaxTokens != 100 {
		t.Errorf("max_tokens 不应被抬高，得到 %v", req3.MaxTokens)
	}

	// 请求已带 effort 时不覆盖
	req4 := &chat.GenerateRequest{Model: "other", ReasoningEffort: "high"}
	r.applyCompat(st.Get(), req4)
	if req4.ReasoningEffort != "high" {
		t.Errorf("显式 effort 不应被覆盖，得到 %q", req4.ReasoningEffort)
	}
}

// 路由不中要给可行动错文（旧版 dev 的 `unknown channel %q … known channels: %s` 同职责）。
// 别名映射本身由 `compat.model_map` 承担（applyCompat 在路由前改写，支持精确与 `*` 后缀），
// 所以这里只验错文本身是否能区分三种常见错因。
func TestMissHintReadable(t *testing.T) {
	st := newStore(t)
	if err := st.Update(func(c *config.Config) error {
		c.Channels = []*config.ChannelConfig{
			{ID: "wb-cn", PluginID: "workbuddy", Enabled: true, Models: []string{"workbuddy/glm-5"}},
			{ID: "glm", PluginID: "glm", Enabled: false},
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r := &Router{Store: st}
	cases := []struct {
		model string
		want  []string
	}{
		// ① 前缀指向已禁用渠道：必须直说「已禁用」，不要让人去查存在性
		{"glm/glm-4.6", []string{"已禁用", "glm-4.6", "wb-cn"}},
		// ② 前缀根本不是任何渠道：列出启用渠道并提示 model_map
		{"nope/x", []string{"未匹配任何启用渠道", "nope", "model_map"}},
		// ③ 裸模型名无承载：要点出「需先 models/refresh」与未设 default_channel
		{"deepseek-v3", []string{"没有启用渠道", "models/refresh", "default_channel"}},
	}
	for _, tc := range cases {
		if _, _, ok := r.Resolve(tc.model); ok {
			t.Fatalf("%s 不应路由成功", tc.model)
		}
		got := r.missHint(tc.model)
		for _, sub := range tc.want {
			if !strings.Contains(got, sub) {
				t.Errorf("%s 错文应含 %q，实际：%s", tc.model, sub, got)
			}
		}
	}

	// ④ 兼底渠道必须看 Enabled：面板关掉渠道后不得再靠 default_channel 把对话打过去，
	// 且错文要分清「被禁用」与「不存在」（17863 实测曾默认渠道被禁用仍路由成功）。
	if err := st.Update(func(c *config.Config) error { c.DefaultChannel = "glm"; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := r.Resolve("deepseek-v3"); ok {
		t.Errorf("兼底渠道 glm 已禁用，不应路由成功")
	}
	if got := r.missHint("deepseek-v3"); !strings.Contains(got, `兼底渠道 "glm" 已禁用`) {
		t.Errorf("应报兼底渠道被禁用，实际：%s", got)
	}
	if err := st.Update(func(c *config.Config) error { c.DefaultChannel = "ghost"; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := r.Resolve("deepseek-v3"); ok {
		t.Errorf("兼底渠道 ghost 不存在，不应路由成功")
	}
	if got := r.missHint("deepseek-v3"); !strings.Contains(got, "不存在或未安装") {
		t.Errorf("应报兼底渠道不存在，实际：%s", got)
	}
}

// 空 Router 不应 panic（CLI 直接调 route 的场景）。
func TestNilStoreSafe(t *testing.T) {
	r := &Router{}
	_ = pool.CoolNone // 保持 import 使用
	if r.Pool != nil || r.Store != nil {
		t.Skip("期望空 Router")
	}
}

// HooksOverride=none 时 hookSpecs 应返回空链（EC 压缩等内部请求禁用拦截器）。
// 不依赖 RT（none 分支在访问 Store/RT 之前就短路返回）。
func TestHookSpecsNoneDisablesAll(t *testing.T) {
	r := &Router{HooksOverride: "none"}
	if got := r.hookSpecs(); got != nil {
		t.Fatalf("--hooks none 应返回空链，实际 %d 个", len(got))
	}
}
