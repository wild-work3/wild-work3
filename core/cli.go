package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"wild-work3/main/protocol/chat"
	"wild-work3/main/protocol/cli"
)

// stdioArgs 拼 spawn 自身 stdio 子进程的参数。**所有影响运行态的 CLI 开关都必须透传**：
// 父进程只负责握手与转发，真正跑路由/hook 链的是子进程（W3-18「不得存在 CLI 专用捷径」）。
// 早期只传 --data/--plugins，导致 `chat --hooks none` 的父进程解析了参数而子进程照旧走
// hook 链——EC 压缩子进程递归触发压缩正是要避的事，静默失效比报错难查。
func stdioArgs(a *application) []string {
	args := []string{"stdio", "--data", a.dataDir, "--plugins", a.pluginsDir}
	if a.configFile != "" {
		args = append(args, "--config", a.configFile)
	}
	if a.hooks != "" {
		args = append(args, "--hooks", a.hooks)
	}
	return args
}

// runChat 命令行直接对话：spawn 自身 `stdio` 命令，走与 gateway 完全相同的 IPC 路径。
// 目的：core 的会话面链路可以脱离 gateway 单独验证（正文实时打到 stdout）。
func runChat(a *application, model, prompt string) {
	if model == "" {
		// 未指定模型时取第一个启用渠道的第一个模型，避免必须手输
		if err := a.init(); err != nil {
			fatal("%v", err)
		}
		cfg := a.store.Get()
		for _, ch := range cfg.Channels {
			if ch.Enabled && len(ch.Models) > 0 {
				model = ch.Models[0]
				break
			}
		}
		if model == "" {
			fatal("没有可用模型：请在渠道配置里填 models 或用 --model 指定")
		}
		a.led.Close()
	}

	req := chat.GenerateRequest{
		Model:          model,
		Stream:         true,
		Messages:       []chat.ChatMessage{{Role: "user", Content: prompt}},
		ConversationID: "core-cli",
	}
	exe, err := os.Executable()
	if err != nil {
		fatal("定位自身可执行文件失败: %v", err)
	}
	p, err := cli.Spawn("core-stdio", exe, stdioArgs(a), a.log)
	if err != nil {
		fatal("启动 core stdio 子进程失败: %v", err)
	}
	defer p.Close()
	p.Out = os.Stdout
	if err := p.Handshake(context.Background()); err != nil {
		fatal("握手失败: %v", err)
	}
	res, err := p.Invoke(context.Background(), cli.Step{Method: "chat.generate", Params: req, Stream: true})
	if err != nil {
		fatal("chat.generate 失败: %v", err)
	}
	fmt.Fprintf(os.Stderr, "\n[结果] ok=%v finish=%s tokens=%d/%d 耗时=%dms 首字=%dms\n",
		res.OK, res.Finish, res.UsageInt("prompt_tokens"), res.UsageInt("completion_tokens"),
		res.UsageInt("time_ms"), res.UsageInt("first_token_ms"))
}

// runModels 列出全部渠道模型（配置 + 插件控制面 models.list 结果）。
func runModels(a *application) {
	if err := a.init(); err != nil {
		fatal("%v", err)
	}
	defer a.led.Close()
	out := map[string]any{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, ch := range a.store.Get().Channels {
		item := map[string]any{"enabled": ch.Enabled, "plugin": ch.PluginID, "configured": ch.Models}
		if ch.PluginID != "" {
			if res, err := a.rt.Call(ctx, ch.PluginID, "models.list",
				map[string]any{"config": ch.Config}); err == nil && len(res.Result) > 0 {
				var v struct {
					Models []struct {
						ID string `json:"id"`
					} `json:"models"`
				}
				if json.Unmarshal(res.Result, &v) == nil {
					ids := make([]string, 0, len(v.Models))
					for _, m := range v.Models {
						ids = append(ids, m.ID)
					}
					item["upstream"] = ids
				}
			} else if err != nil {
				item["error"] = err.Error()
			}
		}
		out[ch.ID] = item
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
}

// runChannels 打印渠道配置（密钥脱敏）。
func runChannels(a *application) {
	if err := a.init(); err != nil {
		fatal("%v", err)
	}
	defer a.led.Close()
	cfg := a.store.Get()
	list := make([]map[string]any, 0, len(cfg.Channels))
	for _, ch := range cfg.Channels {
		list = append(list, map[string]any{
			"channel_id": ch.ID, "plugin": ch.PluginID, "name": ch.DisplayName,
			"enabled": ch.Enabled, "models": ch.Models, "config": maskMap(ch.Config),
		})
	}
	b, _ := json.MarshalIndent(map[string]any{
		"listen": cfg.Listen, "default_channel": cfg.DefaultChannel, "channels": list,
	}, "", "  ")
	fmt.Println(string(b))
}

// runUsage 打印近 N 天用量汇总（读 stat 记账）。
func runUsage(a *application, days int) {
	if err := a.init(); err != nil {
		fatal("%v", err)
	}
	defer a.led.Close()
	recs := a.led.QueryUsage(days)
	var prompt, completion, total, cached, reqs, ok, fail int
	byModel := map[string]int{}
	for _, r := range recs {
		reqs++
		prompt += r.PromptTokens
		completion += r.CompletionTokens
		total += r.TotalTokens
		cached += r.CachedTokens
		byModel[r.Model]++
		if r.OK {
			ok++
		} else {
			fail++
		}
	}
	keys := make([]string, 0, len(byModel))
	for k := range byModel {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	by := make([]string, 0, len(keys))
	for _, k := range keys {
		by = append(by, fmt.Sprintf("%s=%d", k, byModel[k]))
	}
	b, _ := json.MarshalIndent(map[string]any{
		"days": days, "requests": reqs, "ok": ok, "error": fail,
		"prompt_tokens": prompt, "completion_tokens": completion, "total_tokens": total,
		"cached_tokens": cached, "by_model": by,
	}, "", "  ")
	fmt.Println(string(b))
}

// maskMap 复制并脱敏（只保留末 4 位）。
func maskMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		if s, ok := v.(string); ok && isSecretKey(k) && len(s) > 8 {
			out[k] = "***" + s[len(s)-4:]
			continue
		}
		out[k] = v
	}
	return out
}

func isSecretKey(k string) bool {
	l := strings.ToLower(k)
	return strings.Contains(l, "key") || strings.Contains(l, "token") || strings.Contains(l, "secret") || strings.Contains(l, "password")
}
