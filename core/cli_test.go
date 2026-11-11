package main

import (
	"strings"
	"testing"
)

// TestStdioArgsPassthrough 锁 W3-18「不得存在 CLI 专用捷径」：
// 影响运行态的开关（--config / --hooks）必须透传给 spawn 出来的 stdio 子进程，
// 否则父进程解析了参数、真正跑 hook 链的子进程照旧走默认配置（`--hooks none` 静默失效）。
func TestStdioArgsPassthrough(t *testing.T) {
	a := &application{dataDir: "/d", pluginsDir: "/p", configFile: "/d/cfg.json", hooks: "none"}
	got := strings.Join(stdioArgs(a), " ")
	for _, want := range []string{"stdio", "--data /d", "--plugins /p", "--config /d/cfg.json", "--hooks none"} {
		if !strings.Contains(got, want) {
			t.Fatalf("stdioArgs 缺 %q：%s", want, got)
		}
	}

	// 未指定时不得出现（空串透传会让子进程报「--config 需要参数」这类假错）
	b := &application{dataDir: "/d", pluginsDir: "/p"}
	got2 := strings.Join(stdioArgs(b), " ")
	if strings.Contains(got2, "--config") || strings.Contains(got2, "--hooks") {
		t.Fatalf("空值不应透传：%s", got2)
	}
}
