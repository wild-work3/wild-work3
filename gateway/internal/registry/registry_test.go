// 注册表 v2 的单元测试：注册文件解析（含单文件失败分级）、tarball 枚举、
// 版本选择、版本门、allowlist、竞速取文件、条件请求。
package registry

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestSourceURLs(t *testing.T) {
	s := Source{Repo: "wild-work3/ww3-ext-registry"}
	if got := s.TarballURL(Options{}); got != "https://codeload.github.com/wild-work3/ww3-ext-registry/tar.gz/refs/heads/main" {
		t.Fatalf("tarball URL 错: %s", got)
	}
	s.Ref = "dev"
	if got := s.TarballURL(Options{}); !strings.Contains(got, "/refs/heads/dev") {
		t.Fatalf("ref 没生效: %s", got)
	}
	srcs := s.URLs(Options{Proxies: []string{"https://gh-proxy.com", "https://ghfast.top/", " ", "not-a-url"}})
	if len(srcs) != 3 {
		t.Fatalf("应展开成 1 直连 + 2 代理，实得 %d: %v", len(srcs), srcs)
	}
	if !strings.HasPrefix(srcs[1], "https://gh-proxy.com/https://codeload.github.com/") {
		t.Fatalf("代理前缀拼接错: %s", srcs[1])
	}
	// 绝对 URL 形态（离线夹具）优先于 repo
	u := Source{Repo: "a/b", URL: "http://127.0.0.1:1/t.tgz"}
	if got := u.TarballURL(Options{}); got != "http://127.0.0.1:1/t.tgz" {
		t.Fatalf("URL 形态应直用: %s", got)
	}
}

func TestParseFileHardChecks(t *testing.T) {
	sha := strings.Repeat("ab", 32)
	good := fmt.Sprintf(`{"schema":2,"id":"demo","type":"channel","name":"演示","repo":"o/r",
		"versions":[{"version":"1.0.0","file":"demo-1.0.0.zip","sha256":"%s"}]}`, sha)
	p, warns, err := ParseFile("demo.json", []byte(good))
	if err != nil {
		t.Fatalf("合法注册文件被拒: %v", err)
	}
	if p.ID != "demo" || p.Type != TypeChannel || len(p.Versions) != 1 || len(warns) != 0 {
		t.Fatalf("解析结果不对: %+v warns=%v", p, warns)
	}

	cases := []struct{ name, file, body, want string }{
		{"schema 不认", "demo.json", `{"schema":9,"id":"demo","type":"channel","name":"x","repo":"o/r"}`, "schema"},
		{"id 与文件名不符", "other.json", good, "文件名"},
		{"type 非法", "demo.json", `{"schema":2,"id":"demo","type":"weird","name":"x","repo":"o/r"}`, "type"},
		{"缺 name", "demo.json", `{"schema":2,"id":"demo","type":"channel","repo":"o/r"}`, "name"},
		{"repo 非 GitHub", "demo.json", `{"schema":2,"id":"demo","type":"channel","name":"x","repo":"https://gitlab.com/o/r"}`, "GitHub"},
		{"缺 repo", "demo.json", `{"schema":2,"id":"demo","type":"channel","name":"x"}`, "repo"},
		{"坏 JSON", "demo.json", `{`, "解析"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := ParseFile(c.file, []byte(c.body))
			if err == nil {
				t.Fatalf("本该拒: %s", c.body)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("错误文案缺 %q: %v", c.want, err)
			}
		})
	}

	// 版本级问题只丢该条并进 warns（不拖垮整个文件）
	mixed := fmt.Sprintf(`{"schema":2,"id":"demo","type":"channel","name":"x","repo":"o/r","versions":[
		{"version":"1.0.0","file":"demo-1.0.0.zip","sha256":"%s"},
		{"version":"1.1.0"},
		{"version":"1.2.0","file":"demo-1.2.0.zip","sha256":"deadbeef"},
		{"sha256":"%s"}]}`, sha, sha)
	p2, warns2, err := ParseFile("demo.json", []byte(mixed))
	if err != nil {
		t.Fatalf("单条坏版本不该拖垮文件: %v", err)
	}
	if len(p2.Versions) != 1 || p2.Versions[0].Version != "1.0.0" {
		t.Fatalf("应只留合法版本: %+v", p2.Versions)
	}
	if len(warns2) != 3 {
		t.Fatalf("三条坏版本都应进 warns: %v", warns2)
	}
	for _, w := range warns2 {
		if w.File != "demo.json" || w.Reason == "" {
			t.Errorf("warn 形状不对: %+v", w)
		}
	}
}

func TestParseFileMultiPlatform(t *testing.T) {
	sha := strings.Repeat("ab", 32)
	// 同一个版本号为 6 个平台各出一条：这是常态，不能当「重复声明」只留第一条（W3-55）
	plats := [][2]string{{"windows", "amd64"}, {"windows", "arm64"}, {"linux", "amd64"},
		{"linux", "arm64"}, {"darwin", "amd64"}, {"darwin", "arm64"}}
	var vs []string
	for _, p := range plats {
		vs = append(vs, fmt.Sprintf(`{"version":"1.0.0","file":"w-1.0.0-%s-%s.zip","sha256":"%s","os":"%s","arch":"%s"}`,
			p[0], p[1], sha, p[0], p[1]))
	}
	body := fmt.Sprintf(`{"schema":2,"id":"w","type":"channel","name":"x","repo":"o/r","versions":[%s]}`,
		strings.Join(vs, ","))
	p, warns, err := ParseFile("w.json", []byte(body))
	if err != nil {
		t.Fatalf("6 个平台的同版本被拒: %v", err)
	}
	if len(p.Versions) != 6 {
		t.Fatalf("应保留 6 条平台版本，实际 %d：%+v", len(p.Versions), p.Versions)
	}
	if len(warns) != 0 {
		t.Fatalf("同一版本不同平台不该产生 warn: %v", warns)
	}
	// 真重复 = 同 version + 同 os + 同 arch，只留头一条并记 warn
	dup := fmt.Sprintf(`{"schema":2,"id":"w","type":"channel","name":"x","repo":"o/r","versions":[
		{"version":"1.0.0","file":"a.zip","sha256":"%s","os":"linux","arch":"amd64"},
		{"version":"1.0.0","file":"b.zip","sha256":"%s","os":"linux","arch":"amd64"}]}`, sha, sha)
	p2, warns2, err := ParseFile("w.json", []byte(dup))
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.Versions) != 1 || p2.Versions[0].File != "a.zip" {
		t.Fatalf("同平台重复应只留前一条: %+v", p2.Versions)
	}
	if len(warns2) != 1 || !strings.Contains(warns2[0].Reason, "重复声明") {
		t.Fatalf("同平台重复应进 warns: %v", warns2)
	}
	// Key() 区分空 os/arch（通用条目）与显式平台
	if (VersionEntry{Version: "1", OS: "linux"}).Key() == (VersionEntry{Version: "1"}).Key() {
		t.Fatal("Key() 未区分平台")
	}
}

func TestPickVersion(t *testing.T) {
	p := PluginEntry{ID: "d", Versions: []VersionEntry{
		{Version: "1.0.0", SHA256: strings.Repeat("a", 64)},
		{Version: "1.10.0", SHA256: strings.Repeat("b", 64)},
		{Version: "2.0.0", SHA256: strings.Repeat("c", 64), OS: "plan9"},
	}}
	v, ok := p.Pick("")
	if !ok || v.Version != "1.10.0" {
		t.Fatalf("应按平台过滤后取最高（2.0.0 是别的平台）: %+v", v)
	}
	if v, ok := p.Pick("1.0.0"); !ok || v.Version != "1.0.0" {
		t.Fatalf("精确选择失败: %+v", v)
	}
	// 同一版本号为每个平台各一条时，精确选择也必须按当前平台过滤（全平台发布的前提）
	multi := PluginEntry{ID: "m", Versions: []VersionEntry{
		{Version: "1.0.0", File: "plan9.zip", SHA256: strings.Repeat("a", 64), OS: "plan9", Arch: "amd64"},
		{Version: "1.0.0", File: "cur.zip", SHA256: strings.Repeat("b", 64), OS: runtime.GOOS, Arch: runtime.GOARCH},
	}}
	if v, ok := multi.Pick("1.0.0"); !ok || v.File != "cur.zip" {
		t.Fatalf("同版本多平台时精确选择取错了平台: %+v", v)
	}
	if _, ok := p.Pick("9.9.9"); ok {
		t.Fatal("不存在的版本不该命中")
	}
	if !p.Installable() {
		t.Fatal("有当前平台版本应可安装")
	}
	onlyOther := PluginEntry{Versions: []VersionEntry{{Version: "1", OS: "plan9"}}}
	if onlyOther.Installable() {
		t.Fatal("只有别的平台时不可安装")
	}
	if p.ExeName() != "d-ext.exe" && p.ExeName() != "d-ext" {
		t.Fatalf("exe 名不对: %s", p.ExeName())
	}
	hook := PluginEntry{ID: "h", Type: TypeHook}
	if !strings.Contains(hook.ExeName(), "h-hook") {
		t.Fatalf("hook 的 exe 名不对: %s", hook.ExeName())
	}
}

func TestCheckFileName(t *testing.T) {
	// file 会被直接拼进下载 URL（repo + version + file），所以只收纯文件名。
	for _, ok := range []string{"workbuddy-1.4.2.zip", "demo.zip", "A-1.0.ZIP"} {
		if err := checkFileName(ok); err != nil {
			t.Fatalf("%q 应合法: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "  ", " demo.zip", "demo.zip ", "../demo.zip", "sub/demo.zip",
		`sub\demo.zip`, ".", "..", "demo.tar.gz", "demo"} {
		if err := checkFileName(bad); err == nil {
			t.Fatalf("%q 应被拒", bad)
		}
	}
}

func TestLocateReleaseNeedsFile(t *testing.T) {
	// 缺 file 时不该去猜资产列表（早期实现会拉 expanded_assets 再按平台关键字挑）。
	_, err := LocateRelease(context.Background(), "o/r", VersionEntry{Version: "1.0.0"}, Options{}, &releaseCache{})
	if err == nil || !strings.Contains(err.Error(), "缺 file 字段") {
		t.Fatalf("缺 file 应报错，实得 %v", err)
	}
}

func TestMatchTag(t *testing.T) {
	tags := []string{"v1.10.0", "release-1.9.0", "jq-1.7.1", "v2.0.0-rc1"}
	for _, c := range []struct{ ver, want string }{{"1.9.0", "release-1.9.0"}, {"1.7.1", "jq-1.7.1"},
		{"1.10.0", "v1.10.0"}} {
		got, ok := matchTag(tags, c.ver)
		if !ok || got != c.want {
			t.Fatalf("tag 匹配 %s 应得 %s，实得 %q ok=%v", c.ver, c.want, got, ok)
		}
	}
	if _, ok := matchTag(tags, "2.0.0"); ok {
		t.Fatal("预发布 tag 不该匹配正式版 2.0.0")
	}
	if got, ok := matchTag(tags, "2.0.0-rc1"); !ok || got != "v2.0.0-rc1" {
		t.Fatalf("预发布版应匹配到 rc tag: %q %v", got, ok)
	}
	if _, ok := matchTag(tags, "3.0.0"); ok {
		t.Fatal("不存在的版本不该命中")
	}
}

func TestCheckGate(t *testing.T) {
	p := PluginEntry{ID: "a", Versions: []VersionEntry{{Version: "1", Protocol: ProtocolRng{Min: 1, Max: 2},
		Gate: VersionGate{Min: "1.0.0", Max: "1.9.0"}}}}
	v := p.Versions[0]
	if err := CheckGate(p, v, 1, "1.2.0"); err != nil {
		t.Fatalf("区间内应通过: %v", err)
	}
	if err := CheckGate(p, v, 3, "1.2.0"); err == nil || !strings.Contains(err.Error(), "≤2") {
		t.Fatalf("协议超上限应拒: %v", err)
	}
	if err := CheckGate(p, v, 1, "0.9.0"); err == nil || !strings.Contains(err.Error(), "gateway ≥1.0.0") {
		t.Fatalf("gateway 低于下限应拒: %v", err)
	}
	if err := CheckGate(p, v, 1, "2.0.0"); err == nil {
		t.Fatal("gateway 超上限应拒")
	}
	for _, c := range [][3]any{{"1.10.0", "1.9.0", 1}, {"1.0.0", "v1.0", 0}, {"1.0.1", "1.0.2", -1}} {
		if got := CmpSemver(c[0].(string), c[1].(string)); got != c[2].(int) {
			t.Fatalf("CmpSemver(%s,%s)=%d 期望 %d", c[0], c[1], got, c[2].(int))
		}
	}
}

func TestAllowHosts(t *testing.T) {
	h := AllowHosts([]string{"https://gh-proxy.com/", "https://ghfast.top"})
	for _, want := range []string{"github.com", "codeload.github.com", "release-assets.githubusercontent.com", "gh-proxy.com", "ghfast.top"} {
		if !h[want] {
			t.Fatalf("allowlist 缺 %s: %v", want, h)
		}
	}
	if h["api.github.com"] {
		t.Fatal("api.github.com 不该在表内（v2 一律不用它）")
	}
	if h["evil.com"] {
		t.Fatal("allowlist 不应包含未声明主机")
	}
	if !AllowHosts(nil, "127.0.0.1")["127.0.0.1"] {
		t.Fatal("extra 主机（测试用）应生效")
	}
}

// tarGz 打一个注册表仓形状的 tarball（顶层目录 + 平铺 .json）。
func tarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	add := func(name, body string) {
		h := &tar.Header{Name: name, Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	// 顶层目录条目（tarball 的真实形状是 <repo>-<ref>/）
	if err := tw.WriteHeader(&tar.Header{Name: "repo-main", Mode: 0o755, Typeflag: tar.TypeDir}); err != nil {
		t.Fatal(err)
	}
	for name, body := range files {
		add("repo-main/"+name, body)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// tarGzFlat 打一个**平铺** tarball（文件名直接写在根，不带顶层目录）。
func tarGzFlat(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, body := range files {
		h := &tar.Header{Name: "./" + name, Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestParseTarball(t *testing.T) {
	sha := strings.Repeat("ab", 32)
	good := func(id, typ string) string {
		return fmt.Sprintf(`{"schema":2,"id":"%s","type":"%s","name":"%s","repo":"o/%s",
			"versions":[{"version":"1.0.0","file":"demo-1.0.0.zip","sha256":"%s"}]}`, id, typ, id, id, sha)
	}
	body := tarGz(t, map[string]string{
		"demo.json":     good("demo", "channel"),
		"hooked.json":   good("hooked", "hook"),
		"main.json":     good("main", "main"),
		"Bad_Name.json": good("bad", "channel"),    // 命名不合规 → 静默忽略
		"nested/x.json": good("nested", "channel"), // 非顶层 → 忽略
		"broken.json":   `{`,                       // JSON 错 → errors
		"mismatch.json": good("other", "channel"),  // id 与文件名不符 → errors
		"README.md":     "不是注册文件",                  // 非 json → 忽略
	})
	snap, err := ParseTarball(body)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if len(snap.Plugins) != 2 {
		t.Fatalf("应解析出 2 个插件: %+v", snap.Plugins)
	}
	if snap.Main == nil || snap.Main.ID != MainID {
		t.Fatalf("main.json 未识别: %+v", snap.Main)
	}
	if len(snap.Errors) != 2 {
		t.Fatalf("两个坏文件应进 errors: %+v", snap.Errors)
	}
	if p, ok := snap.PluginByID("demo"); !ok || p.ID != "demo" {
		t.Fatalf("PluginByID 失败: %+v", p)
	}
	if _, ok := snap.PluginByID("main"); !ok {
		t.Fatal("PluginByID 应能取到 main")
	}

	// 非 gzip（连到了 HTML 错误页）必须报错而不是静默空清单
	if _, err := ParseTarball([]byte("<html>429</html>")); err == nil {
		t.Fatal("非 gzip 必须报错")
	}

	// 平铺形态（手工 `tar -czf x.tgz .`，没有顶层目录）也必须能读，
	// 否则换个打包方式整份清单就空（自建本地注册表仓实测踩过）。
	flat := tarGzFlat(t, map[string]string{"demo.json": good("demo", "channel"),
		"main.json": good("main", "main")})
	fs, err := ParseTarball(flat)
	if err != nil {
		t.Fatalf("平铺 tarball 解析失败: %v", err)
	}
	if len(fs.Plugins) != 1 || fs.Main == nil {
		t.Fatalf("平铺 tarball 应解析出 1 插件 + main: %d %v", len(fs.Plugins), fs.Main)
	}
}

func TestLoadConditional(t *testing.T) {
	tarball := tarGz(t, map[string]string{
		"demo.json": fmt.Sprintf(`{"schema":2,"id":"demo","type":"channel","name":"d","repo":"o/r",
			"versions":[{"version":"1.0.0","file":"demo-1.0.0.zip","sha256":"%s"}]}`, strings.Repeat("ab", 32)),
	})
	const etag = `W/"abc123"`
	var cond []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cond = append(cond, r.Header.Get("If-None-Match"))
		if r.Header.Get("If-None-Match") == etag {
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
		_, _ = w.Write(tarball)
	}))
	defer srv.Close()

	src := Source{URL: srv.URL + "/t.tgz"}
	o := Options{Total: 5 * time.Second}
	snap, err := Load(context.Background(), src, o, nil)
	if err != nil {
		t.Fatalf("首次拉取失败: %v", err)
	}
	if len(snap.Plugins) != 1 || snap.ETags[srv.URL+"/t.tgz"] != etag {
		t.Fatalf("快照/ETag 不对: %+v", snap)
	}
	firstAt := snap.FetchedAt

	// 第二次带 If-None-Match → 304 → 复用旧快照（只刷新时间与来源）
	time.Sleep(5 * time.Millisecond)
	snap2, err := Load(context.Background(), src, o, snap)
	if err != nil {
		t.Fatalf("条件请求失败: %v", err)
	}
	if len(snap2.Plugins) != 1 {
		t.Fatalf("304 应复用旧内容: %+v", snap2)
	}
	if !snap2.FetchedAt.After(firstAt) {
		t.Errorf("304 后应刷新 fetched_at")
	}
	if cond[len(cond)-1] != etag {
		t.Errorf("第二次必须带 If-None-Match，实得 %q", cond[len(cond)-1])
	}
}

// Cache：TTL 内不重拉；失败保留旧快照 + 负缓存。
func TestCacheTTLAndNegative(t *testing.T) {
	tarball := tarGz(t, map[string]string{
		"demo.json": fmt.Sprintf(`{"schema":2,"id":"demo","type":"channel","name":"d","repo":"o/r",
			"versions":[{"version":"1.0.0","file":"demo-1.0.0.zip","sha256":"%s"}]}`, strings.Repeat("ab", 32)),
	})
	var hits int
	ok := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if !ok {
			http.Error(w, "boom", http.StatusBadGateway)
			return
		}
		_, _ = w.Write(tarball)
	}))
	defer srv.Close()

	src := Source{URL: srv.URL + "/t.tgz"}
	o := Options{Total: 5 * time.Second}
	c := &Cache{}
	for i := 0; i < 3; i++ {
		if _, err := c.Get(context.Background(), src, o, time.Hour); err != nil {
			t.Fatalf("拉取失败: %v", err)
		}
	}
	if hits != 1 {
		t.Fatalf("TTL 内应只拉一次，实得 %d", hits)
	}
	if _, ok2 := c.Peek(src); !ok2 {
		t.Fatal("Peek 应命中缓存")
	}
	if errs := c.Errors(src.Key()); errs != nil {
		t.Fatalf("无坏文件时不该有 errors: %v", errs)
	}

	// 过期 + 源故障：返回旧快照 + 错误（陈旧但可用）
	ok = false
	c.Invalidate(src.Key())
	snap, err := c.Get(context.Background(), src, o, 0)
	if err == nil {
		t.Fatal("源故障必须报错")
	}
	if snap == nil || len(snap.Plugins) != 1 {
		t.Fatalf("失败时应回旧快照: %+v", snap)
	}
	before := hits
	for i := 0; i < 3; i++ {
		_, _ = c.Get(context.Background(), src, o, 0)
	}
	if hits != before {
		t.Errorf("失败后应有负缓存（面板轮询不能次次打外网）: %d→%d", before, hits)
	}
	if _, _, e := c.State(src.Key()); e == nil {
		t.Error("State 应透出上次错误")
	}
}

// 竞速：一个源秒回、一个源慢到触发吞吐判据、一个源回 HTML 错误页 → 必须取秒回的那个。
func TestFetchRacingPicksFastSource(t *testing.T) {
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"version":1}`))
	}))
	defer fast.Close()
	body := strings.Repeat("a", 400_000) // 低于 MaxBody，但配合 tiny 判据会被弃
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		for i := 0; i < 200; i++ { // 每次 2KB，间隔 200ms → 约 10KB/s
			if _, err := w.Write([]byte(body[i*2000 : i*2000+2000])); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(200 * time.Millisecond)
		}
	}))
	defer slow.Close()
	html := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(strings.Repeat("<html>big page</html>", 100000)))
	}))
	defer html.Close()

	ctx := context.Background()
	o := Options{FirstByte: 2 * time.Second, Idle: time.Second, StallWin: 500 * time.Millisecond,
		MinRate: 100 * 1024, MaxBody: 1 << 20, Total: 20 * time.Second}
	start := time.Now()
	got, src, err := FetchURLs(ctx, []string{slow.URL, html.URL, fast.URL}, o)
	if err != nil {
		t.Fatalf("竞速应有一个源成功: %v", err)
	}
	if string(got) != `{"version":1}` || src != fast.URL {
		t.Fatalf("赢家应是快源: src=%s body=%q", src, got)
	}
	if d := time.Since(start); d > 6*time.Second {
		t.Fatalf("并发竞速不该等慢源，耗时 %v", d)
	}
}

func TestFetchAllSourcesFail(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	}))
	defer bad.Close()
	o := Options{Total: 3 * time.Second}
	_, _, err := FetchURLs(context.Background(), []string{bad.URL, bad.URL + "/2"}, o)
	if err == nil {
		t.Fatal("全失败必须报错")
	}
	var e *ErrAllSources
	if !asErrAll(err, &e) || len(e.Reasons) != 2 {
		t.Fatalf("应带逐源原因: %v", err)
	}
	if !strings.Contains(err.Error(), "HTTP 404") {
		t.Fatalf("原因里要能看到状态码: %v", err)
	}
}

func TestFetchFileSHAAndResume(t *testing.T) {
	payload := []byte("wild-work3 插件包内容，含中文与二进制\x00\xff")
	sum := sha256.Sum256(payload)
	want := hex.EncodeToString(sum[:])

	// 支持 Range 的源
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=5-" {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 5-%d/%d", len(payload)-1, len(payload)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(payload[5:])
			return
		}
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	dir := t.TempDir()
	dest := filepath.Join(dir, "pkg.zip")
	if _, err := FetchFile(context.Background(), []string{srv.URL}, dest, "0123", Options{Total: 5 * time.Second}, nil); err == nil {
		t.Fatal("sha256 格式非法必须直接拒")
	}
	if _, err := os.Stat(dest); err == nil {
		t.Fatal("非法 sha 不该落盘")
	}
	// 续传：留一个 5 字节半包，服务端对 Range 回 206，结果必须仍是完整包
	half := filepath.Join(dir, "resume.zip")
	if err := os.WriteFile(half, payload[:5], 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := FetchFile(context.Background(), []string{srv.URL}, half, want, Options{Total: 10 * time.Second}, nil); err != nil {
		t.Fatalf("续传失败: %v", err)
	}
	if b, _ := os.ReadFile(half); string(b) != string(payload) {
		t.Fatalf("续传结果不完整: %d 字节", len(b))
	}
	// 校验失败必须删掉半包（避免永远卡在坏包上）
	if _, err := FetchFile(context.Background(), []string{srv.URL + "/other"}, filepath.Join(dir, "x.zip"), strings.Repeat("ff", 32), Options{Total: 5 * time.Second}, nil); err == nil {
		t.Fatal("摘要不符必须报错")
	} else if !strings.Contains(err.Error(), "SHA256 mismatch from") {
		t.Fatalf("失败原因需归因到源: %v", err)
	}
}

func TestProbe(t *testing.T) {
	var rangeHdr []string
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHdr = append(rangeHdr, r.Header.Get("Range"))
		_, _ = w.Write([]byte(strings.Repeat("z", ProbeBytes)))
	}))
	defer ok.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "x", http.StatusNotFound)
	}))
	defer bad.Close()

	n, src, err := ProbeURLs(context.Background(), []string{bad.URL, ok.URL}, Options{Total: 5 * time.Second})
	if err != nil {
		t.Fatalf("可达源应成功: %v", err)
	}
	if n != ProbeBytes || src != ok.URL {
		t.Fatalf("探测结果异常: n=%d src=%s", n, src)
	}
	for _, h := range rangeHdr {
		if h == "" {
			t.Fatal("Probe 必须带 Range，不得整包拉")
		}
	}
	if _, _, err := ProbeURLs(context.Background(), []string{bad.URL}, Options{Total: 5 * time.Second}); err == nil {
		t.Fatal("404 必须报错")
	}
}

func asErrAll(err error, target **ErrAllSources) bool {
	for err != nil {
		if e, ok := err.(*ErrAllSources); ok {
			*target = e
			return true
		}
		if u, ok := err.(interface{ Unwrap() error }); ok {
			err = u.Unwrap()
			continue
		}
		return false
	}
	return false
}
