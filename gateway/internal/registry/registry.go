// Package registry 是插件注册表（registry）与发布包定位的读取/校验层（W3-48 v2 契约）。
//
// 三条与 v1 的根本差异（见 docs/PLUGIN_REGISTRY.md §12）：
//  1. **平铺注册文件**：注册表仓根目录一个插件一个 `<id>.json`，不再有 `index.json` 与指针详情文件；
//     整仓用 codeload tarball 一次拿全，单文件坏了只丢该文件（不整份不采信）。
//  2. **不签名**：信任 = GitHub 仓访问控制 + PR 审查 + 客户端 sha256 强制校验（§9.3）。
//     注册文件只搬运摘要，注册表仓不验证；摘要由插件仓 workflow 计算写入。
//  3. **包定位靠发布页**：注册文件只给 `repo` + `version`，客户端按 semver 匹配 `tags.atom`
//     （退 `releases.atom`）里的 tag，再用 `releases/expanded_assets/<tag>` 枚举资产。
//     **全程不碰 `api.github.com`**（实测代理全废 + 60req/h/IP 限流）。
package registry

import (
	"encoding/json"
	"fmt"
	"net/url"
	"runtime"
	"strconv"
	"strings"
)

// SchemaVersion 注册文件格式版本（`schema` 字段，不认识的值 → 该文件不采信）。
const SchemaVersion = 2

// 注册文件的 `type` 取值（W3-48：类型写在文件内，不从文件名推断）。
const (
	TypeChannel = "channel"
	TypeHook    = "hook"
	TypeMain    = "main"
)

// 发布页基址。Options.GitHub 可覆盖（离线夹具/mock 上游用），生产环境保持缺省。
const (
	GitHubBase   = "https://github.com"
	CodeloadBase = "https://codeload.github.com"
)

// ProtocolRng IPC 协议兼容区间（max=0 表示不限）。
type ProtocolRng struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

// VersionGate 版本区间（semver 字符串；空表示不限）。
type VersionGate struct {
	Min string `json:"min,omitempty"`
	Max string `json:"max,omitempty"`
}

// VersionEntry `versions[]` 的一条：一个可下载安装的具体包。
//
// `repo + version + file` 三者即可唯一定位一个发布包（代理/直连都只是前缀差异）：
// 客户端用 `repo` 找 tag（semver 匹配 `tags.atom`），再拼
// `<gh>/<repo>/releases/download/<tag>/<file>`；**不需要枚举资产列表**（W3-49）。
// 下载后按 `sha256` 强制校验——`file` 写错的后果是 404，不会装错包。
type VersionEntry struct {
	Version string `json:"version"`
	// File 发布包文件名（zip 名，含扩展名）。**不含路径**，不接受 `../` 与 `/`。
	File      string      `json:"file"`
	SHA256    string      `json:"sha256"` // 必填：包体实际字节（zip）的摘要
	Size      int64       `json:"size,omitempty"`
	OS        string      `json:"os,omitempty"`
	Arch      string      `json:"arch,omitempty"`
	Protocol  ProtocolRng `json:"protocol,omitempty"`
	Gate      VersionGate `json:"gateway,omitempty"`
	Changelog string      `json:"changelog,omitempty"`
}

// Conformance 准入测试摘要（仅展示）。
type Conformance struct {
	Pass int    `json:"pass"`
	Warn int    `json:"warn"`
	Fail int    `json:"fail"`
	Mode string `json:"mode,omitempty"`
	At   string `json:"at,omitempty"`
}

// GatewayInfo `type=main` 专属：gateway 自身的版本与摘要，**仅展示**（不参与自动更新，W3-38）。
type GatewayInfo struct {
	Version string `json:"version"`
	SHA256  string `json:"sha256,omitempty"`
	Size    int64  `json:"size,omitempty"`
	Note    string `json:"note,omitempty"`
}

// PluginEntry 一个注册文件（= 一个插件 / 主程序）。
type PluginEntry struct {
	Schema       int            `json:"schema"`
	ID           string         `json:"id"`
	Type         string         `json:"type"`
	Name         string         `json:"name"`
	Description  string         `json:"description,omitempty"`
	Author       string         `json:"author,omitempty"`
	Repo         string         `json:"repo"`
	Icon         string         `json:"icon,omitempty"`
	Color        string         `json:"color,omitempty"`
	Status       string         `json:"status,omitempty"`
	Conformance  *Conformance   `json:"conformance,omitempty"`
	Capabilities map[string]any `json:"capabilities,omitempty"`
	UpdatedAt    string         `json:"updated_at,omitempty"`
	Gateway      *GatewayInfo   `json:"gateway,omitempty"`
	Versions     []VersionEntry `json:"versions"`
}

// Key 版本条目的唯一键：**版本号 + os + arch**。
//
// 同一个版本号会为 6 个平台各出一条（全平台发布），所以不能用版本号单独做去重键
// ——那会把 windows 之后的所有平台条目当「重复声明」丢掉。
func (v VersionEntry) Key() string {
	return v.Version + "|" + orAny(v.OS) + "|" + orAny(v.Arch)
}

// orAny 空值归一成 "any"（省略 os/arch = 通用包）。
func orAny(s string) string {
	if s == "" {
		return "any"
	}
	return s
}

// ExeName 按官方样板约定推一个可执行文件名（`<id>-ext.exe` / `<id>-hook.exe`，非 Windows 无后缀）。
//
// Deprecated: 这**不是契约**（用户 m00634/m00642）。插件包内可执行文件叫什么完全自由
// （反例：`chatgpt-cli` 包内是 `chatgpt-cli`，`chatglm-cli` 包内是 `chatglm-cli`），
// 身份以 `manifest.id` 为准。安装路径（store.Install）本来就不校验文件名，
// core 侧靠 resolveExe 的三级解析（约定名 → 裸 id → 目录扫描）去发现。
// 任何需要用“包内实际文件名”的地方都应从 zip 目录项或 Discover 结果取，不要用本方法猜。
func (p PluginEntry) ExeName() string {
	sfx := "-ext"
	if p.Type == TypeHook {
		sfx = "-hook"
	}
	if runtime.GOOS == "windows" {
		return p.ID + sfx + ".exe"
	}
	return p.ID + sfx
}

// PlatformOK 该版本条目是否适用于当前平台（省略 = 通用）。
func (v VersionEntry) PlatformOK() bool {
	if v.OS != "" && v.OS != runtime.GOOS {
		return false
	}
	if v.Arch != "" && v.Arch != runtime.GOARCH {
		return false
	}
	return true
}

// Pick 选版本（§6.2）：want 非空时精确命中（semver 相等）；否则按平台过滤后取最高版本，
// 不依赖 `versions[]` 的书写顺序。
func (p PluginEntry) Pick(want string) (VersionEntry, bool) {
	if want != "" {
		for _, v := range p.Versions {
			if !v.PlatformOK() {
				continue // 同一版本号有 6 条平台条目，必须按当前平台过滤
			}
			if v.Version == want || CmpSemver(v.Version, want) == 0 {
				return v, true
			}
		}
		return VersionEntry{}, false
	}
	var best *VersionEntry
	for i := range p.Versions {
		v := p.Versions[i]
		if !v.PlatformOK() {
			continue
		}
		if best == nil || CmpSemver(v.Version, best.Version) > 0 {
			best = &p.Versions[i]
		}
	}
	if best == nil {
		return VersionEntry{}, false
	}
	return *best, true
}

// FileError 一个注册文件（或其中一条版本）被丢弃的原因。**必须透出**（§1.2），静默吞掉会让
// 「插件从商店消失」变成无法诊断的故障。
type FileError struct {
	File   string `json:"file"`
	Reason string `json:"reason"`
}

// ParseFile 解析并静态校验一个注册文件。file 是文件名（`<id>.json`）。
//
// 分级处置（§1.2）：文件级问题直接返回 error（该文件不采信）；
// 版本级问题只丢那一条，其余版本仍可用，并通过第二个返回值报告。
func ParseFile(file string, b []byte) (PluginEntry, []FileError, error) {
	var p PluginEntry
	if err := json.Unmarshal(b, &p); err != nil {
		return p, nil, fmt.Errorf("JSON 解析失败: %w", err)
	}
	if p.Schema != SchemaVersion {
		return p, nil, fmt.Errorf("schema 不认识: %d（只支持 %d）", p.Schema, SchemaVersion)
	}
	want := strings.TrimSuffix(file, ".json")
	if p.ID == "" {
		return p, nil, fmt.Errorf("缺 id 字段")
	}
	if p.ID != want {
		return p, nil, fmt.Errorf("id 与文件名不符: 文件名 %s 声明 %s", want, p.ID)
	}
	switch p.Type {
	case TypeChannel, TypeHook, TypeMain:
	case "":
		return p, nil, fmt.Errorf("缺 type 字段（channel|hook|main）")
	default:
		return p, nil, fmt.Errorf("type 非法: %q（只允许 channel|hook|main）", p.Type)
	}
	if strings.TrimSpace(p.Name) == "" {
		return p, nil, fmt.Errorf("缺 name 字段")
	}
	repo, err := NormalizeRepo(p.Repo)
	if err != nil {
		return p, nil, err
	}
	p.Repo = repo
	if p.Author == "" {
		p.Author = "wild-work3"
	}
	if p.Status == "" {
		p.Status = "unverified"
	}
	if p.Type == TypeMain && p.Gateway != nil && p.Gateway.Version == "" {
		return p, nil, fmt.Errorf("main 条目的 gateway 对象缺 version")
	}

	var warns []FileError
	keep := make([]VersionEntry, 0, len(p.Versions))
	seen := map[string]bool{}
	for _, v := range p.Versions {
		if strings.TrimSpace(v.Version) == "" {
			warns = append(warns, FileError{File: file, Reason: "有一条版本缺 version 字段，已跳过"})
			continue
		}
		if !isHexSHA(v.SHA256) {
			warns = append(warns, FileError{File: file, Reason: fmt.Sprintf("版本 %s 的 sha256 缺失或非 64 位十六进制，已跳过", v.Version)})
			continue
		}
		if err := checkFileName(v.File); err != nil {
			warns = append(warns, FileError{File: file, Reason: fmt.Sprintf("版本 %s 的 file 不合法（%s），已跳过", v.Version, err)})
			continue
		}
		if k := v.Key(); seen[k] {
			warns = append(warns, FileError{File: file, Reason: fmt.Sprintf("版本 %s（%s/%s）重复声明，已跳过后一条",
				v.Version, orAny(v.OS), orAny(v.Arch))})
			continue
		}
		seen[v.Key()] = true
		v.SHA256 = strings.ToLower(v.SHA256)
		keep = append(keep, v)
	}
	p.Versions = keep
	return p, warns, nil
}

// checkFileName 校验 `versions[].file`：必须是个纯文件名。
//
// 它会被直接拼进下载 URL，所以既不能为空（否则无法定位包），也不能带路径——
// `../` 之类拼进去会改变 URL 语义，是典型的路径注入面。
func checkFileName(s string) error {
	t := strings.TrimSpace(s)
	if t == "" {
		return fmt.Errorf("缺 file 字段（发布包文件名，如 workbuddy-1.4.2.zip）")
	}
	if t != s {
		return fmt.Errorf("file 首尾不能有空白")
	}
	if strings.ContainsAny(t, "/\\") || t == "." || t == ".." {
		return fmt.Errorf("file 只能是文件名，不能含路径: %q", s)
	}
	if !strings.HasSuffix(strings.ToLower(t), ".zip") {
		return fmt.Errorf("file 必须是 .zip 包: %q", s)
	}
	return nil
}

// NormalizeRepo 把 `https://github.com/o/r` / `github.com/o/r` / `o/r` 统一成 `o/r`。
// 只收 GitHub（下载 host allowlist 只含 GitHub 域，§2.3）。
func NormalizeRepo(s string) (string, error) {
	t := strings.TrimSpace(s)
	if t == "" {
		return "", fmt.Errorf("缺 repo 字段（插件自身仓库地址）")
	}
	t = strings.TrimSuffix(t, ".git")
	t = strings.TrimSuffix(t, "/")
	if strings.Contains(t, "://") {
		u, err := url.Parse(t)
		if err != nil || u.Host == "" {
			return "", fmt.Errorf("repo 不是合法 URL: %s", s)
		}
		if h := strings.ToLower(u.Hostname()); h != "github.com" && h != "www.github.com" {
			return "", fmt.Errorf("repo 只接受 GitHub 仓库（当前 %s）；其他托管请手动安装", u.Hostname())
		}
		t = strings.TrimPrefix(u.Path, "/")
	}
	t = strings.TrimPrefix(t, "github.com/")
	parts := strings.Split(t, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", fmt.Errorf("repo 应形如 owner/name 或 https://github.com/owner/name: %s", s)
	}
	return parts[0] + "/" + parts[1], nil
}

// ---------- 版本门 ----------

// ErrGate 版本门不满足（可读中文，直接透给面板）。
type ErrGate struct{ Msg string }

func (e *ErrGate) Error() string { return e.Msg }

// CheckGate 校验某版本的协议区间与 gateway 版本门（安装/升级前必跑，不做「装了再说」）。
func CheckGate(p PluginEntry, v VersionEntry, protocolVer int, gatewayVer string) error {
	if v.Protocol.Min > 0 && protocolVer < v.Protocol.Min {
		return &ErrGate{Msg: fmt.Sprintf("插件 %s %s 需要 IPC 协议 ≥%d，当前为 %d", p.ID, v.Version, v.Protocol.Min, protocolVer)}
	}
	if v.Protocol.Max > 0 && protocolVer > v.Protocol.Max {
		return &ErrGate{Msg: fmt.Sprintf("插件 %s %s 只支持 IPC 协议 ≤%d，当前为 %d", p.ID, v.Version, v.Protocol.Max, protocolVer)}
	}
	if err := checkSemverGate(fmt.Sprintf("插件 %s %s 需要 gateway ≥%s，当前为 %s", p.ID, v.Version, v.Gate.Min, gatewayVer),
		v.Gate.Min, gatewayVer, true); err != nil {
		return err
	}
	if err := checkSemverGate(fmt.Sprintf("插件 %s %s 只支持 gateway ≤%s，当前为 %s", p.ID, v.Version, v.Gate.Max, gatewayVer),
		v.Gate.Max, gatewayVer, false); err != nil {
		return err
	}
	return nil
}

func checkSemverGate(msg, bound, ver string, isMin bool) error {
	if strings.TrimSpace(bound) == "" {
		return nil
	}
	if cmpSemver(ver, bound) < 0 == isMin {
		return &ErrGate{Msg: msg}
	}
	return nil
}

// AllowHosts 构造下载主机 allowlist：GitHub 各域 + 各代理主机（+ 测试用额外项）。
//
// 目的：注册文件被塞进别的主机时，即便 sha256 自洽也不该下。
// 只校**请求 URL** 的主机，不追重定向落点（`releases/download` 会 302 到
// release-assets.githubusercontent.com，该主机已在表内）。
func AllowHosts(proxies []string, extra ...string) map[string]bool {
	m := map[string]bool{
		"github.com": true, "objects.githubusercontent.com": true,
		"codeload.github.com": true, "release-assets.githubusercontent.com": true,
	}
	for _, p := range proxies {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if u, err := url.Parse(p); err == nil && u.Hostname() != "" {
			m[strings.ToLower(u.Hostname())] = true
		}
	}
	for _, e := range extra {
		if u, err := url.Parse(e); err == nil && u.Hostname() != "" {
			m[strings.ToLower(u.Hostname())] = true
			continue
		}
		if t := strings.ToLower(strings.TrimSpace(e)); t != "" {
			m[t] = true
		}
	}
	return m
}

// AbsoluteSources 把「一个绝对 URL」展开成候选源（直连 + 各代理前缀），并发竞速由 fetch.go 负责。
func AbsoluteSources(base string, proxies []string) []string {
	var out []string
	if base != "" {
		out = append(out, base)
	}
	for _, p := range proxies {
		p = strings.TrimSpace(p)
		if p == "" || base == "" {
			continue
		}
		if !strings.HasSuffix(p, "/") {
			p += "/"
		}
		if u, err := url.Parse(p); err != nil || u.Scheme == "" {
			continue
		}
		out = append(out, p+base)
	}
	return out
}

func isHexSHA(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

// cmpSemver 比较两个版本号：按 . 分段数字比，非数字段按字典序；缺段视为 0。
// 只需支撑 "1.0.0" / "1.2" / "1.4.2-rc.1" 这类自产版本，不实现完整 SemVer2 优先级规则。
func cmpSemver(a, b string) int {
	as := strings.Split(strings.TrimPrefix(strings.TrimSpace(a), "v"), ".")
	bs := strings.Split(strings.TrimPrefix(strings.TrimSpace(b), "v"), ".")
	for i := 0; i < len(as) || i < len(bs); i++ {
		x, y := "0", "0"
		if i < len(as) {
			x = strings.TrimSpace(as[i])
		}
		if i < len(bs) {
			y = strings.TrimSpace(bs[i])
		}
		if xi, e1 := strconv.Atoi(x); e1 == nil {
			if yi, e2 := strconv.Atoi(y); e2 == nil {
				if xi != yi {
					if xi < yi {
						return -1
					}
					return 1
				}
				continue
			}
		}
		if x != y {
			if x < y {
				return -1
			}
			return 1
		}
	}
	return 0
}

// CmpSemver 导出版本比较（面板/测试用）。
func CmpSemver(a, b string) int { return cmpSemver(a, b) }
