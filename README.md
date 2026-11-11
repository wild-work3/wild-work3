# wild-work3（主项目）

把各类上游 AI 服务（官方 API Key、网页版客户端、匿名免费通道）聚合成 **OpenAI 兼容 API** 的多进程工具。
本仓库是主项目单 Go module，一次构建产出四个二进制：**gateway**（对外唯一出口 `wild-work3`）、**core**（业务运行时 `ww3-core`）、以及两个官方样板插件 `custom-api-ext` / `loopguard-hook`。

内部对话数据面一律走 **stdio + 版本化 JSONL 流式 IPC**，HTTP 只作外部入口：

```
客户端 ──HTTP(OpenAI Chat / OpenAI Responses / Anthropic Messages)──> wild-work3（gateway）
                                                                        │ stdio JSONL IPC
                                                                        ▼
                                                                     ww3-core
                                          模型路由 / 账号池 / 冷却降级 / 用量与积分记账
                              │ spawn（一请求一进程）                │ 双向流 hook.stream
                              ▼                                      ▼
                    <id>-ext.exe 渠道插件                    <id>-hook.exe 拦截器插件
                    （上游认证 + 协议转换）                   （请求/事件链式改写）
```

## 分层职责

| 进程 | 负责 | 明确不负责 |
|---|---|---|
| **gateway** `wild-work3` | 对外三协议 HTTP（Chat 为基础，Responses/Anthropic 为转换兼容）、管理鉴权、core 与插件的分发/升级/回滚、插件商店、单 origin 反代（`/v1/*` `/gw/*` `/core/*` `/ext/<id>/*`）、系统托盘 | 任何渠道业务、账号调度、直接调用渠道插件 |
| **core** `ww3-core` | 六级模型路由、账号选择与冷却/粘性/降级/重试、插件运行时（spawn/监督/回收/并发配额）、统一用量与错误处理、Dashboard API、Web UI | 三套下游协议实现、上游登录细节 |
| **渠道插件** `<id>-ext` | 上游认证与凭据刷新、请求/响应转换、模型发现、渠道专属业务（余额/签到） | 下游协议实现、全局调度、私自重试换号吞错 |
| **拦截器插件** `<id>-hook` | 在主链路流式响应上做逐帧检测与改写（已落地 `response.event` 类，双向流 `hook.stream`），链式处理 | 上游具体协议 |

原则：**插件报告事实，core 执行策略**。

## 仓库结构

| 目录 | 说明 |
|---|---|
| `protocol/` | 统一契约的 Go 实现：`jsonl`（帧）`rpc`（客户端/服务端）`plugin`（call/stdio 两形态底座）`chat`（归一化对话请求与事件）`errkind`（错误分类与可轮换性）`meta`（manifest/能力/模型/额度）`authflow`（6 种认证范式）`cli`（命令式外壳、日志脱敏、自驱动子进程）`stat`（插件自记流水） |
| `gateway/` | 二进制 `wild-work3`：`internal/{v1,gw,ipc,proxy,registry,store,tray}` |
| `core/` | 二进制 `ww3-core`：`internal/{api,config,pool,route,runtime,scheduler,stat}`，`internal/api/web/` 为 Web UI（`//go:embed`） |
| `custom-api-ext/` | 官方渠道样板：自定义 OpenAI 兼容端点（API Key），含上游 `/models` 能力探测 |
| `loopguard-hook/` | 官方拦截器样板：思考链死循环守卫（`hook.stream` 逐帧检测，命中即中断且不伪造 `[DONE]`） |
| `appconfig/` | 全项目唯一 `config.json`（`gateway` / `core` / `rules` 三段），单写者 + 跨进程锁，坏 JSON fail closed |
| `rules/` | 规则引擎（`regex` + 受限 `jsonpath`，Anchor 预检）；默认规则表内嵌在 `rules/ruleset/`，权威在 core，Pass A 在 gateway 执行 |
| `build.sh` | 本仓**独立**构建脚本：克隆即可 `bash build.sh`，不依赖工作区根的批处理（根脚本只是逐个调用各仓自带的它，再把 `.exe` 汇总到根 `dist/`） |

## 构建

Go 1.26.6。仓库根即 module `wild-work3/main`，自带独立构建脚本（不依赖任何外部脚本）：

```bash
bash build.sh          # 产出 dist/{wild-work3,ww3-core,custom-api-ext,loopguard-hook}.exe
bash build.sh --zip    # 顺带给两个官方样板各打一个插件包 dist/<id>-<version>.zip（包根 = exe + manifest.json）
bash build.sh --clean  # 只清 dist/ 里的产物，不动运行数据；--debug 保留符号表与本机路径
```

脚本内部等价于：

```bash
go build -trimpath "-ldflags=-s -w" -o dist/wild-work3.exe      ./gateway
go build -trimpath "-ldflags=-s -w" -o dist/ww3-core.exe        ./core
go build -trimpath "-ldflags=-s -w" -o dist/custom-api-ext.exe  ./custom-api-ext
go build -trimpath "-ldflags=-s -w" -o dist/loopguard-hook.exe  ./loopguard-hook
```

两个参数都不是可省的：`-s -w` 去符号表与 DWARF（四个二进制 35.9MB → 25.3MB）；`-trimpath` 不把本机绝对路径与 `$GOPATH/pkg/mod` 烧进产物（实测不带时 gateway 里有 36 处项目路径与 31 处模块缓存路径，公开产物会泄露本机目录）。

Go 单文件是静态链接整条依赖闭包，体积与「是不是纯 CLI」无关：带 `net/http` + TLS 的最小 CLI 就 7MB 起，gateway 再叠 systray、core 再叠 `//go:embed` 的 Web UI（104KB）；不依赖 `net/http` 的 `loopguard-hook` 只需 2.8MB——反向印证体积来自依赖图而非业务代码量。

## 运行

```bash
wild-work3.exe serve                       # 默认带系统托盘，拉起 core 子进程
wild-work3.exe serve --no-tray             # 无头模式（服务/无桌面环境/CI）
wild-work3.exe serve --data ./data         # 数据目录（默认 = exe 同目录/data，或 $WW3_DATA）
```

打开 <http://127.0.0.1:7863> 即 Web 面板。只监听 127.0.0.1 并做 Host 校验（防 DNS rebinding）；core 默认 `127.0.0.1:7864`，由 gateway 下发 `--listen`，不对外暴露。

分发与更新（gateway 段配置，或同名 CLI 开关；W3-48）：

| 键 / 开关 | 缺省 |
|---|---|
| `--registry` | `wild-work3/ww3-ext-registry`（官方插件市场仓：根目录平铺 `<id>.json`，整仓 tarball 一次拉取） |
| `--registry-ref` | `main`（拉取分支/tag/commit；锁版本时写 tag/sha） |
| `--proxy` | `https://gh-proxy.com/`、`https://ghfast.top/`（与直连**并发竞速**，直连不作依赖） |

主程序分两个包发布：**`wild-work3.exe`（gateway）单独发布且不自动更新**（只在 `/gw/updates` 展示版本 + `sha256` + 手动替换指引）；**`main.zip` = `ww3-core.exe` + `custom-api-ext.exe` + `loopguard-hook.exe`**，走与插件相同的自动更新机制。第三方插件不走商店：下载符合契约的包，解到 `<plugins>/<id>/<ver>/` 并写 `current`（裸版本号无换行）即可手动安装。

## 命令行自测

三个可执行文件都可直接命令行调用，且 **CLI 模式自己 spawn 自身子进程走真实 IPC**——CLI 跑通即等于协议跑通，不存在 CLI 专用捷径。验证顺序：插件 CLI → core CLI → gateway 端到端。

```bash
custom-api-ext.exe ping                                   # 控制面逐项 ✓/✗
custom-api-ext.exe chat --model gpt-4o-mini --prompt hi   # 正文走 stdout，日志走 stderr
ww3-core.exe   channels | models | usage --days 7 | chat
wild-work3.exe status | models | chat
```

core 另有两个影响运行态的开关（serve/stdio/chat 都认，且 `chat` 会透传给 spawn 出的 stdio 子进程）：
`--config <file>` 指定统一配置文件（缺省 `<data>/config.json`）、`--hooks none` 禁用全部拦截器（`id1,id2` = 只启用指定实例）。
典型用途：需要**干净 core 实例**的内部请求（如滚动上下文管理器 EC 的压缩子进程）——否则压缩请求会再过一次
ec hook 而递归触发压缩。

开关一律双短横线（无单短横线简写），`--data/--auth/--config/--proxy/--base-url/--timeout` 等通用开关的优先级为 flag > `WW3_*` 环境变量 > 缺省；数据目录下 `auths/ logs/ stat/` 三个子目录名是契约。

## 对外 API

```bash
# OpenAI Chat Completions
curl -N http://127.0.0.1:7863/v1/chat/completions -H "Authorization: Bearer <api-key>" \
  -d '{"model":"custom/gpt-4o-mini","messages":[{"role":"user","content":"你好"}],"stream":true}'

# OpenAI Responses（gateway 内转成统一内部请求）
curl -N http://127.0.0.1:7863/v1/responses -H "Authorization: Bearer <api-key>" \
  -d '{"model":"custom/gpt-4o-mini","input":"你好","stream":true}'

# Anthropic Messages
curl -N http://127.0.0.1:7863/v1/messages -H "x-api-key: <api-key>" -H "anthropic-version: 2023-06-01" \
  -d '{"model":"custom/gpt-4o-mini","max_tokens":64,"messages":[{"role":"user","content":"你好"}],"stream":true}'
```

模型名 `<前缀>/<model>` 的前缀即渠道命名空间。`/v1/models` 返回思考档位明细（上游未声明时整个 `reasoning` 对象不出现，不编造）。

两个标准可选能力端点（仅声明了对应能力的渠道可用，其余回 `not_supported`）：`POST /v1/systemone`（结构化决策）、`POST /v1/images/{generations,edits}`（生图，参考图 0~N 张），均为非流式 JSON。

## 写一个插件

两条路，都不依赖任何 SDK 模块：

1. **复制 `protocol/` 代码级复用**（Go 插件推荐；独立仓放 `internal/protocol/` 副本，契约副本需与主仓一致）。
2. **按文本契约自选语言实现**：一行一个 JSON 对象 + `\n`，请求 `{"v":1,"id":"r1","op":"invoke","method":"chat.generate","params":{...}}`，响应是同 `id` 的 `op:"event"` 序列 + 一条 `op:"result"`；stdout 只出协议，日志走 stderr。

必需命令 `call` / `stdio` / `manifest` / `ping`（`call` 是 core 的唯一调用形态：读一条 invoke → 发事件 + 一条 result → 退出）。渠道插件声明 `capabilities.conversation = ["chat.generate"]`，拦截器插件声明 `["hook.stream"]`；`manifest.json` 的 `id` 必须与商店条目 ID、数据归属前缀同源（= 仓库名去掉 `ww3-` 与 `-ext`/`-hook`/`-cli`）。

### 拦截器的 `request.prepare` 能力（可选）

拦截器可额外声明 `capabilities.conversation = ["hook.stream", "request.prepare"]`，从而在**渠道调用之前**获得一次改写请求的机会（注入管理区/工具定义/系统提示，或 fail-closed 拒绝请求）。

- 方法：`request.prepare`（一次性 invoke→result，不产生事件流）。
- params：`{request, config, hook_id, chain}`，其中 `request` 是**业务载荷视图**：`chat.GenerateRequest` 去掉调度字段（`channel_config`/`account_uid`/`extra` **不下发**，与 W3-35 规则引擎 `ruleOpaqueKeys` 同一边界——渠道基址与密钥不得进任何外部插件进程）。
- result：`{"request": <改写片段>}`；`request` 为 null 表示不改写；`ok=false + error` 表示拒绝（渠道不启动，`error.kind` 原样保留供 core 按 errkind 分类）。
- **回写是按字段的**（不是整包替换）：core 只采纳 `messages` / `tools` / `tool_choice` 三项；`messages` 为 null 视为不改写。`model`/`account_uid`/`channel_config`/`extra`/`conversation_id` 由 core 拥有（路由与选号结果），hook 回了也不采纳。
- 链语义：正向串行（H0 先改，Hn 后改，每层看到上一层的结果）；未声明该能力的层被跳过。
- 时序：`request.prepare` 在所有 `hook.stream` 进程 spawn **之前**完成，因此 hook.stream 的 params.request 已是改写后的请求。注意它在 `route.try` 的**每次 attempt** 都会跑一遍（换号/降级时 prepare 进程会被重新拉起），prepare 不得带外部副作用。
- 未声明 `request.prepare` 的拦截器（如 loopguard）行为完全不变（向后兼容）。

**完整 JSONL 示例**（注入一条管理区 system 消息到 messages 最前）：

```jsonl
// core → hook（invoke）——注意 request 里没有 channel_config/account_uid/extra
{"v":1,"id":"r1","op":"invoke","method":"request.prepare","params":{"request":{"model":"ds/deepseek-flash","messages":[{"role":"user","content":"你好"}]},"config":{},"hook_id":"ec","chain":{"index":0,"total":1}}}
// hook → core（result）——只回需要改写的部分即可，其余字段 core 不动
{"v":1,"id":"r1","op":"result","ok":true,"result":{"request":{"messages":[{"role":"system","content":"[EC_CONTEXT]...","name":"ec_management"},{"role":"user","content":"你好"}]}}}
```

> 改写片段**不需要**回全量字段：core 按字段采纳 `messages/tools/tool_choice`，其余（含调度字段）保持 core 的原值。这一条是硬约束而非便利——曾实现为整包替换，导致 hook 只回 `{messages}` 时 `account_uid`/`channel_config` 被清空，渠道基址回落到默认真实上游（既记错账又违反探测红线）。

## 红线

- ⛔ 零 token 落盘：任何日志落盘前经递归脱敏；面板只出脱敏值，明文永不回前端
- ⛔ stdout 只输出协议消息，日志走 stderr
- ⛔ 流式截断不得补 `[DONE]` 伪装正常收尾；终止事件只能出现一次
- ⛔ 已发出部分响应后不得透明重放整段生成
- ⛔ 插件不得绕过 core 私自重试/换模型/吞错
- ⛔ 内部对话数据面只支持流式；不把 HTTP SSE 当内部传输协议

## 版本

| 组件 | 版本 |
|---|---|
| gateway | 1.0.0 |
| core | 1.0.0 |
| IPC 协议 | 1 |

三者版本号独立；契约演进 append-only（只加可选字段/新方法）。

## 许可

[MIT](LICENSE)
