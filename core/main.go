package main

import (
	"fmt"
	"os"

	"wild-work3/main/protocol/cli"
)

// ww3-core 是业务运行时：控制面 HTTP 管理 API（/api/*，经 gateway 反代到 /core/*）
// + 会话面 stdio IPC（chat.generate，供 gateway 调用）。
//
// 命令行形态（同 W3-21 契约）：
//
//	ww3-core <command> --data <dir> [--switch value ...]
//
//	serve     默认。HTTP 管理面 + stdio IPC 同时跑（生产形态）
//	stdio     只跑 stdio IPC（gateway 调它；或 CLI 自驱动子进程用这个）
//	chat      命令行直接对话：spawn 自身 stdio，走真实 IPC
//	models    列出所有渠道的模型（读配置 + 插件控制面）
//	channels  列出渠道配置（脱敏）
//	usage     打印近 N 天用量汇总
func main() {
	app := cli.NewApp("ww3-core", "wild-work3 核心运行时（路由/账号/冷却/记账）", "serve")

	cServe := app.Cmd("serve", "HTTP 管理面 + stdio IPC 同时跑（生产形态，gateway 用这个）")
	data := cServe.Str("data", defaultDataDir(), "数据目录")
	plugins := cServe.Str("plugins", defaultPluginsDir(), "插件目录")
	configFile := cServe.Str("config", "", "统一配置文件路径（缺省 <data>/config.json）")
	hooks := cServe.Str("hooks", "", "拦截器链覆盖：none=禁用全部；id1,id2=只启用指定实例；空=从配置读")
	listen := cServe.Str("listen", "", "管理面监听（覆盖 config.json 的 core 段）")
	gwListen := cServe.Str("gw-listen", os.Getenv("WW3_GATEWAY_LISTEN"), "gateway 对外监听（仅用于「打开错端口」提示，不落盘）")

	cStdio := app.Cmd("stdio", "只跑 stdio IPC（gateway / CLI 自驱动子进程用）")
	sdata := cStdio.Str("data", defaultDataDir(), "数据目录")
	splugins := cStdio.Str("plugins", defaultPluginsDir(), "插件目录")
	sconfig := cStdio.Str("config", "", "统一配置文件路径（缺省 <data>/config.json）")
	shooks := cStdio.Str("hooks", "", "拦截器链覆盖：none=禁用全部；id1,id2=只启用指定实例；空=从配置读")

	cChat := app.Cmd("chat", "命令行直接对话：spawn 自身 stdio，走真实 IPC")
	cdata := cChat.Str("data", defaultDataDir(), "数据目录")
	cplugins := cChat.Str("plugins", defaultPluginsDir(), "插件目录")
	cconfig := cChat.Str("config", "", "统一配置文件路径（缺省 <data>/config.json）")
	chooks := cChat.Str("hooks", "", "拦截器链覆盖：none=禁用全部；id1,id2=只启用指定实例；空=从配置读")
	model := cChat.Str("model", os.Getenv("WW3_MODEL"), "模型名")
	prompt := cChat.Str("prompt", "你好，请用一句话自我介绍", "提示词")

	cModels := app.Cmd("models", "列出所有渠道的模型")
	mdata := cModels.Str("data", defaultDataDir(), "数据目录")
	mplugins := cModels.Str("plugins", defaultPluginsDir(), "插件目录")

	cChannels := app.Cmd("channels", "列出渠道配置（脱敏）")
	chdata := cChannels.Str("data", defaultDataDir(), "数据目录")
	chplugins := cChannels.Str("plugins", defaultPluginsDir(), "插件目录")

	cUsage := app.Cmd("usage", "打印近 N 天用量汇总")
	udata := cUsage.Str("data", defaultDataDir(), "数据目录")
	uplugins := cUsage.Str("plugins", defaultPluginsDir(), "插件目录")
	days := cUsage.Int("days", 1, "统计天数")

	// core 可被 gateway 自动替换，本行只给人工排障用（版本与 /api/health 同源）。
	app.SetVersion(func() string {
		return fmt.Sprintf("ww3-core %s｜IPC 协议 1｜升级由 gateway 劾担（重命名当前 exe 后放入新文件）", coreVersion)
	})

	cmd, err := app.Parse(os.Args[1:])
	if err != nil {
		if err == cli.ErrHelp {
			return
		}
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}
	var dd, pp, cf, hk string
	switch cmd.CmdName() {
	case "serve":
		dd, pp, cf, hk = *data, *plugins, *configFile, *hooks
	case "stdio":
		dd, pp, cf, hk = *sdata, *splugins, *sconfig, *shooks
	case "chat":
		dd, pp, cf, hk = *cdata, *cplugins, *cconfig, *chooks
	case "models":
		dd, pp = *mdata, *mplugins
	case "channels":
		dd, pp = *chdata, *chplugins
	case "usage":
		dd, pp = *udata, *uplugins
	}

	// 日志统一落 <data>/logs/core.log；插件子进程各自落 <插件根>/logs/<plugin>.log
	log, err := cli.NewLogger("core", cli.LogPath(dd, "core"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "[core] 初始化日志失败: %v\n", err)
	}
	defer log.Close()
	setLog(log)

	app2 := &application{dataDir: dd, pluginsDir: pp, configFile: cf, listen: *listen, hooks: hk, gwListen: *gwListen, log: log}

	switch cmd.CmdName() {
	case "stdio":
		app2.runStdioOnly()
	case "chat":
		runChat(app2, *model, *prompt)
	case "models":
		runModels(app2)
	case "channels":
		runChannels(app2)
	case "usage":
		runUsage(app2, *days)
	default:
		app2.runServe()
	}
}
