#!/usr/bin/env bash
# wild-work3 主项目独立构建脚本：克隆本仓后 `bash build.sh` 即可产出四个二进制，不依赖任何外部脚本。
# 产物：本仓 dist/
#   wild-work3.exe      gateway（对外唯一出口）
#   ww3-core.exe        core（业务运行时）
#   custom-api-ext.exe  官方渠道样板
#   loopguard-hook.exe  官方拦截器样板
# 用法：bash build.sh [--clean] [--debug] [--zip]
#   --clean  只清本仓 dist/ 里的产物（不动运行数据目录）
#   --debug  保留符号表/DWARF 与本机路径，便于本地排障
#   --zip    给两个官方样板再各打一个插件包 dist/<id>-<version>-<os>-<arch>.zip（包根 = exe + manifest.json）
#            包名带平台后缀是强制约定（W3-55）：同一版本要为多个平台各发一个包，
#            注册文件的 versions[].file 靠它区分；同名会让后传的覆盖先传的。
#            本脚本只做本机构建（不交叉）；跨平台产物由 CI 在原生 runner 上产出。
set -euo pipefail
cd "$(dirname "$0")"

OUT="$PWD/dist"
BUILD_FLAGS=(-trimpath "-ldflags=-s -w")   # -trimpath 见 README：不把本机路径烧进公开产物
DO_ZIP=0
TARGET=""

# 目标表：目标名 = 产物文件名 = 入口包
TARGETS=("wild-work3.exe:./gateway" "ww3-core.exe:./core" \
         "custom-api-ext.exe:./custom-api-ext" "loopguard-hook.exe:./loopguard-hook")
# 需要打插件包的两项（gateway/core 是运行时交付物，不走插件包形状）
SAMPLES=(custom-api-ext.exe loopguard-hook.exe)

for a in "$@"; do
  case "$a" in
    --clean)
      for f in "$OUT"/*.exe "$OUT"/*.zip "$OUT"/*.sha256 "$OUT"/manifest.json; do
        if [ -e "$f" ]; then
          rm -f "$f" 2>/dev/null || echo "[clean] 进程占用，跳过：$(basename "$f")"
        fi
      done
      rm -rf "$OUT/.data" ;;
    --debug) BUILD_FLAGS=() ;;
    --zip)   DO_ZIP=1 ;;
    *) echo "未知参数: $a（可用 --clean / --debug / --zip）" >&2; exit 2 ;;
  esac
done

# 平台后缀取自本机 go 环境（本脚本不交叉；CI 各自原生 runner 构建）
GOOS_T=$(go env GOOS); GOARCH_T=$(go env GOARCH)

mkdir -p "$OUT"
echo "[build] wild-work3/main -> dist/  flags=${BUILD_FLAGS[*]:-none}"
for t in "${TARGETS[@]}"; do
  exe=${t%%:*}; pkg=${t##*:}
  go build "${BUILD_FLAGS[@]}" -o "$OUT/$exe" "$pkg"
done

# manifest 由刚产出的 exe 自己吐：保证包内清单与二进制同源，不在仓里手抄一份版本号
if [ "$DO_ZIP" = 1 ]; then
  ZIPPER=$(command -v zip || true)
  [ -n "$ZIPPER" ] || ZIPPER=$(command -v bsdtar || true)
  if [ -z "$ZIPPER" ]; then
    for t in /c/Windows/System32/tar.exe "$SYSTEMROOT/System32/tar.exe"; do
      [ -f "$t" ] && ZIPPER="$t" && break
    done
  fi
  [ -n "$ZIPPER" ] || { echo "[zip] 找不到 zip / bsdtar / tar.exe，无法打插件包" >&2; exit 1; }
  for exe in "${SAMPLES[@]}"; do
    tmp="$OUT/manifest-$exe.json"
    WW3_DATA="$OUT/.data" "$OUT/$exe" manifest --out "$tmp"   # 临时数据根：否则插件会在 dist/ 落下 logs/
    id=$(sed -n 's/.*"id":[[:space:]]*"\([^"]*\)".*/\1/p' "$tmp" | head -1)
    ver=$(sed -n 's/.*"version":[[:space:]]*"\([^"]*\)".*/\1/p' "$tmp" | head -1)
    [ -n "$id" ] && [ -n "$ver" ] || { echo "[zip] $exe 的 manifest 缺 id/version" >&2; exit 1; }
    # 包名带平台（W3-55）：同一版本号要为 6 个平台各发一个包，注册文件的 versions[].file
    # 靠它区分。只有 Windows 出 .exe，其余平台是无后缀的单一可执行文件。
    stem="${exe%.exe}"
    pkg_exe="$stem"
    [ "$GOOS_T" = "windows" ] && pkg_exe="$stem.exe"
    pkgname="$id-$ver-$GOOS_T-$GOARCH_T.zip"
    work="$OUT/pkg-$id"
    rm -rf "$work"; mkdir -p "$work"
    cp "$OUT/$exe" "$work/$pkg_exe"                     # 包根只放 exe + manifest.json（store.Install 逐项比对）
    mv "$tmp" "$work/manifest.json"
    rm -f "$OUT/$pkgname"
    case "$ZIPPER" in
      */zip) ( cd "$work" && zip -q "../$pkgname" "$pkg_exe" manifest.json ) ;;
      *)     ( cd "$work" && "$ZIPPER" -a -cf "../$pkgname" "$pkg_exe" manifest.json ) ;;
    esac
    rm -rf "$work"
    if command -v sha256sum >/dev/null; then
      ( cd "$OUT" && sha256sum "$pkgname" | tee "$pkgname.sha256" )
    elif command -v shasum >/dev/null; then
      ( cd "$OUT" && shasum -a 256 "$pkgname" | tee "$pkgname.sha256" )
    fi
    echo "[zip] dist/$pkgname"
  done

  # main.zip = core + 两个官方样板（W3-48 §3.2：gateway 不在包内，它不自动更新）
  # 安装器按 `<exe 词干>.manifest.json` 配对，故样板清单必须改名为 <stem>.manifest.json
  cver=$("$OUT/ww3-core.exe" --version 2>/dev/null | sed -n 's/^ww3-core \([0-9][0-9.]*\).*/\1/p' | head -1)
  [ -n "$cver" ] || { echo "[zip] 读不出 ww3-core 版本号" >&2; exit 1; }
  work="$OUT/pkg-main"
  rm -rf "$work"; mkdir -p "$work"
  core_exe="ww3-core"
  [ "$GOOS_T" = "windows" ] && core_exe="ww3-core.exe"
  cp "$OUT/ww3-core.exe" "$work/$core_exe"
  for exe in "${SAMPLES[@]}"; do
    stem="${exe%.exe}"
    s_exe="$stem"
    [ "$GOOS_T" = "windows" ] && s_exe="$stem.exe"
    WW3_DATA="$OUT/.data" "$OUT/$exe" manifest --out "$work/$stem.manifest.json"
    cp "$OUT/$exe" "$work/$s_exe"
  done
  rm -rf "$OUT/.data"
  pkgname="main-$cver-$GOOS_T-$GOARCH_T.zip"
  rm -f "$OUT/$pkgname"
  case "$ZIPPER" in
    */zip) ( cd "$work" && zip -q "../$pkgname" ./* ) ;;
    *)     ( cd "$work" && "$ZIPPER" -a -cf "../$pkgname" ./* ) ;;
  esac
  rm -rf "$work"
  if command -v sha256sum >/dev/null; then
    ( cd "$OUT" && sha256sum "$pkgname" | tee "$pkgname.sha256" )
  elif command -v shasum >/dev/null; then
    ( cd "$OUT" && shasum -a 256 "$pkgname" | tee "$pkgname.sha256" )
  fi
  echo "[zip] dist/$pkgname（core + ${SAMPLES[*]}）"
  rm -rf "$OUT/.data"
fi

ls -l --block-size=1 "$OUT" | awk 'NR>1 && $NF ~ /\.(exe|zip)$/ {printf "  %-34s %8.2f MB\n", $NF, $5/1048576}'
