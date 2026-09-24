#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IPTV-kk 节目源自动更新脚本

工作逻辑：
  1. 从 TARGETS 里配置的公开上游源抓取最新直播源（支持 txt / m3u 两种格式）
  2. 以仓库现有的分组结构和频道清单为骨架，用上游抓到的同名频道源刷新
     每条频道的最终来源顺序：仓库保留源（组播等） > 上游新源 > 仓库旧公网源
  3. 按 URL 去重、过滤明显无效的条目（广告视频、说明文件等）、按频道条数上限裁剪
  4. 只有新增条目达到 MIN_NEW_ENTRIES 才写回文件，避免上游抽风时污染仓库

默认只更新仓库已有的频道，不引入新频道（INCLUDE_NEW_CHANNELS=True 可放开）。
任何单个上游抓取失败都会被跳过，不影响其它源。
"""

import os
import re
import sys
import urllib.request
from datetime import datetime, timezone, timedelta

CST = timezone(timedelta(hours=8))
UA = "Mozilla/5.0 (compatible; IPTV-kk-bot/1.0)"
TIMEOUT = 30

# ---------------------------------------------------------------------------
# 可调参数（均可用环境变量覆盖，方便 Actions 手动触发时调整）
# ---------------------------------------------------------------------------
# 是否允许把仓库原本没有的频道写进来
INCLUDE_NEW_CHANNELS = os.environ.get("IPTVKK_INCLUDE_NEW", "false").lower() == "true"
# 新增条目少于这个数就不写回文件
MIN_NEW_ENTRIES = int(os.environ.get("IPTVKK_MIN_NEW", "5"))
# 环境变量可整体覆盖单频道条数上限
MAX_OVERRIDE = os.environ.get("IPTVKK_MAX_PER_CHANNEL", "").strip()

TARGETS = {
    "iptv.txt": {
        # 仓库原有的组播源永久保留，不会被新源挤掉
        "keep": ("/rtp/", "/udp/", "rtsp://", "rtp://", "udp://"),
        # 允许写入的协议
        "schemes": ("http://", "https://", "rtp://", "udp://", "rtsp://"),
        # 单频道保留条数上限
        "max": 20,
        "disclaimer": "http://kakaxi.indevs.in/LOGO/Disclaimer.mp4",
        "sources": [
            "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/itv.m3u",
            "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/index.m3u",
            "https://raw.githubusercontent.com/kimwang1978/collect-tv-txt/main/bbxx365_lite.txt",
        ],
    },
    "ipv4.txt": {
        # 公网源全部可以换新，不留旧货
        "keep": (),
        "schemes": ("http://", "https://"),
        "max": 8,
        "disclaimer": "http://kakaxi.indevs.in/Disclaimer.mp4",
        "sources": [
            "https://raw.githubusercontent.com/YanG-1989/m3u/main/Gather.m3u",
            "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/index.m3u",
            "https://raw.githubusercontent.com/kimwang1978/collect-tv-txt/main/bbxx365_lite.txt",
        ],
    },
}

# 明显不是直播源的条目
BAD_EXT = (".mp4", ".mkv", ".avi", ".flv", ".png", ".jpg", ".jpeg", ".gif", ".webp")
BAD_KW = ("disclaimer", "about", "免责", "readme", "github.com", "gitee.com", "gitlab.com")

GROUP_TIME = "更新时间"


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------
def now_str():
    return datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")


def norm_name(name):
    """频道名归一化，用于跨源匹配（CCTV 1 / CCTV-1 / CCTV1 视为同一个）"""
    s = re.sub(r"[（(].*?[)）]", "", name or "")
    s = re.sub(r"[\s\-_·•]+", "", s)
    s = re.sub(r"(高清|超清|标清|4k|8k|hd|fhd|sd|1080p|720p|50fps|hevc)+$", "", s, flags=re.I)
    return s.lower().strip()


def is_valid(url, schemes):
    if not url:
        return False
    low = url.lower()
    if not low.startswith(tuple(schemes)):
        return False
    if low.split("?")[0].endswith(BAD_EXT):
        return False
    return not any(k in low for k in BAD_KW)


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        return resp.read().decode("utf-8", "ignore")


# ---------------------------------------------------------------------------
# 解析上游
# ---------------------------------------------------------------------------
def parse_m3u(text):
    items, name, group = [], None, ""
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#EXTINF"):
            m = re.search(r'group-title="([^"]*)"', line)
            group = m.group(1).strip() if m else ""
            disp = line.split(",")[-1].strip()
            m2 = re.search(r'tvg-name="([^"]*)"', line)
            name = (m2.group(1).strip() if m2 else "") or disp
        elif line.startswith("#"):
            continue
        elif name:
            items.append((group, name, line))
            name = None
    return items


def parse_txt(text):
    items, group = [], ""
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "," not in line:
            continue
        if line.endswith(",#genre#"):
            group = line[: -len(",#genre#")].strip()
            continue
        name, url = line.split(",", 1)
        name, url = name.strip(), url.strip()
        if name and url:
            items.append((group, name, url))
    return items


def parse_upstream(url, text):
    return parse_m3u(text) if "m3u" in url.lower() else parse_txt(text)


# ---------------------------------------------------------------------------
# 现有文件读写
# ---------------------------------------------------------------------------
def load_existing(path):
    """返回 (首行标题, [(分组, [(频道名, url)])])"""
    header, blocks = "", []
    if not os.path.exists(path):
        return header, blocks
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.read().splitlines()
    cur = None
    for i, line in enumerate(lines):
        raw = line.strip()
        if i == 0 and raw.startswith(("更新时间:", "更新时间：")):
            header = raw
            continue
        if not raw or raw.startswith("#"):
            continue
        if raw.endswith(",#genre#"):
            cur = (raw[: -len(",#genre#")].strip(), [])
            blocks.append(cur)
            continue
        if "," not in raw:
            continue
        name, url = raw.split(",", 1)
        name, url = name.strip(), url.strip()
        if cur is None:
            cur = ("未分组", [])
            blocks.append(cur)
        cur[1].append((name, url))
    return header, blocks


def write_target(path, header, blocks, disclaimer, now):
    # 保留原首行里「（北京时间）」后面的说明文字（如 iptv.txt 的“仅供娱乐，切勿商用。”）
    m = re.search(r"（北京时间）(.*)$", header)
    suffix = m.group(1) if m else ""
    lines = [f"更新时间: {now}（北京时间）{suffix}", ""]
    lines.append(f"{GROUP_TIME},#genre#")
    lines.append(f"{now},{disclaimer}")
    lines.append("")
    for group, entries in blocks:
        if group == GROUP_TIME or not entries:
            continue
        lines.append(f"{group},#genre#")
        for name, url in entries:
            lines.append(f"{name},{url}")
        lines.append("")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).rstrip() + "\n")


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def update_target(path, cfg):
    print(f"\n===== 处理 {path} =====")
    header, blocks = load_existing(path)
    disclaimer = cfg["disclaimer"]
    for group, entries in blocks:
        if group == GROUP_TIME and entries:
            disclaimer = entries[0][1]
            break

    schemes = cfg["schemes"]
    keep = tuple(cfg["keep"])
    limit = int(MAX_OVERRIDE) if MAX_OVERRIDE else int(cfg["max"])

    # 归一化频道名 -> 分组下标
    index = {}
    for gi, (group, entries) in enumerate(blocks):
        for name, _url in entries:
            index.setdefault(norm_name(name), gi)

    fetched, ok_sources = [], 0
    for url in cfg["sources"]:
        try:
            text = fetch(url)
            items = parse_upstream(url, text)
            fetched.extend(items)
            ok_sources += 1
            print(f"  ✔ {url} -> {len(items)} 条")
        except Exception as e:
            print(f"  ✘ {url} 抓取失败：{e}")

    if ok_sources == 0:
        print(f"  !! {path} 所有上游均不可用，放弃本次更新")
        return False, 0

    # 先把上游新源按频道归拢
    fresh = {}       # 归一化频道名 -> [(name, url)]
    new_blocks = {}  # 新频道的分组 -> [(name, url)]
    for group, name, url in fetched:
        if not is_valid(url, schemes):
            continue
        key = norm_name(name)
        if not key:
            continue
        if key in index:
            bucket = fresh.setdefault(key, [])
            if len(bucket) < limit:
                bucket.append((name, url))
        elif INCLUDE_NEW_CHANNELS:
            b = new_blocks.setdefault(group or "新增频道", [])
            if len(b) < limit * 20:
                b.append((name, url))

    # 逐频道重组：保留源 > 上游新源 > 旧公网源，超限从尾部裁剪
    added = 0

    def is_keep(url):
        return bool(keep) and any(t in url.lower() for t in keep)

    for gi, (group, entries) in enumerate(blocks):
        if group == GROUP_TIME:
            continue
        # 按频道归拢，保持频道首次出现的顺序
        order, bykey = [], {}
        for name, url in entries:
            k = norm_name(name)
            if k not in bykey:
                bykey[k] = []
                order.append(k)
            bykey[k].append((name, url))

        merged_all = []
        for k in order:
            ch = bykey[k]
            kept = [e for e in ch if is_keep(e[1])]
            old_public = [e for e in ch if not is_keep(e[1])]
            seen = {u for _n, u in ch}
            new_part = []
            for name, url in fresh.get(k, []):
                if url in seen:
                    continue
                seen.add(url)
                new_part.append((name, url))
            merged = (kept + new_part + old_public)[:limit]
            new_urls = {u for _n, u in new_part}
            added += sum(1 for _n, u in merged if u in new_urls)
            merged_all.extend(merged)

        if merged_all != entries:
            blocks[gi] = (group, merged_all)

    print(f"  本次引入新源 {added} 条（单频道上限 {limit}）")
    if added < MIN_NEW_ENTRIES:
        print(f"  新增不足 {MIN_NEW_ENTRIES} 条，保持原文件不变")
        return False, added

    for g, entries in new_blocks.items():
        blocks.append((g, entries))

    write_target(path, header, blocks, disclaimer, now_str())
    total = sum(len(e) for _g, e in blocks)
    print(f"  ✔ 已写入 {path}，共 {total} 条")
    return True, added


def main():
    print(f"IPTV-kk 节目源更新 | {now_str()}（北京时间）")
    total_added, changed = 0, []
    for path, cfg in TARGETS.items():
        written, added = update_target(path, cfg)
        total_added += added
        if written:
            changed.append(path)
    print(f"\n合计引入新源 {total_added} 条，本次更新文件：{changed or '无'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
