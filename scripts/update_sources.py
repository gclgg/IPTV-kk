#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IPTV-kk 精选源更新脚本（央视频道 / 凤凰卫视 / 凤凰资讯）

流程：
  1. 从 UPSTREAMS 配置的公开上游抓取直播源（支持 txt / m3u）
  2. 只保留「央视频道、凤凰卫视、凤凰资讯」三类，频道名统一标准化
  3. 并发用 ffprobe 读分辨率 + ffmpeg 拉流测速
  4. 按「速度分(0-60) + 分辨率分(0-40)」总分排名，每个频道只保留前 TOP_N 个源
  5. 结果写入 OUTPUT_FILE

注意：测速结果取决于运行机器的网络位置。GitHub 托管的 runner 在美国，
     国内运营商组播源基本连不通，测出来的会是境外可达的源。要贴合国内网络
     请使用自托管 runner（国内机器）。
"""

import os
import re
import sys
import time
import shutil
import tempfile
import subprocess
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

CST = timezone(timedelta(hours=8))
UA = "Mozilla/5.0 (compatible; IPTV-kk-bot/1.0)"
HTTP_TIMEOUT = 30

# ---------------------------------------------------------------------------
# 可调参数（环境变量可覆盖）
# ---------------------------------------------------------------------------
# 每个频道最终保留几个源
TOP_N = int(os.environ.get("IPTVKK_TOP_N", "5"))
# 每个频道最多拿多少个候选去测速（控制总耗时）
CANDIDATES_PER_CHANNEL = int(os.environ.get("IPTVKK_CANDIDATES", "40"))
# 测速并发数
WORKERS = int(os.environ.get("IPTVKK_WORKERS", "16"))
# 单个源拉流测速的秒数
SPEED_SECONDS = int(os.environ.get("IPTVKK_SPEED_SECONDS", "4"))
# 输出文件（改成 iptv.txt / ipv4.txt 即可直接覆盖原文件）
OUTPUT_FILE = os.environ.get("IPTVKK_OUTPUT", "精选源.txt")
# 是否在 URL 后面用 $ 备注带上速度/分辨率
SHOW_METRICS = os.environ.get("IPTVKK_SHOW_METRICS", "1") != "0"
# 调试用：跳过真实测速，只验证筛选和输出格式
DRY_RUN = os.environ.get("IPTVKK_DRY_RUN", "0") == "1"

FFPROBE_TIMEOUT = 12
FFMPEG_TIMEOUT = SPEED_SECONDS + 12

UPSTREAMS = [
    "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/itv.m3u",
    "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/index.m3u",
    "https://raw.githubusercontent.com/YanG-1989/m3u/main/Gather.m3u",
    "https://raw.githubusercontent.com/kimwang1978/collect-tv-txt/main/bbxx365_lite.txt",
]

BAD_EXT = (".mp4", ".mkv", ".avi", ".flv", ".png", ".jpg", ".jpeg", ".gif", ".webp")
BAD_KW = ("disclaimer", "about", "免责", "readme", "github.com", "gitee.com", "gitlab.com")

GROUP_ORDER = ["央视频道", "凤凰卫视", "凤凰资讯"]


# ---------------------------------------------------------------------------
# 频道识别与标准化 -> (分组, 标准频道名) 或 None
# ---------------------------------------------------------------------------
def canon(name):
    n = (name or "").strip()
    if not n:
        return None
    low = n.lower().replace(" ", "")

    # 凤凰资讯（资讯优先于卫视判断）
    if "凤凰" in n and ("资讯" in n or "info" in low or "infonews" in low):
        return ("凤凰资讯", "凤凰资讯台")

    # 凤凰卫视（中文台、香港台等统一归到凤凰卫视）
    if "凤凰" in n or "phoenix" in low:
        return ("凤凰卫视", "凤凰卫视")

    # 央视频道：CCTV1 / CCTV-1 / CCTV 1 / CCTV5+ / CCTV4K
    # 注意：4K/8K 必须先于数字判断，否则 CCTV4K 会命中成 CCTV4
    if re.search(r"cctv\s*[-_ ]?\s*(4k|8k)", n, re.I):
        return ("央视频道", "CCTV4K")
    m = re.search(r"cctv\s*[-_ ]?\s*(\d{1,2})\s*(\+)?", n, re.I)
    if m:
        return ("央视频道", f"CCTV{int(m.group(1))}{m.group(2) or ''}")
    if "央视新闻" in n or ("央视" in n and "新闻" in n):
        return ("央视频道", "CCTV13")
    if re.search(r"cctv", n, re.I):
        return ("央视频道", "CCTV1")
    return None


def chan_sort_key(name):
    m = re.search(r"(\d+)", name)
    return (0, int(m.group(1)), name) if m else (1, 0, name)


# ---------------------------------------------------------------------------
# 抓取与解析
# ---------------------------------------------------------------------------
def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return resp.read().decode("utf-8", "ignore")


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
        if not line or line.startswith("#") or "," not in line:
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


def is_valid(url):
    low = url.lower()
    if not low.startswith(("http://", "https://", "rtsp://")):
        return False
    if low.split("?")[0].endswith(BAD_EXT):
        return False
    return not any(k in low for k in BAD_KW)


# ---------------------------------------------------------------------------
# 测速与分辨率
# ---------------------------------------------------------------------------
def run(cmd, timeout):
    return subprocess.run(cmd, capture_output=True, timeout=timeout)


def measure(url):
    """返回 dict: {width, height, speed_kbps, ok}"""
    res = {"width": 0, "height": 0, "speed_kbps": 0.0, "ok": False}
    if DRY_RUN:
        # 调试模式：用 URL 造一个稳定的假分数，只用于验证筛选与输出格式
        import hashlib
        h = int(hashlib.md5(url.encode()).hexdigest()[:8], 16)
        res.update(width=1920, height=[0, 1080, 720, 576, 1080][h % 5],
                   speed_kbps=float(h % 4000 + 200), ok=True)
        return res
    if not shutil.which("ffprobe"):
        return res

    rw = "10000000"
    try:
        p = run(["ffprobe", "-v", "error", "-select_streams", "v:0",
                 "-show_entries", "stream=width,height", "-of", "csv=p=0",
                 "-rw_timeout", rw, "-analyzeduration", "3000000",
                 "-probesize", "1000000", url], FFPROBE_TIMEOUT)
        parts = p.stdout.decode("utf-8", "ignore").strip().split(",")
        if len(parts) >= 2 and parts[0].strip().isdigit():
            res["width"] = int(parts[0])
            res["height"] = int(parts[1])
    except Exception:
        return res

    if res["height"] <= 0:
        return res

    tmp = tempfile.NamedTemporaryFile(suffix=".ts", delete=False)
    tmp.close()
    try:
        t0 = time.time()
        run(["ffmpeg", "-nostdin", "-v", "error", "-rw_timeout", rw,
             "-i", url, "-t", str(SPEED_SECONDS), "-c", "copy",
             "-y", tmp.name], FFMPEG_TIMEOUT)
        elapsed = max(time.time() - t0, 0.5)
        size = os.path.getsize(tmp.name) if os.path.exists(tmp.name) else 0
        res["speed_kbps"] = round(size / 1024 / elapsed, 1)
        res["ok"] = size > 0
    except Exception:
        res["ok"] = False
    finally:
        if os.path.exists(tmp.name):
            os.remove(tmp.name)
    return res


def score(width, height, kbps):
    """速度分 0-60 + 分辨率分 0-40；不可用返回 -1"""
    if height <= 0 or kbps <= 0:
        return -1
    mbps = kbps / 1024.0
    s_speed = min(60.0, mbps / 8.0 * 60.0)
    if height >= 1080:
        s_res = 40
    elif height >= 720:
        s_res = 32
    elif height >= 576:
        s_res = 24
    elif height >= 480:
        s_res = 18
    elif height >= 360:
        s_res = 12
    else:
        s_res = 8
    return round(s_speed + s_res, 1)


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def collect_candidates():
    """返回 {(分组, 频道名): [url, ...]}"""
    buckets, seen = {}, set()
    for url in UPSTREAMS:
        try:
            items = parse_upstream(url, fetch(url))
            print(f"  ✔ {url} -> {len(items)} 条")
        except Exception as e:
            print(f"  ✘ {url} 抓取失败：{e}")
            continue
        for _g, name, u in items:
            u = u.strip()
            if not is_valid(u):
                continue
            c = canon(name)
            if not c:
                continue
            if u in seen:
                continue
            seen.add(u)
            buckets.setdefault(c, []).append(u)
    return buckets


def build_output(results, now):
    lines = [f"更新时间: {now}（北京时间）", ""]
    kept = 0
    for group in GROUP_ORDER:
        chans = sorted([c for (g, c) in results if g == group], key=chan_sort_key)
        if not chans:
            continue
        lines.append(f"{group},#genre#")
        for ch in chans:
            ranked = sorted(results[(group, ch)], key=lambda x: (-x[0], -x[1]))
            for sc, kbps, height, u in ranked[:TOP_N]:
                if SHOW_METRICS:
                    spd = f"{kbps / 1024:.1f}Mbps" if kbps >= 1024 else f"{kbps:.0f}KB/s"
                    lines.append(f"{ch},{u}${spd} {height}p 评分{sc}")
                else:
                    lines.append(f"{ch},{u}")
                kept += 1
        lines.append("")
    return lines, kept


def main():
    now = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"IPTV-kk 精选源更新 | {now}（北京时间）")
    if DRY_RUN:
        print("!! DRY_RUN 模式：不做真实测速")

    print("\n[1/3] 抓取上游并筛选频道")
    buckets = collect_candidates()
    total = sum(len(v) for v in buckets.values())
    print(f"  命中频道 {len(buckets)} 个，候选源 {total} 条")
    if not buckets:
        print("  没有命中任何目标频道，放弃写入")
        return 1

    jobs = []
    for (group, ch), urls in buckets.items():
        for u in urls[:CANDIDATES_PER_CHANNEL]:
            jobs.append((group, ch, u))
    print(f"\n[2/3] 测速与分辨率检测（{len(jobs)} 条，并发 {WORKERS}）")

    results, done = {}, 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(measure, u): (g, c, u) for g, c, u in jobs}
        for fut in as_completed(futures):
            g, c, u = futures[fut]
            try:
                m = fut.result()
            except Exception:
                m = {"width": 0, "height": 0, "speed_kbps": 0.0, "ok": False}
            done += 1
            if done % 25 == 0:
                print(f"  进度 {done}/{len(jobs)}")
            if m.get("height", 0) > 0 and m.get("speed_kbps", 0) > 0:
                sc = score(m["width"], m["height"], m["speed_kbps"])
                if sc > 0:
                    results.setdefault((g, c), []).append((sc, m["speed_kbps"], m["height"], u))

    print("\n[3/3] 排名并输出")
    lines, kept = build_output(results, now)
    if kept == 0:
        print("  没有任何源通过测速（网络不通或 ffmpeg 缺失），放弃写入")
        return 1

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).rstrip() + "\n")

    print(f"  ✔ 已写入 {OUTPUT_FILE}，共 {kept} 条源")
    for group in GROUP_ORDER:
        chans = sorted([c for (g, c) in results if g == group], key=chan_sort_key)
        if chans:
            print(f"    {group}: {len(chans)} 个频道 -> {', '.join(chans[:12])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
