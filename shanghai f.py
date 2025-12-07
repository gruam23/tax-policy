# -*- coding: utf-8 -*-
"""
上海税务局政策抓取（最终版 V5.1 - 修正 httpx 编码错误）
- 【架构】: 使用 AsyncIO + httpx 替代 Threading + Requests，实现更高并发
- 四大栏目使用 WAS XML 接口（channelid=123952 + extrasql）
- 按税种分类使用静态列表页（index.html, index_1.html, ...）
- 所有税种合并到 "按税种分类" Sheet
- 自动断点续抓（跳过已抓链接）
"""

import os
import re
import time
import math
import traceback
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import asyncio  # 导入 asyncio
import httpx  # 导入 httpx 替代 requests

# ========== 用户配置 ==========
OUTPUT_FILE = r"C:\Users\锦\Desktop\上海税收政策.xlsx"
BASE_DOMAIN = "https://shanghai.chinatax.gov.cn"
WAS_SEARCH_URL = BASE_DOMAIN + "/was5/web/search"
CHANNEL_ID = "123952"  # 源码中政策法规库使用的 channelid
PREPAGE = 15

# 使用信号量控制并发
CONCURRENT_REQUESTS = 200
# 请求超时
REQUEST_TIMEOUT = 15

# 税种列表（从页面源码提取）
TAX_PATHS = [
    "swzsgl", "nsfw", "zzs", "xfs", "qysds", "grsds", "jckss", "ccs",
    "zys", "cztdsys", "tdzzs", "dcs", "yhs", "qs", "node94", "ssxd",
    "hbs", "zhsszc", "sbf", "fssr", "ykgf", "node92"
]

# 四大栏目的 extrasql（来自你提供的源码）
EXTRASQL_MAP = {
    "国务院文件": "CHARACTERUNIT='国务院'  and  CHARACTERUNIT!=('%办%','%委员会%')",
    "总局文件": "CHARACTERUNIT=('国家税务总局','税务总局')  and  CHARACTERUNIT!=('%上海%','%上海市%')",
    "市政府文件": "CHARACTERUNIT='上海市人民政府' and  CHARACTERUNIT!=('%法制办公室%','合作交流办公室')",
    "市局文件": "CHARACTERUNIT=('上海市国家税务局','上海市地方税务局','上海市国家税务局上海市地方税务局','国家税务总M局上海市税务局')",
}

# (V4 结构) 所有税种合并
SHEET_ORDER = ["国务院文件", "总局文件", "市政府文件", "市局文件", "按税种分类"]


# ========== 内部函数 ==========

def norm_link(href, base):
    """使用 urljoin 将相对链接转换为绝对链接"""
    if not href:
        return ""
    href = href.strip()
    full_url = urljoin(base, href)
    return full_url


def parse_was_xml(xml_text):
    """解析 WAS 返回的 XML（无需修改，不涉及 I/O）"""
    soup = BeautifulSoup(xml_text, "lxml-xml")
    recs = []
    for rec in soup.find_all("REC"):
        item = {
            "TITLE": rec.find("TITLE").text if rec.find("TITLE") else "",
            "URL": rec.find("URL").text if rec.find("URL") else "",
            "WH": rec.find("WH").text if rec.find("WH") else "",
            "FWDW": rec.find("FWDW").text if rec.find("FWDW") else "",
            "RECNO": rec.find("RECNO").text if rec.find("RECNO") else "",
            "PRINTTIME": rec.find("PRINTTIME").text if rec.find("PRINTTIME") else "",
        }
        recs.append(item)
    pagecount_tag = soup.find("PAGECOUNT")
    recordcount_tag = soup.find("RECORDCOUNT")
    pagecount = int(pagecount_tag.text) if pagecount_tag and pagecount_tag.text.isdigit() else None
    recordcount = int(recordcount_tag.text) if recordcount_tag and recordcount_tag.text.isdigit() else None
    return recs, pagecount, recordcount


async def was_fetch_list(client, extrasql, page=1):
    """通过 WAS 搜索接口获取某一页数据（返回 records, pagecount, recordcount）"""
    try:
        data = {
            "channelid": CHANNEL_ID,
            "searchword": "",
            "extrasql": extrasql,
            "page": page,
            "prepage": str(PREPAGE)
        }
        resp = await client.post(WAS_SEARCH_URL, data=data, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        recs, pagecount, recordcount = parse_was_xml(resp.text)
        base_for_was = BASE_DOMAIN + "/zcfw/zcfgk/"
        for r in recs:
            r["URL"] = norm_link(r["URL"], base=base_for_was)
        return recs, pagecount, recordcount
    except Exception as e:
        print(f"[WAS fetch error] extrasql={extrasql[:80]} page={page} -> {e}")
        return [], None, None


async def fetch_static_list_for_path(client, path_folder):
    """
    抓取税种静态目录下的所有列表项。
    path_folder: 如 'zzs' 或 'qysds'
    """
    results = []
    base_folder_url = f"{BASE_DOMAIN}/zcfw/zcfgk/{path_folder}/"
    max_pages_try = 200  # 安全上限
    for p in range(0, max_pages_try):
        if p == 0:
            url = base_folder_url + "index.html"
        else:
            url = base_folder_url + f"index_{p}.html"
        try:
            r = await client.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code >= 400:
                break

            # httpx 会在 .text 中自动处理编码
            # r.encoding = r.apparent_encoding or "utf-8" (已移除)

            soup = BeautifulSoup(r.text, "lxml")
            ul = soup.find("ul", id="zcfglist")
            if not ul:
                items = soup.select("ul.infolist li a, ul.list li a, .mainbox_r .list ul li a")
            else:
                items = ul.find_all("a", href=True)
            if not items:
                break
            for a in items:
                href = a.get("href")
                title = a.get("title") or a.get_text(strip=True)
                full = norm_link(href, base=url)  # (V2 404 修复)

                parent_li = a.find_parent("li")
                pubdate = ""
                docno = ""
                fwdw = ""
                if parent_li:
                    t = parent_li.select_one(".time") or parent_li.select_one(".printtime") or parent_li.select_one(
                        ".date")
                    if t: pubdate = t.get_text(strip=True)
                    w = parent_li.select_one(".wh")
                    if w: docno = w.get_text(strip=True)
                    fw = parent_li.select_one(".title")
                    if fw: fwdw = ""
                results.append({
                    "标题": title, "链接": full, "文号": docno,
                    "发布日期": pubdate, "发文单位": fwdw, "栏目": path_folder
                })

        except Exception as e:
            print(f"[静态列表抓取异常] {url} -> {e}")
            break
    return results


async def fetch_detail(client, semaphore, url):
    """详情页抓取（asyncio 版）"""
    async with semaphore:
        try:
            resp = await client.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()  # 4xx 或 5xx 错误会在此抛出异常

            # <-- 【修改】: 移除此行
            # resp.encoding = resp.apparent_encoding or "utf-8"
            # httpx 会在调用 resp.text 时自动处理编码

            soup = BeautifulSoup(resp.text, "lxml")
            selectors = [".TRS_Editor", ".Custom_UnionStyle", ".conTxt", ".article-content", ".main-content", "#zoom",
                         ".zw", ".detail-content"]
            content = ""
            for sel in selectors:
                el = soup.select_one(sel)
                if el and el.get_text(strip=True):
                    content = el.get_text("\n", strip=True)
                    break
            if not content:
                content = soup.body.get_text("\n", strip=True)[:12000] if soup.body else ""

            docno = ""
            fwdw = ""
            pubdate = ""
            meta = soup.select_one(".head_show")
            if meta:
                txt = meta.get_text("\n", strip=True)
                for line in txt.splitlines():
                    if "文号" in line and not docno:
                        docno = line.replace("文号：", "").strip()
                    if "发文单位" in line and not fwdw:
                        fwdw = line.replace("发文单位：", "").strip()
                    if "发文日期" in line and not pubdate:
                        pubdate = line.replace("发文日期：", "").strip()

            if not pubdate:
                m = re.search(r"\d{4}[-/年]\d{1,2}[-/月]\d{1,2}", resp.text)
                if m:
                    pubdate = m.group().replace("年", "-").replace("月", "-").replace("日", "")
            return {"正文": content, "文号": docno, "发文单位": fwdw, "发布日期": pubdate}

        except httpx.HTTPStatusError as e:
            print(f"[详情抓取失败] {e} -> {url}")
            return {"正文": f"抓取失败: {e}", "文号": "", "发文单位": "", "发布日期": ""}
        except Exception as e:
            print(f"[详情抓取失败] {e} -> {url}")
            return {"正文": f"抓取失败: {e}", "文号": "", "发文单位": "", "发布日期": ""}


# ========== 主流程 ==========
def load_existing_links(output_file):
    """如果 Excel 已存在，读取所有 sheet 的链接（无需修改）"""
    existing = set()
    if os.path.exists(output_file):
        try:
            xls = pd.read_excel(output_file, sheet_name=None, engine="openpyxl")
            for sheet, df in xls.items():
                if "链接" in df.columns:
                    existing.update(df["链接"].dropna().astype(str).tolist())
            print(f"[断点续抓] 读取已有链接 {len(existing)} 条")
        except Exception as e:
            print(f"[读取现有 Excel 失败] {e}")
    return existing


def save_to_excel(grouped_records, output_file):
    """按 sheet 写入 Excel（无需修改）"""
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        for sheet in SHEET_ORDER:
            recs = grouped_records.get(sheet, [])
            if not recs:
                pd.DataFrame(columns=["标题", "链接", "文号", "发布日期", "发文单位", "栏目", "正文"]).to_excel(writer,
                                                                                                                sheet_name=sheet[
                                                                                                                    :31],
                                                                                                                index=False)
            else:
                df = pd.DataFrame(recs)
                cols = ["标题", "链接", "文号", "发布日期", "发文单位", "栏目", "正文"]
                for c in cols:
                    if c not in df.columns:
                        df[c] = ""
                df = df[cols]
                df.to_excel(writer, sheet_name=sheet[:31], index=False)
    print(f"[保存完成] {output_file}")


async def main():
    start = time.time()
    existing_links = load_existing_links(OUTPUT_FILE)
    grouped = {k: [] for k in SHEET_ORDER}

    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    }

    # verify=False 忽略 SSL 证书错误
    async with httpx.AsyncClient(headers=headers, follow_redirects=True, verify=False) as client:

        # ---------- 1) 四大栏目：通过 WAS 接口抓取（带分页） ----------
        print("开始抓取四大栏目（WAS 接口）...")
        for sheet_name, extrasql in EXTRASQL_MAP.items():
            print(f"  → 抓取栏目：{sheet_name}")
            page = 1
            total_count = 0
            while True:
                recs, pagecount, recordcount = await was_fetch_list(client, extrasql, page=page)
                if not recs:
                    break
                items = []
                for r in recs:
                    url = r.get("URL") or ""
                    if not url or url in existing_links:
                        continue
                    items.append({
                        "标题": r.get("TITLE") or "", "链接": url, "文号": r.get("WH") or "",
                        "发布日期": r.get("PRINTTIME") or "", "发文单位": r.get("FWDW") or "",
                        "栏目": sheet_name, "正文": ""
                    })

                if items:
                    tasks = []
                    for it in items:
                        tasks.append(
                            (asyncio.create_task(fetch_detail(client, semaphore, it["链接"])), it)
                        )

                    for task, it in tasks:
                        detail = await task
                        it["正文"] = detail.get("正文", "")
                        it["文号"] = it["文号"] or detail.get("文号", "")
                        it["发文单位"] = it["发文单位"] or detail.get("发文单位", "")
                        it["发布日期"] = it["发布日期"] or detail.get("发布日期", "")
                        grouped[sheet_name].append(it)
                        existing_links.add(it["链接"])

                total_count += len(items)
                print(f"    page {page} -> 采集 {len(items)} 条 (累计 {total_count})")

                if pagecount and page >= pagecount:
                    break
                page += 1

        # ---------- 2) 按税种分类：遍历各税种静态目录 ----------
        print("开始抓取按税种分类（静态目录每个子目录分页）...")
        sheet_tax = "按税种分类"  # (V4 结构)

        for tax in TAX_PATHS:
            print(f"  → 税种: {tax} (将存入 '{sheet_tax}' Sheet)")
            list_items = await fetch_static_list_for_path(client, tax)

            to_fetch = []
            for it in list_items:
                if not it["链接"] or it["链接"] in existing_links:
                    continue
                to_fetch.append({
                    "标题": it["标题"], "链接": it["链接"], "文号": it.get("文号", ""),
                    "发布日期": it.get("发布日期", ""), "发文单位": it.get("发文单位", ""),
                    "栏目": tax, "正文": ""
                })

            if not to_fetch:
                print(f"    {tax} 无新增条目，跳过")
                continue

            tasks = []
            for rec in to_fetch:
                tasks.append(
                    (asyncio.create_task(fetch_detail(client, semaphore, rec["链接"])), rec)
                )

            for task, rec in tasks:
                d = await task
                rec["正文"] = d.get("正文", "")
                rec["文号"] = rec["文号"] or d.get("文号", "")
                rec["发文单位"] = rec["发文单位"] or d.get("发文单位", "")
                rec["发布日期"] = rec["发布日期"] or d.get("发布日期", "")

                grouped[sheet_tax].append(rec)  # (V4 结构)
                existing_links.add(rec["链接"])

            print(f"    {tax} 抓取完成，新增 {len(to_fetch)} 条")

    # ---------- 保存 Excel ----------
    print("开始写入 Excel ...")
    try:
        save_to_excel(grouped, OUTPUT_FILE)
    except Exception as e:
        print(f"[写入 Excel 出错] {e}\n尝试分批写入...")
        with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
            for sheet in SHEET_ORDER:
                recs = grouped.get(sheet, [])
                df = pd.DataFrame(recs)
                df.to_excel(writer, sheet_name=sheet[:31], index=False)

    elapsed = time.time() - start
    print(f"全部完成，耗时 {elapsed:.1f} 秒，总计写入文件：{OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())