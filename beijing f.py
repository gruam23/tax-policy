# -*- coding: utf-8 -*-
"""
全国税务局政策抓取 (V17.0 - 全能融合版)
- 基础架构：回归 V6.5，支持 [全国地区] 和 [所有栏目] (问题解答/办税指南等)
- 核心逻辑：集成 V16.0 的 [完美状态字典]
- 智能判定：
  1. 有 yxx 代码 -> 按 V16 字典翻译 (含废止/失效/已修改)
  2. 无 yxx 代码 -> 默认为 "全文有效" (适用于大多数问答和指南)
"""

import asyncio
import httpx
import pandas as pd
import math
import sys
import os
import re
import tkinter as tk
from tkinter import filedialog

# ========== 🟢 你的指挥中心 ==========

# 1. 区域选择 (填 "全部" 或 ["北京", "上海"])
TARGET_REGIONS_CONFIG = ["山东"]

# 2. 栏目选择 (填 "全部" 或 ["政策法规", "问题解答", "办税指南"])
# 这里我已经把其他板块加回来了！
TARGET_CATEGORIES_CONFIG = "全部"

# ====================================

# 🗺️ 地区字典 (完整版回归)
REGION_MAP = {
    "总局": 12703, "北京": 12704, "天津": 12709, "河北": 12710, "山西": 12711,
    "内蒙古": 12712, "辽宁": 12713, "吉林": 12714, "黑龙江": 12715, "上海": 12716,
    "江苏": 12717, "浙江": 12718, "安徽": 12719, "福建": 12720, "江西": 12721,
    "山东": 12722, "河南": 12723, "湖北": 12724, "湖南": 12725, "广东": 12726,
    "广西": 12727, "四川": 12728, "贵州": 12729, "云南": 12730, "西藏": 12731,
    "陕西": 12732, "甘肃": 12733, "青海": 12734, "宁夏": 12735, "新疆": 12736,
    "海南": 12739, "重庆": 12740, "大连": 12741, "宁波": 12742, "厦门": 12743,
    "青岛": 12744, "深圳": 12745,
}

# 📚 栏目字典 (完整版回归)
CATEGORY_MAP = {
    "政策法规": 180, "问题解答": 181, "常用资料": 182, "表证单书": 183, "办税指南": 184,
}

# 🔑 【核心字典 V16.0】(您辛苦验证的成果)
# 适用于所有栏目，只要出现这些代码，就按此翻译
YXX_CODE_MAP = {
    961: "全文废止",
    962: "全文废止",  # ID 466868 验证
    963: "全文有效",
    964: "已修改",  # ID 466970 验证
    965: "全文失效",  # ID 466824 验证
    966: "全文废止",  # ID 467040 验证
}


# --- 配置解析 ---
def parse_config(config, full_map):
    if config == "全部": return list(full_map.keys())
    if isinstance(config, str): return [config]
    if isinstance(config, list): return config
    return []


target_regions_list = parse_config(TARGET_REGIONS_CONFIG, REGION_MAP)
target_categories_list = parse_config(TARGET_CATEGORIES_CONFIG, CATEGORY_MAP)

reg_label = "全国" if len(target_regions_list) > 5 else "&".join(target_regions_list)
cat_label = "全栏目" if len(target_categories_list) > 3 else "&".join(target_categories_list)

# ========== 🟢 弹出窗口选择保存路径 ==========

default_filename = f"{reg_label}_{cat_label}.xlsx"
print("⏳ 正在唤起保存窗口，请选择 Excel 存放位置...")

root = tk.Tk()
root.withdraw()
root.attributes('-topmost', True)
OUTPUT_FILE = filedialog.asksaveasfilename(
    title="请选择保存位置",
    initialfile=default_filename,
    defaultextension=".xlsx",
    filetypes=[("Excel 文件", "*.xlsx"), ("所有文件", "*.*")]
)

if not OUTPUT_FILE:
    print("❌ 你取消了保存，程序已停止。")
    sys.exit()

print(f"✅ 文件将保存至: {OUTPUT_FILE}")

# =================================================

SAVE_INTERVAL = 300
LIST_API = "https://znhd.beijing.chinatax.gov.cn:8443/zsknsrd/api/zsknsrdsjjsService/search/v1/listKnowledge"
SEMAPHORE = asyncio.Semaphore(20)

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://znhd.beijing.chinatax.gov.cn:8443",
    "Referer": "https://znhd.beijing.chinatax.gov.cn:8443/znhdzsknsrd/index?from=zcfg",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0"
}


def get_payload(page, region_id, category_id):
    return {
        "Field": category_id,
        "SortBy": "UpdateTime",
        "PageNumber": page, "PageSize": 20, "Order": "desc",
        "Range": [1, 2, 6], "Ztfl": [], "Yxx": [], "Zssx": [[], []], "Text": "",
        "Zsqy": [region_id]
    }


def load_existing_ids(filepath):
    if not os.path.exists(filepath): return set()
    print(f">>> [断点续抓] 正在读取历史存档: {filepath} ...")
    try:
        df = pd.read_excel(filepath, usecols=["链接"], engine='openpyxl')
        ids = set()
        for link in df["链接"].dropna().astype(str):
            match = re.search(r"id=(\d+)", link)
            if match: ids.add(match.group(1))
        print(f">>> [断点续抓] 已加载 {len(ids)} 条历史记录。")
        return ids
    except:
        return set()


async def process_one_item(client, item, region_name, category_name):
    doc_id = item.get("id", "")
    content = item.get("answer", "")

    # === 核心逻辑 ===
    yxx_code = item.get("yxx")

    # 1. 优先查字典 (961-966)
    if yxx_code in YXX_CODE_MAP:
        sxrq_result = YXX_CODE_MAP[yxx_code]

    # 2. 如果没有代码 (None)，分情况处理
    elif yxx_code is None:
        # 无论是政策法规，还是问题解答、办税指南
        # 只要没有标记失效代码，统一默认为“全文有效”
        # (这是最安全的策略，避免漏掉有效文件)
        sxrq_result = "全文有效"

        # 3. 未知代码兜底
    else:
        sxrq_result = f"未知状态({yxx_code})"

    info = {
        "地区": region_name,
        "栏目": category_name,
        "标题": item.get("question", ""),
        "文号": item.get("fwzh", ""),
        "发布日期": item.get("fwrq", ""),
        "生效日期": sxrq_result,
        "更新时间": item.get("updateTime", ""),
        "正文": content,
        "链接": f"https://znhd.beijing.chinatax.gov.cn:8443/znhdzsknsrd/index?from=zcfg&id={doc_id}"
    }

    return info


async def fetch_page_and_details(client, page, existing_ids, region_id, category_id, region_name, category_name):
    payload = get_payload(page, region_id, category_id)
    async with SEMAPHORE:
        try:
            resp = await client.post(LIST_API, json=payload, timeout=20)
            data = resp.json()
            items = data.get("Response", {}).get("Data", {}).get("List", [])
            total = data.get("Response", {}).get("Data", {}).get("Total", 0)
            if not items: return [], total

            new_items = [i for i in items if str(i.get("id", "")) not in existing_ids]
            if not new_items: return [], total

            tasks = [process_one_item(client, i, region_name, category_name) for i in new_items]
            results = await asyncio.gather(*tasks)
            return results, total
        except:
            return [], 0


def save_to_excel_safe(data, filepath):
    if not data: return
    print(f"    💾 正在存档 (新增 {len(data)} 条)...")
    try:
        df = pd.DataFrame(data)
        cols = ["地区", "栏目", "标题", "文号", "发布日期", "生效日期", "更新时间", "正文", "链接"]
        for c in cols:
            if c not in df.columns: df[c] = ""
        df = df[cols]
        df.to_excel(filepath, index=False, engine='openpyxl')
        print(f"    ✅ [成功] 文件已更新")
    except PermissionError:
        print("    ⚠️ [警告] Excel文件被占用，请关闭它！")
    except Exception as e:
        print(f"    ❌ [错误] {e}")


async def main():
    print("=" * 60)
    print(f"🚀 启动 V17.0 全能融合版")
    print(f"🎯 地区: {reg_label}")
    print(f"📚 栏目: {cat_label}")
    print(f"📁 输出: {OUTPUT_FILE}")
    print("=" * 60)

    existing_ids = load_existing_ids(OUTPUT_FILE)
    all_data = []

    if os.path.exists(OUTPUT_FILE):
        try:
            df_old = pd.read_excel(OUTPUT_FILE, engine='openpyxl')
            all_data = df_old.to_dict('records')
        except:
            pass

    limits = httpx.Limits(max_keepalive_connections=20, max_connections=50)

    async with httpx.AsyncClient(headers=HEADERS, verify=False, limits=limits) as client:

        total_tasks = len(target_regions_list) * len(target_categories_list)
        current_task = 0

        for reg_name in target_regions_list:
            for cat_name in target_categories_list:
                current_task += 1
                rid = REGION_MAP.get(reg_name)
                cid = CATEGORY_MAP.get(cat_name)

                if not rid or not cid: continue

                print(f"\n🔄 [{current_task}/{total_tasks}] 正在抓取: {reg_name} - {cat_name}")

                first, total = await fetch_page_and_details(client, 1, existing_ids, rid, cid, reg_name, cat_name)

                if total == 0 and not first:
                    print(f"    ⚪ 无数据")
                    continue

                if first:
                    all_data.extend(first)
                    for i in first:
                        m = re.search(r"id=(\d+)", i['链接'])
                        if m: existing_ids.add(m.group(1))

                page_size = 20
                pages = math.ceil(total / page_size)
                print(f"    🟢 发现 {total} 条数据，共 {pages} 页")

                tasks = [fetch_page_and_details(client, p, existing_ids, rid, cid, reg_name, cat_name) for p in
                         range(2, pages + 1)]

                if tasks:
                    done_cnt = 0
                    last_save = len(all_data)
                    for future in asyncio.as_completed(tasks):
                        res, _ = await future
                        done_cnt += 1
                        if res:
                            all_data.extend(res)
                            for i in res:
                                m = re.search(r"id=(\d+)", i['链接'])
                                if m: existing_ids.add(m.group(1))

                        if done_cnt % 5 == 0:
                            sys.stdout.write(f"\r    ▶️  进度: {done_cnt}/{len(tasks)} 页")
                            sys.stdout.flush()

                        if len(all_data) - last_save >= SAVE_INTERVAL:
                            print("")
                            save_to_excel_safe(all_data, OUTPUT_FILE)
                            last_save = len(all_data)

        print("\n\n" + "=" * 60)
        print("🎉 全部完成！")
        save_to_excel_safe(all_data, OUTPUT_FILE)


if __name__ == "__main__":
    import warnings

    warnings.filterwarnings("ignore")
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())