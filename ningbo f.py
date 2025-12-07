# -*- coding: utf-8 -*-
from DrissionPage import ChromiumPage, ChromiumOptions
import pandas as pd
import time
import random
import os
from urllib.parse import urljoin

# ================= 配置区域 =================
TARGET_URL = "https://ningbo.chinatax.gov.cn/zcwj/zcfgk/index.html"
VERSION = "v10.0 (稳如老狗版 - 强制休眠翻页)"


def get_desktop_path():
    return os.path.join(os.path.expanduser("~"), "Desktop")


OUTPUT_FILE = os.path.join(get_desktop_path(), "宁波税务_政策法规库_全量抓取.xlsx")


# ================= 核心逻辑 =================

def extract_detail(tab):
    try:
        info = {
            "正文": "", "文号": "", "发文单位": "", "发布日期": "", "附件": []
        }

        # 1. Meta
        try:
            date_ele = tab.ele('xpath://meta[@name="PubDate"]')
            if date_ele: info["发布日期"] = date_ele.attr("content").split(" ")[0]
            source_ele = tab.ele('xpath://meta[@name="ContentSource"]')
            if source_ele: info["发文单位"] = source_ele.attr("content")
        except:
            pass

        # 2. 正文
        content_ele = tab.ele('#zoom')
        if content_ele:
            info["正文"] = content_ele.text
        else:
            info["正文"] = tab.ele('.info-cont').text if tab.ele('.info-cont') else "正文提取失败"

        # 3. 文号
        if not info["文号"]:
            first_part = info["正文"][:300]
            if "发布文号" in first_part:
                try:
                    parts = first_part.split("发布文号")
                    candidate = parts[1].split("\n")[0].replace("】", "").replace(":", "").replace("：", "").strip()
                    info["文号"] = candidate
                except:
                    pass

        # 4. 附件
        links = tab.eles('tag:a')
        for link in links:
            href = link.attr('href')
            if not href: continue
            if href.endswith(('.doc', '.docx', '.xls', '.xlsx', '.pdf', '.zip', '.rar')):
                full_url = urljoin(tab.url, href)
                info["附件"].append({
                    "文件名": link.text,
                    "链接": full_url
                })
        return info

    except Exception as e:
        print(f"    ❌ 详情页解析出错: {e}")
        return {}


def save_to_excel(data_list, filepath):
    if not data_list: return
    while True:
        try:
            df_new = pd.DataFrame(data_list)
            if os.path.exists(filepath):
                try:
                    with pd.ExcelWriter(filepath, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
                        pass
                    df_old = pd.read_excel(filepath, engine="openpyxl")
                    df = pd.concat([df_old, df_new], ignore_index=True)
                    df.drop_duplicates(subset=["链接", "附件链接"], keep="last", inplace=True)
                except PermissionError:
                    raise PermissionError
                except:
                    df = df_new
            else:
                df = df_new

            cols = ["标题", "发布日期", "发文单位", "文号", "正文", "附件文件名", "附件链接", "链接"]
            for c in cols:
                if c not in df.columns: df[c] = ""
            df = df[cols]
            df.to_excel(filepath, index=False, engine="openpyxl")
            print(f"   💾 已保存 (总行数: {len(df)})")
            break
        except PermissionError:
            print("\n🚨 错误：Excel 文件被占用！请关闭文件...")
            time.sleep(5)
        except Exception as e:
            print(f"   ❌ Excel保存未知失败: {e}")
            break


def main():
    print(f"🚀 启动采集器 - {VERSION}")

    co = ChromiumOptions()
    co.set_user_agent(
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    co.set_argument('--blink-settings=imagesEnabled=false')
    co.set_argument('--mute-audio')
    co.set_argument('--window-position=-3000,-3000')  # 移出屏幕
    co.ignore_certificate_errors()

    page = ChromiumPage(addr_or_opts=co)

    print(f"🌐 正在访问: {TARGET_URL}")
    page.get(TARGET_URL)
    time.sleep(3)  # 首次加载多等一会

    processed_urls = set()
    if os.path.exists(OUTPUT_FILE):
        try:
            try:
                df = pd.read_excel(OUTPUT_FILE, engine="openpyxl")
                processed_urls = set(df["链接"].dropna().tolist())
                print(f"📚 已读取 {len(processed_urls)} 条历史记录")
            except:
                pass
        except:
            pass

    page_num = 1
    empty_page_count = 0

    while True:
        print(f"\n🔄 正在处理第 {page_num} 页...")

        # 1. 扫描链接 (v3.0 风格)
        try:
            page.wait.ele('tag:a', timeout=10)
        except:
            pass

        all_links = page.eles('tag:a')
        article_links = []
        for link in all_links:
            url = link.attr('href')
            title = link.text

            if not url or "javascript" in url: continue
            if not title or len(title) < 5: continue

            # 混合过滤器：是文章 且 不是分类页
            is_article = ("/art/" in url) or ("/content/" in url) or ("202" in url)
            is_category = url.endswith("index.html")

            if is_article and not is_category:
                if url not in processed_urls:
                    article_links.append({"title": title, "url": url})

        unique_links = []
        seen = set()
        for item in article_links:
            if item['url'] not in seen:
                unique_links.append(item)
                seen.add(item['url'])

        if not unique_links:
            print("⚠️ 本页未发现新数据。")
            empty_page_count += 1
            if empty_page_count >= 3:
                print("🛑 连续 3 页无数据，判断为结束。")
                break
        else:
            print(f"   📄 筛选出 {len(unique_links)} 篇新文章")
            empty_page_count = 0

        # 2. 抓取
        for item in unique_links:
            print(f"   Downloading: {item['title'][:15]}...")
            try:
                new_tab = page.new_tab(item["url"])
                new_tab.ele('#zoom', timeout=8)

                detail = extract_detail(new_tab)
                new_tab.close()

                row_base = {
                    "标题": item["title"],
                    "链接": item["url"],
                    "发布日期": detail.get("发布日期", ""),
                    "发文单位": detail.get("发文单位", ""),
                    "文号": detail.get("文号", ""),
                    "正文": detail.get("正文", "")
                }

                current_data = []
                if detail["附件"]:
                    for att in detail["附件"]:
                        row = row_base.copy()
                        row["附件文件名"] = att["文件名"]
                        row["附件链接"] = att["链接"]
                        current_data.append(row)
                else:
                    row_base["附件文件名"] = ""
                    row_base["附件链接"] = ""
                    current_data.append(row_base)

                processed_urls.add(item["url"])
                save_to_excel(current_data, OUTPUT_FILE)
                time.sleep(0.05)
            except Exception as e:
                print(f"   ❌: {e}")
                if page.tabs_count > 1: page.close_tabs(page.tab_ids[1:])

                # 3. 翻页 (v10.0: 傻瓜式强制休眠)
        print("👆 翻页中...")
        try:
            # 锁定右侧
            right_box = page.ele('.right-box')
            if right_box:
                next_btn = right_box.ele('.layui-laypage-next')
            else:
                next_btn = page.ele('.layui-laypage-next')

            if next_btn:
                # 检查禁用
                class_val = next_btn.attr("class")
                if class_val and "disabled" in class_val:
                    print(f"🛑 按钮变灰，抓取结束 (共 {page_num} 页)")
                    break

                # 🌟 关键修改：使用 JS 点击 + 强制休眠
                # 这种方式最无脑，但也最稳
                next_btn.click(by_js=True)

                print("   ⏳ 等待页面刷新 (3秒)...")
                time.sleep(3)

                print("   ✅ 假定翻页成功，继续下一轮")
                page_num += 1
            else:
                print("🛑 未找到翻页按钮，结束。")
                break

        except Exception as e:
            print(f"🛑 翻页流程出错: {e}")
            break

    print(f"\n🎉 完成！文件: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()