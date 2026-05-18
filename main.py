"""
AstrBot RSS/RSSHub source manager plugin.

Features:
- /rss add <url> [note]
- /rss rm <index>
- /rss addby <index> <routing> [note]
- /rss rmby <index>
- /rss get <index>
- /rss list
- /rss check <index|all> [limit]
- /rss help
- /rsshub add <url> [note]
- /rsshub rm <index>
- /rsshub list
- /rsshub help
"""

from __future__ import annotations

import asyncio
import html
import ipaddress
import json
import re
import shlex
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import feedparser
import httpx

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star
from astrbot.core.utils.astrbot_path import get_astrbot_data_path


PLUGIN_NAME = "astrbot_plugin_rss"


RSS_LIST_TEMPLATE = r"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<style>
  * {
    box-sizing: border-box;
  }
  body {
    width: 980px;
    margin: 0;
    padding: 32px;
    background: #f5f7fb;
    color: #182033;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC",
      "Microsoft YaHei", "Noto Sans CJK SC", Arial, sans-serif;
  }
  .card {
    background: #ffffff;
    border-radius: 28px;
    box-shadow: 0 18px 50px rgba(24, 32, 51, 0.12);
    overflow: hidden;
    border: 1px solid rgba(24, 32, 51, 0.06);
  }
  .header {
    padding: 30px 34px 24px 34px;
    background: linear-gradient(135deg, #edf4ff, #fff8e8);
    border-bottom: 1px solid rgba(24, 32, 51, 0.08);
  }
  .title {
    font-size: 34px;
    font-weight: 800;
    letter-spacing: -0.02em;
    margin: 0;
  }
  .subtitle {
    margin-top: 10px;
    color: #626d82;
    font-size: 18px;
  }
  .list {
    padding: 20px 24px 28px 24px;
  }
  .item {
    display: grid;
    grid-template-columns: 72px 1fr;
    gap: 18px;
    padding: 20px 10px;
    border-bottom: 1px solid #edf0f5;
  }
  .item:last-child {
    border-bottom: none;
  }
  .idx {
    width: 52px;
    height: 52px;
    border-radius: 16px;
    background: #182033;
    color: white;
    font-size: 22px;
    font-weight: 800;
    display: flex;
    align-items: center;
    justify-content: center;
  }
  .name {
    font-size: 22px;
    font-weight: 800;
    line-height: 1.35;
    word-break: break-word;
  }
  .note {
    margin-top: 8px;
    display: inline-block;
    padding: 4px 10px;
    border-radius: 999px;
    background: #fff2c7;
    color: #765100;
    font-size: 15px;
    max-width: 100%;
    word-break: break-word;
  }
  .url {
    margin-top: 9px;
    color: #5a657a;
    font-size: 15px;
    line-height: 1.45;
    word-break: break-all;
  }
  .meta {
    margin-top: 8px;
    color: #8a94a8;
    font-size: 14px;
    line-height: 1.45;
    word-break: break-word;
  }
  .empty {
    padding: 52px 34px;
    color: #626d82;
    font-size: 21px;
    text-align: center;
  }
  .footer {
    padding: 0 34px 28px 34px;
    color: #8a94a8;
    font-size: 14px;
  }
</style>
</head>
<body>
<div class="card">
  <div class="header">
    <h1 class="title">RSS Sources</h1>
    <div class="subtitle">共 {{ count }} 个 RSS 源{% if generated_at %} · {{ generated_at }}{% endif %}</div>
  </div>

  {% if items %}
  <div class="list">
    {% for item in items %}
    <div class="item">
      <div class="idx">{{ item.index }}</div>
      <div>
        <div class="name">{{ item.title or "未命名 RSS 源" }}</div>
        {% if item.note %}
        <div class="note">{{ item.note }}</div>
        {% endif %}
        <div class="url">{{ item.url }}</div>
        <div class="meta">
          {% if item.hub_label %}来自 RSSHub：{{ item.hub_label }} · {% endif %}
          {% if item.last_checked %}上次检查：{{ item.last_checked }}{% else %}尚未检查{% endif %}
          {% if item.last_error %} · 最近错误：{{ item.last_error }}{% endif %}
        </div>
      </div>
    </div>
    {% endfor %}
  </div>
  {% else %}
    <div class="empty">暂无 RSS 源。使用 /rss add &lt;url&gt; [备注] 添加。</div>
  {% endif %}

  <div class="footer">提示：/rss get &lt;index&gt; 查看详情；/rsshub list 查看 RSSHub 源。</div>
</div>
</body>
</html>
"""


class RssPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig | None = None):
        super().__init__(context)
        self.config = config or {}
        self._lock = asyncio.Lock()
        self._data_path = Path(get_astrbot_data_path()) / "plugin_data" / PLUGIN_NAME
        self._data_file = self._data_path / "rss_data.json"
        self._data_path.mkdir(parents=True, exist_ok=True)
        self._data: dict[str, Any] = {"rss_sources": [], "rsshub_sources": []}
        self._load_data()

    @filter.command_group("rss")
    def rss():
        """RSS 源管理。发送 /rss help 查看帮助。"""
        pass

    @filter.command_group("rsshub")
    def rsshub():
        """RSSHub 源管理。发送 /rsshub help 查看帮助。"""
        pass

    @rss.command("help")
    async def rss_help(self, event: AstrMessageEvent):
        """查看 RSS 指令帮助。"""
        yield event.plain_result(self._rss_help_text())

    @rsshub.command("help")
    async def rsshub_help(self, event: AstrMessageEvent):
        """查看 RSSHub 指令帮助。"""
        yield event.plain_result(self._rsshub_help_text())

    @rss.command("add")
    async def rss_add(self, event: AstrMessageEvent):
        """添加 RSS 源：/rss add <url> [备注]。"""
        parts = self._split_command(event.message_str)
        if len(parts) < 3:
            yield event.plain_result("用法：/rss add <url> [备注]")
            return

        url = self._normalize_url(parts[2])
        note = " ".join(parts[3:]).strip()
        result = await self._add_rss_source(url=url, note=note, source="manual")
        yield event.plain_result(result)

    @rss.command("rm")
    async def rss_rm(self, event: AstrMessageEvent):
        """删除 RSS 源：/rss rm <index>。"""
        parts = self._split_command(event.message_str)
        if len(parts) < 3:
            yield event.plain_result("用法：/rss rm <index>")
            return

        try:
            index = self._parse_index(parts[2])
        except ValueError as exc:
            yield event.plain_result(str(exc))
            return

        async with self._lock:
            sources = self._data["rss_sources"]
            if index < 1 or index > len(sources):
                yield event.plain_result(f"RSS 源序号不存在：{index}")
                return
            removed = sources.pop(index - 1)
            self._save_data()

        title = removed.get("title") or removed.get("url")
        yield event.plain_result(f"已删除 RSS 源 #{index}：{title}")

    @rss.command("addby")
    async def rss_addby(self, event: AstrMessageEvent):
        """根据 RSSHub 源添加路由：/rss addby <rsshub_index> <routing> [备注]。"""
        parts = self._split_command(event.message_str)
        if len(parts) < 4:
            yield event.plain_result("用法：/rss addby <rsshub_index> <routing> [备注]\n示例：/rss addby 1 /github/issue/DIYgod/RSSHub")
            return

        try:
            hub_index = self._parse_index(parts[2])
        except ValueError as exc:
            yield event.plain_result(str(exc))
            return

        routing = parts[3].strip()
        note = " ".join(parts[4:]).strip()

        async with self._lock:
            hubs = self._data["rsshub_sources"]
            if hub_index < 1 or hub_index > len(hubs):
                yield event.plain_result(f"RSSHub 源序号不存在：{hub_index}")
                return
            hub = hubs[hub_index - 1].copy()

        url = self._build_rsshub_url(hub["url"], routing)
        if not note:
            note = f"RSSHub #{hub_index}: {routing}"

        result = await self._add_rss_source(
            url=url,
            note=note,
            source="rsshub",
            hub_id=hub["id"],
            hub_base=hub["url"],
            routing=routing,
        )
        yield event.plain_result(result)

    @rss.command("rmby")
    async def rss_rmby(self, event: AstrMessageEvent):
        """删除某个 RSSHub 源下创建的所有 RSS 源：/rss rmby <rsshub_index>。"""
        parts = self._split_command(event.message_str)
        if len(parts) < 3:
            yield event.plain_result("用法：/rss rmby <rsshub_index>")
            return

        try:
            hub_index = self._parse_index(parts[2])
        except ValueError as exc:
            yield event.plain_result(str(exc))
            return

        async with self._lock:
            hubs = self._data["rsshub_sources"]
            if hub_index < 1 or hub_index > len(hubs):
                yield event.plain_result(f"RSSHub 源序号不存在：{hub_index}")
                return

            hub = hubs[hub_index - 1]
            hub_id = hub["id"]
            before = len(self._data["rss_sources"])
            self._data["rss_sources"] = [
                item for item in self._data["rss_sources"]
                if item.get("hub_id") != hub_id
            ]
            removed_count = before - len(self._data["rss_sources"])
            self._save_data()

        yield event.plain_result(f"已删除 RSSHub #{hub_index} 下的 {removed_count} 个 RSS 源。")

    @rss.command("get")
    async def rss_get(self, event: AstrMessageEvent):
        """获取 RSS 源信息：/rss get <index>。"""
        parts = self._split_command(event.message_str)
        if len(parts) < 3:
            yield event.plain_result("用法：/rss get <index>")
            return

        try:
            index = self._parse_index(parts[2])
        except ValueError as exc:
            yield event.plain_result(str(exc))
            return

        async with self._lock:
            sources = self._data["rss_sources"]
            if index < 1 or index > len(sources):
                yield event.plain_result(f"RSS 源序号不存在：{index}")
                return
            source = sources[index - 1].copy()

        feed = await self._fetch_feed(source["url"])
        if not feed["ok"]:
            yield event.plain_result(f"获取失败：{feed['error']}\nURL：{source['url']}")
            return

        await self._update_source_meta(source["id"], feed)

        entries = feed["entries"][:5]
        lines = [
            f"RSS #{index}",
            f"标题：{feed['title'] or source.get('title') or '未命名'}",
            f"链接：{feed['link'] or source['url']}",
            f"备注：{source.get('note') or '无'}",
            f"源地址：{source['url']}",
            f"条目数：{len(feed['entries'])}",
        ]
        if source.get("hub_base"):
            lines.append(f"RSSHub：{source.get('hub_base')} · 路由：{source.get('routing') or '-'}")
        if entries:
            lines.append("")
            lines.append("最近条目：")
            for i, entry in enumerate(entries, 1):
                title = entry.get("title") or "无标题"
                link = entry.get("link") or ""
                published = entry.get("published") or entry.get("updated") or ""
                lines.append(f"{i}. {title}")
                if published:
                    lines.append(f"   时间：{published}")
                if link:
                    lines.append(f"   链接：{link}")

        yield event.plain_result("\n".join(lines))

    @rss.command("list")
    async def rss_list(self, event: AstrMessageEvent):
        """列出所有 RSS 源，并渲染成图片。"""
        async with self._lock:
            sources = [item.copy() for item in self._data["rss_sources"]]
            hubs = {hub["id"]: hub for hub in self._data["rsshub_sources"]}

        items = []
        for idx, item in enumerate(sources, 1):
            hub_label = ""
            if item.get("hub_id") in hubs:
                hub = hubs[item["hub_id"]]
                hub_label = hub.get("note") or hub.get("url") or ""
            elif item.get("hub_base"):
                hub_label = item.get("hub_base", "")

            items.append({
                "index": idx,
                "title": item.get("title") or item.get("url"),
                "url": item.get("url", ""),
                "note": item.get("note", ""),
                "hub_label": hub_label,
                "last_checked": item.get("last_checked", ""),
                "last_error": item.get("last_error", ""),
            })

        data = {
            "items": items,
            "count": len(items),
            "generated_at": self._now_local_text(),
        }

        try:
            image_url = await self.html_render(
                RSS_LIST_TEMPLATE,
                data,
                options={"full_page": True, "type": "png"},
            )
            yield event.image_result(image_url)
        except Exception as exc:
            logger.error(f"RSS list render failed: {exc}")
            yield event.plain_result(self._format_rss_list_text(sources, hubs))

    @rss.command("check")
    async def rss_check(self, event: AstrMessageEvent):
        """检查 RSS 源可用性和最新条目：/rss check <index|all> [limit]。"""
        parts = self._split_command(event.message_str)
        if len(parts) < 3:
            yield event.plain_result("用法：/rss check <index|all> [limit]\n示例：/rss check 1 5")
            return

        target = parts[2].lower()
        limit = 3
        if len(parts) >= 4:
            try:
                limit = max(1, min(int(parts[3]), 10))
            except ValueError:
                yield event.plain_result("limit 必须是 1 到 10 的整数。")
                return

        async with self._lock:
            sources = [item.copy() for item in self._data["rss_sources"]]

        if not sources:
            yield event.plain_result("暂无 RSS 源。")
            return

        if target == "all":
            selected = list(enumerate(sources, 1))
        else:
            try:
                index = self._parse_index(target)
            except ValueError as exc:
                yield event.plain_result(str(exc))
                return
            if index < 1 or index > len(sources):
                yield event.plain_result(f"RSS 源序号不存在：{index}")
                return
            selected = [(index, sources[index - 1])]

        reports = []
        for index, source in selected:
            feed = await self._fetch_feed(source["url"])
            if not feed["ok"]:
                await self._mark_source_error(source["id"], feed["error"])
                reports.append(f"#{index} ❌ {source.get('title') or source['url']}\n错误：{feed['error']}")
                continue

            await self._update_source_meta(source["id"], feed, update_last_entry=True)
            latest = feed["entries"][:limit]
            title = feed["title"] or source.get("title") or source["url"]
            block = [f"#{index} ✅ {title}", f"条目数：{len(feed['entries'])}"]
            for i, entry in enumerate(latest, 1):
                block.append(f"{i}. {entry.get('title') or '无标题'}")
                if entry.get("link"):
                    block.append(f"   {entry['link']}")
            reports.append("\n".join(block))

        yield event.plain_result("\n\n".join(reports))

    @rsshub.command("add")
    async def rsshub_add(self, event: AstrMessageEvent):
        """添加 RSSHub 源：/rsshub add <url> [备注]。"""
        parts = self._split_command(event.message_str)
        if len(parts) < 3:
            yield event.plain_result("用法：/rsshub add <url> [备注]\n示例：/rsshub add https://rsshub.app 官方 RSSHub")
            return

        url = self._normalize_url(parts[2])
        note = " ".join(parts[3:]).strip()

        try:
            self._validate_public_http_url(url)
        except ValueError as exc:
            yield event.plain_result(f"URL 不合法：{exc}")
            return

        async with self._lock:
            hubs = self._data["rsshub_sources"]
            if not self._allow_duplicate() and any(item["url"] == url for item in hubs):
                yield event.plain_result(f"RSSHub 源已存在：{url}")
                return

            hubs.append({
                "id": self._new_id(),
                "url": url.rstrip("/"),
                "note": note,
                "created_at": self._now_iso(),
                "last_error": "",
            })
            self._save_data()

        yield event.plain_result(f"已添加 RSSHub 源 #{len(hubs)}：{note or url}")

    @rsshub.command("rm")
    async def rsshub_rm(self, event: AstrMessageEvent):
        """删除 RSSHub 源：/rsshub rm <index>。不会自动删除该 Hub 下已创建的 RSS 源。"""
        parts = self._split_command(event.message_str)
        if len(parts) < 3:
            yield event.plain_result("用法：/rsshub rm <index>")
            return

        try:
            index = self._parse_index(parts[2])
        except ValueError as exc:
            yield event.plain_result(str(exc))
            return

        async with self._lock:
            hubs = self._data["rsshub_sources"]
            if index < 1 or index > len(hubs):
                yield event.plain_result(f"RSSHub 源序号不存在：{index}")
                return
            removed = hubs.pop(index - 1)
            self._save_data()

        yield event.plain_result(
            f"已删除 RSSHub 源 #{index}：{removed.get('note') or removed.get('url')}\n"
            f"提示：如需删除该 Hub 下已创建的 RSS 源，请先使用 /rss rmby <index>。"
        )

    @rsshub.command("list")
    async def rsshub_list(self, event: AstrMessageEvent):
        """列出所有 RSSHub 源。"""
        async with self._lock:
            hubs = [item.copy() for item in self._data["rsshub_sources"]]
            rss_sources = [item.copy() for item in self._data["rss_sources"]]

        if not hubs:
            yield event.plain_result("暂无 RSSHub 源。使用 /rsshub add <url> [备注] 添加。")
            return

        counts: dict[str, int] = {}
        for source in rss_sources:
            hub_id = source.get("hub_id")
            if hub_id:
                counts[hub_id] = counts.get(hub_id, 0) + 1

        lines = ["RSSHub 源列表："]
        for i, hub in enumerate(hubs, 1):
            note = f"｜{hub['note']}" if hub.get("note") else ""
            lines.append(f"{i}. {hub['url']}{note}｜已创建 RSS：{counts.get(hub['id'], 0)}")
        yield event.plain_result("\n".join(lines))

    async def terminate(self):
        """插件卸载/停用时调用。"""
        async with self._lock:
            self._save_data()

    async def _add_rss_source(
        self,
        *,
        url: str,
        note: str = "",
        source: str = "manual",
        hub_id: str | None = None,
        hub_base: str | None = None,
        routing: str | None = None,
    ) -> str:
        try:
            self._validate_public_http_url(url)
        except ValueError as exc:
            return f"URL 不合法：{exc}"

        feed: dict[str, Any] | None = None
        if self._validate_on_add():
            feed = await self._fetch_feed(url)
            if not feed["ok"]:
                return f"RSS 源校验失败：{feed['error']}\n如确认为内网或临时不可用源，可在插件配置中关闭 validate_on_add 后再添加。"

        async with self._lock:
            sources = self._data["rss_sources"]
            if not self._allow_duplicate() and any(item["url"] == url for item in sources):
                return f"RSS 源已存在：{url}"

            item = {
                "id": self._new_id(),
                "url": url,
                "note": note,
                "source": source,
                "hub_id": hub_id or "",
                "hub_base": hub_base or "",
                "routing": routing or "",
                "title": "",
                "link": "",
                "created_at": self._now_iso(),
                "last_checked": "",
                "last_entry_id": "",
                "last_error": "",
            }

            if feed and feed["ok"]:
                item["title"] = feed["title"]
                item["link"] = feed["link"]
                item["last_checked"] = self._now_iso()
                if feed["entries"]:
                    item["last_entry_id"] = self._entry_id(feed["entries"][0])

            sources.append(item)
            self._save_data()
            index = len(sources)

        title = item.get("title") or item["url"]
        note_text = f"\n备注：{note}" if note else ""
        return f"已添加 RSS 源 #{index}：{title}{note_text}\nURL：{url}"

    async def _fetch_feed(self, url: str) -> dict[str, Any]:
        timeout = float(self.config.get("request_timeout", 15))
        max_bytes = int(self.config.get("max_feed_bytes", 2_000_000))
        user_agent = str(self.config.get("user_agent", "AstrBot-RSS-Plugin/1.0"))

        try:
            async with httpx.AsyncClient(
                timeout=timeout,
                follow_redirects=True,
                headers={"User-Agent": user_agent},
            ) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                content = resp.content[:max_bytes]
        except Exception as exc:
            return {"ok": False, "error": f"请求失败：{exc}"}

        parsed = feedparser.parse(content)
        entries = []
        for entry in parsed.entries:
            entries.append({
                "id": str(entry.get("id") or entry.get("guid") or entry.get("link") or entry.get("title") or ""),
                "title": str(entry.get("title") or ""),
                "link": str(entry.get("link") or ""),
                "published": str(entry.get("published") or ""),
                "updated": str(entry.get("updated") or ""),
                "summary": self._strip_html(str(entry.get("summary") or ""))[:280],
            })

        title = str(parsed.feed.get("title") or "")
        link = str(parsed.feed.get("link") or "")

        if not title and not entries:
            error = "未解析到有效 feed 标题或条目"
            if getattr(parsed, "bozo", False) and getattr(parsed, "bozo_exception", None):
                error += f"：{parsed.bozo_exception}"
            return {"ok": False, "error": error}

        return {
            "ok": True,
            "title": title,
            "link": link,
            "entries": entries,
            "raw_bozo": bool(getattr(parsed, "bozo", False)),
        }

    async def _update_source_meta(
        self,
        source_id: str,
        feed: dict[str, Any],
        update_last_entry: bool = False,
    ) -> None:
        async with self._lock:
            for item in self._data["rss_sources"]:
                if item["id"] == source_id:
                    item["title"] = feed.get("title") or item.get("title") or ""
                    item["link"] = feed.get("link") or item.get("link") or ""
                    item["last_checked"] = self._now_iso()
                    item["last_error"] = ""
                    if update_last_entry and feed.get("entries"):
                        item["last_entry_id"] = self._entry_id(feed["entries"][0])
                    self._save_data()
                    break

    async def _mark_source_error(self, source_id: str, error: str) -> None:
        async with self._lock:
            for item in self._data["rss_sources"]:
                if item["id"] == source_id:
                    item["last_checked"] = self._now_iso()
                    item["last_error"] = error[:300]
                    self._save_data()
                    break

    def _load_data(self) -> None:
        if not self._data_file.exists():
            self._save_data()
            return

        try:
            loaded = json.loads(self._data_file.read_text(encoding="utf-8"))
            if not isinstance(loaded, dict):
                raise ValueError("data root is not dict")
            loaded.setdefault("rss_sources", [])
            loaded.setdefault("rsshub_sources", [])
            self._data = loaded
        except Exception as exc:
            logger.error(f"Failed to load RSS plugin data: {exc}")
            backup = self._data_file.with_suffix(f".broken.{int(datetime.now().timestamp())}.json")
            try:
                self._data_file.rename(backup)
            except Exception:
                pass
            self._data = {"rss_sources": [], "rsshub_sources": []}
            self._save_data()

    def _save_data(self) -> None:
        self._data_path.mkdir(parents=True, exist_ok=True)
        tmp = self._data_file.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(self._data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(self._data_file)

    def _validate_on_add(self) -> bool:
        return bool(self.config.get("validate_on_add", True))

    def _allow_duplicate(self) -> bool:
        return bool(self.config.get("allow_duplicate", False))

    def _normalize_url(self, raw_url: str) -> str:
        raw_url = raw_url.strip()
        if not raw_url:
            return raw_url
        if not re.match(r"^https?://", raw_url, re.I):
            raw_url = "https://" + raw_url
        parsed = urlparse(raw_url)
        normalized = parsed._replace(fragment="")
        return urlunparse(normalized)

    def _validate_public_http_url(self, url: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError("仅支持 http/https URL")
        if not parsed.netloc:
            raise ValueError("缺少域名或主机")
        host = (parsed.hostname or "").strip().lower()
        if not host:
            raise ValueError("缺少主机名")
        if not bool(self.config.get("allow_local_urls", False)):
            if host in {"localhost", "127.0.0.1", "::1"} or host.endswith(".local"):
                raise ValueError("默认禁止 localhost/.local 地址；如确需使用，请在配置中启用 allow_local_urls")
            try:
                ip = ipaddress.ip_address(host)
                if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                    raise ValueError("默认禁止内网、回环、保留 IP；如确需使用，请在配置中启用 allow_local_urls")
            except ValueError as exc:
                if "默认禁止" in str(exc):
                    raise
                # Hostname is not an IP address. Keep it allowed.
                pass

    def _build_rsshub_url(self, hub_base: str, routing: str) -> str:
        routing = routing.strip()
        if re.match(r"^https?://", routing, re.I):
            return self._normalize_url(routing)
        if not routing.startswith("/"):
            routing = "/" + routing
        return urljoin(hub_base.rstrip("/") + "/", routing.lstrip("/"))

    def _parse_index(self, raw: str) -> int:
        try:
            index = int(raw)
        except ValueError:
            raise ValueError("index 必须是正整数。")
        if index <= 0:
            raise ValueError("index 必须从 1 开始。")
        return index

    def _split_command(self, message: str) -> list[str]:
        message = (message or "").strip()
        if message.startswith("/"):
            message = message[1:]
        try:
            return shlex.split(message)
        except ValueError:
            # Fallback for unmatched quotes.
            return message.split()

    def _entry_id(self, entry: dict[str, Any]) -> str:
        return str(entry.get("id") or entry.get("link") or entry.get("title") or "")

    def _strip_html(self, text: str) -> str:
        text = re.sub(r"<[^>]+>", "", text)
        return html.unescape(re.sub(r"\s+", " ", text)).strip()

    def _new_id(self) -> str:
        return uuid.uuid4().hex

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    def _now_local_text(self) -> str:
        return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")

    def _format_rss_list_text(self, sources: list[dict[str, Any]], hubs: dict[str, dict[str, Any]]) -> str:
        if not sources:
            return "暂无 RSS 源。使用 /rss add <url> [备注] 添加。"

        lines = ["RSS 源列表："]
        for i, item in enumerate(sources, 1):
            note = f"｜{item['note']}" if item.get("note") else ""
            title = item.get("title") or item.get("url")
            hub_text = ""
            if item.get("hub_id") in hubs:
                hub = hubs[item["hub_id"]]
                hub_text = f"｜RSSHub：{hub.get('note') or hub.get('url')}"
            lines.append(f"{i}. {title}{note}{hub_text}\n   {item.get('url')}")
        return "\n".join(lines)

    def _rss_help_text(self) -> str:
        return (
            "RSS 插件指令：\n"
            "/rss add <url> [备注] - 添加 RSS 源\n"
            "/rss rm <index> - 删除第 index 个 RSS 源\n"
            "/rss addby <rsshub_index> <routing> [备注] - 使用 RSSHub 源和路由添加 RSS\n"
            "/rss rmby <rsshub_index> - 删除某个 RSSHub 源下创建的所有 RSS 源\n"
            "/rss get <index> - 获取第 index 个 RSS 源详情\n"
            "/rss list - 以图片列出全部 RSS 源\n"
            "/rss check <index|all> [limit] - 检查 RSS 源并显示最新条目\n"
            "/rss help - 查看帮助\n\n"
            "备注中如需保留特殊字符，可用引号包裹，例如：/rss add https://example.com/feed.xml \"我的备注\""
        )

    def _rsshub_help_text(self) -> str:
        return (
            "RSSHub 插件指令：\n"
            "/rsshub add <url> [备注] - 添加 RSSHub 基础地址\n"
            "/rsshub rm <index> - 删除第 index 个 RSSHub 源\n"
            "/rsshub list - 列出 RSSHub 源\n"
            "/rsshub help - 查看帮助\n\n"
            "示例：\n"
            "/rsshub add https://rsshub.app 官方 RSSHub\n"
            "/rss addby 1 /github/issue/DIYgod/RSSHub RSSHub Issues"
        )
