
"""
astrbot_plugin_rss

RSS/RSSHub source manager for AstrBot.

v0.1.1:
- Per conversation/group subscription storage by event.unified_msg_origin.
- User-created cron polling tasks.
- Active push new RSS entries to the conversation that created the task.
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
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star
from astrbot.core.utils.astrbot_path import get_astrbot_data_path


PLUGIN_NAME = "astrbot_plugin_rss"
DATA_VERSION = 2


RSS_LIST_TEMPLATE = r"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<style>
  * { box-sizing: border-box; }
  body {
    width: 980px; margin: 0; padding: 32px; background: #f5f7fb; color: #182033;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC",
      "Microsoft YaHei", "Noto Sans CJK SC", Arial, sans-serif;
  }
  .card { background: #fff; border-radius: 28px; box-shadow: 0 18px 50px rgba(24,32,51,.12);
    overflow: hidden; border: 1px solid rgba(24,32,51,.06); }
  .header { padding: 30px 34px 24px; background: linear-gradient(135deg,#edf4ff,#fff8e8);
    border-bottom: 1px solid rgba(24,32,51,.08); }
  .title { font-size: 34px; font-weight: 800; margin: 0; }
  .subtitle { margin-top: 10px; color: #626d82; font-size: 18px; }
  .list { padding: 20px 24px 28px; }
  .item { display: grid; grid-template-columns: 72px 1fr; gap: 18px; padding: 20px 10px;
    border-bottom: 1px solid #edf0f5; }
  .item:last-child { border-bottom: none; }
  .idx { width: 52px; height: 52px; border-radius: 16px; background: #182033; color: white;
    font-size: 22px; font-weight: 800; display: flex; align-items: center; justify-content: center; }
  .name { font-size: 22px; font-weight: 800; line-height: 1.35; word-break: break-word; }
  .note { margin-top: 8px; display: inline-block; padding: 4px 10px; border-radius: 999px;
    background: #fff2c7; color: #765100; font-size: 15px; max-width: 100%; word-break: break-word; }
  .url { margin-top: 9px; color: #5a657a; font-size: 15px; line-height: 1.45; word-break: break-all; }
  .meta { margin-top: 8px; color: #8a94a8; font-size: 14px; line-height: 1.45; word-break: break-word; }
  .empty { padding: 52px 34px; color: #626d82; font-size: 21px; text-align: center; }
  .footer { padding: 0 34px 28px; color: #8a94a8; font-size: 14px; }
</style>
</head>
<body>
<div class="card">
  <div class="header">
    <h1 class="title">RSS Sources</h1>
    <div class="subtitle">当前会话 · 共 {{ count }} 个 RSS 源{% if generated_at %} · {{ generated_at }}{% endif %}</div>
  </div>
  {% if items %}
  <div class="list">
    {% for item in items %}
    <div class="item">
      <div class="idx">{{ item.index }}</div>
      <div>
        <div class="name">{{ item.title or "未命名 RSS 源" }}</div>
        {% if item.note %}<div class="note">{{ item.note }}</div>{% endif %}
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
    <div class="empty">当前会话暂无 RSS 源。使用 /rss add &lt;url&gt; [备注] 添加。</div>
  {% endif %}
  <div class="footer">提示：/rss get &lt;index&gt; 查看详情；/rss cronlist 查看当前会话的定时任务。</div>
</div>
</body>
</html>
"""


RSS_ENTRY_TEMPLATE = r"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<style>
  * { box-sizing: border-box; }
  body {
    width: 980px; margin: 0; padding: 24px; background: #eef2f8; color: #1b2335;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC",
      "Microsoft YaHei", "Noto Sans CJK SC", Arial, sans-serif;
  }
  .card { background: #fff; border-radius: 24px; box-shadow: 0 18px 40px rgba(27,35,53,.12);
    border: 1px solid rgba(27,35,53,.08); overflow: hidden; }
  .header { padding: 26px 30px 20px; background: linear-gradient(135deg,#edf3ff,#fff7e7);
    border-bottom: 1px solid rgba(27,35,53,.08); }
  .feed-tag { display: inline-block; padding: 6px 12px; border-radius: 999px; background: #1b2335;
    color: #fff; font-size: 14px; font-weight: 700; margin-bottom: 14px; }
  h1 { margin: 0; font-size: 30px; line-height: 1.35; font-weight: 800; word-break: break-word; }
  .meta { margin-top: 14px; color: #647086; font-size: 15px; line-height: 1.7; word-break: break-word; }
  .section { padding: 24px 30px 8px; }
  .section h2 { margin: 0 0 14px; font-size: 20px; font-weight: 800; }
  .content { color: #263149; font-size: 16px; line-height: 1.8; word-break: break-word; }
  .content img { max-width: 100%; height: auto; border-radius: 12px; }
  .content pre, .content code { white-space: pre-wrap; word-break: break-word; }
  .content table { width: 100%; border-collapse: collapse; display: block; overflow-x: auto; }
  .content table td, .content table th { border: 1px solid #d8deea; padding: 8px 10px; }
  .divider { height: 1px; margin: 0 30px; background: #edf0f5; }
  .footer { padding: 10px 30px 26px; color: #8b95a7; font-size: 13px; }
</style>
</head>
<body>
<div class="card">
  <div class="header">
    <div class="feed-tag">{{ feed_title or "RSS Entry" }}</div>
    <h1>{{ entry.title or "无标题" }}</h1>
    <div class="meta">
      第 {{ entry.index }} 条 / 共展示 {{ shown_count }} 条{% if total_count %}（源内共 {{ total_count }} 条）{% endif %}<br>
      {% if entry.author %}作者：{{ entry.author }}<br>{% endif %}
      {% if entry.published %}发布时间：{{ entry.published }}<br>{% elif entry.updated %}更新时间：{{ entry.updated }}<br>{% endif %}
      {% if entry.link %}链接：{{ entry.link }}<br>{% endif %}
      {% if entry.entry_id %}ID：{{ entry.entry_id }}<br>{% endif %}
      {% if entry.tags %}标签：{{ entry.tags|join(", ") }}{% endif %}
    </div>
  </div>
  <div class="section">
    <h2>Summary / Description</h2>
    <div class="content">
      {% if entry.summary_html %}{{ entry.summary_html | safe }}{% else %}<p>（无）</p>{% endif %}
    </div>
  </div>
  <div class="divider"></div>
  <div class="section">
    <h2>Full Content</h2>
    <div class="content">
      {% if entry.content_html %}{{ entry.content_html | safe }}{% else %}<p>（无）</p>{% endif %}
    </div>
  </div>
  <div class="footer">Rendered by astrbot_plugin_rss</div>
</div>
</body>
</html>
"""


class CronExpression:
    """Small 5-field cron matcher. Supports *, ranges, commas, and steps."""

    RANGES = (
        (0, 59, "分钟"),
        (0, 23, "小时"),
        (1, 31, "日"),
        (1, 12, "月"),
        (0, 6, "星期"),
    )

    def __init__(self, expr: str):
        self.expr = " ".join(expr.strip().split())
        fields = self.expr.split()
        if len(fields) != 5:
            raise ValueError("Cron 表达式必须是 5 段：分钟 小时 日 月 星期")
        self.minutes = self._parse_field(fields[0], *self.RANGES[0])
        self.hours = self._parse_field(fields[1], *self.RANGES[1])
        self.days = self._parse_field(fields[2], *self.RANGES[2])
        self.months = self._parse_field(fields[3], *self.RANGES[3])
        self.weekdays = self._parse_field(fields[4], *self.RANGES[4])

    def match(self, dt: datetime) -> bool:
        weekday = (dt.weekday() + 1) % 7  # Python: Mon=0; plugin cron: Sun=0
        return (
            dt.minute in self.minutes
            and dt.hour in self.hours
            and dt.day in self.days
            and dt.month in self.months
            and weekday in self.weekdays
        )

    @classmethod
    def _parse_field(cls, field: str, min_value: int, max_value: int, name: str) -> set[int]:
        result: set[int] = set()
        for part in field.split(","):
            part = part.strip()
            if not part:
                raise ValueError(f"{name}字段包含空项。")

            if "/" in part:
                base, step_raw = part.split("/", 1)
                try:
                    step = int(step_raw)
                except ValueError:
                    raise ValueError(f"{name}字段步长必须是整数：{part}")
                if step <= 0:
                    raise ValueError(f"{name}字段步长必须大于 0：{part}")
            else:
                base, step = part, 1

            if base == "*":
                start, end = min_value, max_value
            elif "-" in base:
                start_raw, end_raw = base.split("-", 1)
                start, end = cls._to_int(start_raw, name), cls._to_int(end_raw, name)
            else:
                start = cls._to_int(base, name)
                end = max_value if "/" in part else start

            if start < min_value or end > max_value or start > end:
                raise ValueError(f"{name}字段范围应为 {min_value}-{max_value}：{part}")

            result.update(range(start, end + 1, step))

        if not result:
            raise ValueError(f"{name}字段未解析出有效值。")
        return result

    @staticmethod
    def _to_int(raw: str, name: str) -> int:
        try:
            return int(raw)
        except ValueError:
            raise ValueError(f"{name}字段必须是整数、*、范围、步长或逗号分隔值：{raw}")


class RssPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig | None = None):
        super().__init__(context)
        self.config = config or {}
        self._lock = asyncio.Lock()
        self._cron_task: asyncio.Task | None = None
        self._data_path = Path(get_astrbot_data_path()) / "plugin_data" / PLUGIN_NAME
        self._data_file = self._data_path / "rss_data.json"
        self._data_path.mkdir(parents=True, exist_ok=True)
        self._data: dict[str, Any] = {"version": DATA_VERSION, "scopes": {}}
        self._load_data()
        self._ensure_cron_loop()

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
        self._ensure_cron_loop()
        yield event.plain_result(self._rss_help_text())

    @rsshub.command("help")
    async def rsshub_help(self, event: AstrMessageEvent):
        """查看 RSSHub 指令帮助。"""
        self._ensure_cron_loop()
        yield event.plain_result(self._rsshub_help_text())

    @rss.command("add")
    async def rss_add(self, event: AstrMessageEvent):
        """为当前会话添加 RSS 源：/rss add <url> [备注]。"""
        self._ensure_cron_loop()
        scope_id = self._scope_id(event)
        parts = self._split_command(event.message_str)
        if len(parts) < 3:
            yield event.plain_result("用法：/rss add <url> [备注]")
            return
        url = self._normalize_url(parts[2])
        note = " ".join(parts[3:]).strip()
        result = await self._add_rss_source(scope_id, url=url, note=note, source="manual")
        yield event.plain_result(result)

    @rss.command("rm")
    async def rss_rm(self, event: AstrMessageEvent):
        """删除当前会话的 RSS 源：/rss rm <index>。"""
        self._ensure_cron_loop()
        scope_id = self._scope_id(event)
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
            scope = self._get_scope_locked(scope_id)
            sources = scope["rss_sources"]
            if index < 1 or index > len(sources):
                yield event.plain_result(f"当前会话 RSS 源序号不存在：{index}")
                return
            removed = sources.pop(index - 1)
            before_jobs = len(scope["cron_jobs"])
            scope["cron_jobs"] = [job for job in scope["cron_jobs"] if job.get("source_id") != removed.get("id")]
            removed_jobs = before_jobs - len(scope["cron_jobs"])
            self._save_data()

        extra = f"\n同时删除关联定时任务 {removed_jobs} 个。" if removed_jobs else ""
        yield event.plain_result(f"已删除当前会话 RSS 源 #{index}：{removed.get('title') or removed.get('url')}{extra}")

    @rss.command("addby")
    async def rss_addby(self, event: AstrMessageEvent):
        """根据 RSSHub 源为当前会话添加路由：/rss addby <rsshub_index> <routing> [备注]。"""
        self._ensure_cron_loop()
        scope_id = self._scope_id(event)
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
            scope = self._get_scope_locked(scope_id)
            hubs = scope["rsshub_sources"]
            if hub_index < 1 or hub_index > len(hubs):
                yield event.plain_result(f"当前会话 RSSHub 源序号不存在：{hub_index}")
                return
            hub = hubs[hub_index - 1].copy()

        url = self._build_rsshub_url(hub["url"], routing)
        if not note:
            note = f"RSSHub #{hub_index}: {routing}"

        result = await self._add_rss_source(
            scope_id,
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
        """删除当前会话的某个 RSSHub 源下创建的所有 RSS 源：/rss rmby <rsshub_index>。"""
        self._ensure_cron_loop()
        scope_id = self._scope_id(event)
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
            scope = self._get_scope_locked(scope_id)
            hubs = scope["rsshub_sources"]
            if hub_index < 1 or hub_index > len(hubs):
                yield event.plain_result(f"当前会话 RSSHub 源序号不存在：{hub_index}")
                return

            hub = hubs[hub_index - 1]
            hub_id = hub["id"]
            removed_source_ids = {item["id"] for item in scope["rss_sources"] if item.get("hub_id") == hub_id}
            before = len(scope["rss_sources"])
            scope["rss_sources"] = [item for item in scope["rss_sources"] if item.get("hub_id") != hub_id]
            before_jobs = len(scope["cron_jobs"])
            scope["cron_jobs"] = [job for job in scope["cron_jobs"] if job.get("source_id") not in removed_source_ids]
            removed_count = before - len(scope["rss_sources"])
            removed_jobs = before_jobs - len(scope["cron_jobs"])
            self._save_data()

        yield event.plain_result(f"已删除当前会话 RSSHub #{hub_index} 下的 {removed_count} 个 RSS 源，并删除关联定时任务 {removed_jobs} 个。")

    @rss.command("get")
    async def rss_get(self, event: AstrMessageEvent):
        """获取 RSS 源信息：/rss get <index>。"""
        self._ensure_cron_loop()
        scope_id = self._scope_id(event)
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
            scope = self._get_scope_locked(scope_id)
            sources = scope["rss_sources"]
            if index < 1 or index > len(sources):
                yield event.plain_result(f"当前会话 RSS 源序号不存在：{index}")
                return
            source = sources[index - 1].copy()

        feed = await self._fetch_feed(source["url"])
        if not feed["ok"]:
            yield event.plain_result(f"获取失败：{feed['error']}\nURL：{source['url']}")
            return

        await self._update_source_meta(scope_id, source["id"], feed)

        total_entries = len(feed["entries"])
        max_entries = self._max_get_entries()
        entries = feed["entries"][:max_entries]

        header_lines = [
            f"RSS #{index} 总信息（当前会话）",
            f"标题：{feed['title'] or source.get('title') or '未命名'}",
            f"链接：{feed['link'] or source['url']}",
            f"备注：{source.get('note') or '无'}",
            f"源地址：{source['url']}",
            f"描述：{feed.get('subtitle') or '无'}",
            f"源内条目总数：{total_entries}",
            f"本次展示条数：{len(entries)}（配置上限：{max_entries}）",
        ]
        if source.get("hub_base"):
            header_lines.append(f"RSSHub：{source.get('hub_base')} · 路由：{source.get('routing') or '-'}")
        yield event.plain_result("\n".join(header_lines))

        for item_index, entry in enumerate(entries, 1):
            yield event.plain_result(self._entry_meta_text(entry, item_index))
            image_url = await self._render_entry_image(feed, entry, item_index, len(entries), total_entries)
            if image_url:
                yield event.image_result(image_url)
            else:
                yield event.plain_result(self._entry_text_fallback(entry))

        if total_entries > max_entries:
            yield event.plain_result(
                f"提醒：该 RSS 源共有 {total_entries} 条内容，本次仅展示前 {max_entries} 条。"
                f"如需调整，请在插件配置中修改 max_get_entries。"
            )

    @rss.command("list")
    async def rss_list(self, event: AstrMessageEvent):
        """列出当前会话的所有 RSS 源，并渲染成图片。"""
        self._ensure_cron_loop()
        scope_id = self._scope_id(event)
        async with self._lock:
            scope = self._get_scope_locked(scope_id)
            sources = [item.copy() for item in scope["rss_sources"]]
            hubs = {hub["id"]: hub for hub in scope["rsshub_sources"]}

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

        try:
            image_url = await self.html_render(
                RSS_LIST_TEMPLATE,
                {"items": items, "count": len(items), "generated_at": self._now_local_text()},
                options={"full_page": True, "type": "png"},
            )
            yield event.image_result(image_url)
        except Exception as exc:
            logger.error(f"RSS list render failed: {exc}")
            yield event.plain_result(self._format_rss_list_text(sources, hubs))

    @rss.command("check")
    async def rss_check(self, event: AstrMessageEvent):
        """检查当前会话的 RSS 源可用性和最新条目：/rss check <index|all> [limit]。"""
        self._ensure_cron_loop()
        scope_id = self._scope_id(event)
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
            scope = self._get_scope_locked(scope_id)
            sources = [item.copy() for item in scope["rss_sources"]]

        if not sources:
            yield event.plain_result("当前会话暂无 RSS 源。")
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
                yield event.plain_result(f"当前会话 RSS 源序号不存在：{index}")
                return
            selected = [(index, sources[index - 1])]

        reports = []
        for index, source in selected:
            feed = await self._fetch_feed(source["url"])
            if not feed["ok"]:
                await self._mark_source_error(scope_id, source["id"], feed["error"])
                reports.append(f"#{index} ❌ {source.get('title') or source['url']}\n错误：{feed['error']}")
                continue

            await self._update_source_meta(scope_id, source["id"], feed, update_last_entry=True)
            latest = feed["entries"][:limit]
            title = feed["title"] or source.get("title") or source["url"]
            block = [f"#{index} ✅ {title}", f"条目数：{len(feed['entries'])}"]
            for i, entry in enumerate(latest, 1):
                block.append(f"{i}. {entry.get('title') or '无标题'}")
                if entry.get("link"):
                    block.append(f"   {entry['link']}")
            reports.append("\n".join(block))

        yield event.plain_result("\n\n".join(reports))

    @rss.command("cron")
    async def rss_cron(self, event: AstrMessageEvent):
        """为当前会话添加一个 RSS 源的定时任务。"""
        self._ensure_cron_loop()
        scope_id = self._scope_id(event)
        parts = self._split_command(event.message_str)
        if len(parts) < 4:
            yield event.plain_result(
                "用法：/rss cron <rss_index> <cron_expr>\n"
                "示例：/rss cron 1 0/5 * * * *\n"
                "Cron 格式：分钟 小时 日 月 星期；星期 0 表示星期天。"
            )
            return
        try:
            source_index = self._parse_index(parts[2])
            cron_expr = self._parse_cron_expr_from_parts(parts[3:])
            CronExpression(cron_expr)
        except ValueError as exc:
            yield event.plain_result(str(exc))
            return

        async with self._lock:
            scope = self._get_scope_locked(scope_id)
            sources = scope["rss_sources"]
            if source_index < 1 or source_index > len(sources):
                yield event.plain_result(f"当前会话 RSS 源序号不存在：{source_index}")
                return
            source = sources[source_index - 1].copy()

        feed = await self._fetch_feed(source["url"])
        if not feed["ok"]:
            yield event.plain_result(f"定时任务添加失败：RSS 源当前不可用。\n错误：{feed['error']}")
            return

        latest_id = self._entry_id(feed["entries"][0]) if feed.get("entries") else ""
        now = self._now_iso()
        job = {
            "id": self._new_id(),
            "source_id": source["id"],
            "cron_expr": cron_expr,
            "created_at": now,
            "enabled": True,
            "last_run_minute": "",
            "last_entry_id": latest_id,
            "last_error": "",
        }

        async with self._lock:
            scope = self._get_scope_locked(scope_id)
            scope["cron_jobs"].append(job)
            for item in scope["rss_sources"]:
                if item["id"] == source["id"]:
                    item["title"] = feed.get("title") or item.get("title") or ""
                    item["link"] = feed.get("link") or item.get("link") or ""
                    item["last_checked"] = now
                    item["last_entry_id"] = latest_id
                    item["last_error"] = ""
                    break
            self._save_data()
            job_index = len(scope["cron_jobs"])

        yield event.plain_result(
            f"已为当前会话 RSS 源 #{source_index} 添加定时任务 #{job_index}。\n"
            f"RSS：{feed.get('title') or source.get('title') or source['url']}\n"
            f"Cron：{cron_expr}\n"
            f"说明：已将当前最新条目设为基准，之后只推送新内容。"
        )

    @rss.command("cronlist")
    async def rss_cronlist(self, event: AstrMessageEvent):
        """列出当前会话的 RSS 定时任务。"""
        self._ensure_cron_loop()
        scope_id = self._scope_id(event)
        async with self._lock:
            scope = self._get_scope_locked(scope_id)
            jobs = [job.copy() for job in scope["cron_jobs"]]
            sources = {item["id"]: item for item in scope["rss_sources"]}

        if not jobs:
            yield event.plain_result("当前会话暂无 RSS 定时任务。使用 /rss cron <index> <cron_expr> 添加。")
            return

        lines = ["当前会话 RSS 定时任务："]
        for i, job in enumerate(jobs, 1):
            source = sources.get(job.get("source_id"), {})
            title = source.get("title") or source.get("url") or "源已不存在"
            status = "启用" if job.get("enabled", True) else "停用"
            err = f"｜最近错误：{job['last_error']}" if job.get("last_error") else ""
            lines.append(
                f"{i}. {status}｜{job.get('cron_expr')}｜{title}"
                f"｜上次触发：{job.get('last_run_minute') or '无'}{err}"
            )
        yield event.plain_result("\n".join(lines))

    @rss.command("cronrm")
    async def rss_cronrm(self, event: AstrMessageEvent):
        """删除当前会话的 RSS 定时任务。"""
        self._ensure_cron_loop()
        scope_id = self._scope_id(event)
        parts = self._split_command(event.message_str)
        if len(parts) < 3:
            yield event.plain_result("用法：/rss cronrm <cron_index>")
            return
        try:
            index = self._parse_index(parts[2])
        except ValueError as exc:
            yield event.plain_result(str(exc))
            return
        async with self._lock:
            scope = self._get_scope_locked(scope_id)
            jobs = scope["cron_jobs"]
            if index < 1 or index > len(jobs):
                yield event.plain_result(f"当前会话定时任务序号不存在：{index}")
                return
            removed = jobs.pop(index - 1)
            self._save_data()
        yield event.plain_result(f"已删除当前会话定时任务 #{index}：{removed.get('cron_expr')}")

    @rsshub.command("add")
    async def rsshub_add(self, event: AstrMessageEvent):
        """为当前会话添加 RSSHub 源：/rsshub add <url> [备注]。"""
        self._ensure_cron_loop()
        scope_id = self._scope_id(event)
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
            scope = self._get_scope_locked(scope_id)
            hubs = scope["rsshub_sources"]
            if not self._allow_duplicate() and any(item["url"] == url for item in hubs):
                yield event.plain_result(f"当前会话 RSSHub 源已存在：{url}")
                return
            hubs.append({
                "id": self._new_id(),
                "url": url.rstrip("/"),
                "note": note,
                "created_at": self._now_iso(),
                "last_error": "",
            })
            self._save_data()
            index = len(hubs)
        yield event.plain_result(f"已添加当前会话 RSSHub 源 #{index}：{note or url}")

    @rsshub.command("rm")
    async def rsshub_rm(self, event: AstrMessageEvent):
        """删除当前会话的 RSSHub 源：/rsshub rm <index>。不会自动删除该 Hub 下已创建的 RSS 源。"""
        self._ensure_cron_loop()
        scope_id = self._scope_id(event)
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
            scope = self._get_scope_locked(scope_id)
            hubs = scope["rsshub_sources"]
            if index < 1 or index > len(hubs):
                yield event.plain_result(f"当前会话 RSSHub 源序号不存在：{index}")
                return
            removed = hubs.pop(index - 1)
            self._save_data()
        yield event.plain_result(
            f"已删除当前会话 RSSHub 源 #{index}：{removed.get('note') or removed.get('url')}\n"
            f"提示：如需删除该 Hub 下已创建的 RSS 源，请先使用 /rss rmby <index>。"
        )

    @rsshub.command("list")
    async def rsshub_list(self, event: AstrMessageEvent):
        """列出当前会话的所有 RSSHub 源。"""
        self._ensure_cron_loop()
        scope_id = self._scope_id(event)
        async with self._lock:
            scope = self._get_scope_locked(scope_id)
            hubs = [item.copy() for item in scope["rsshub_sources"]]
            rss_sources = [item.copy() for item in scope["rss_sources"]]

        if not hubs:
            yield event.plain_result("当前会话暂无 RSSHub 源。使用 /rsshub add <url> [备注] 添加。")
            return

        counts: dict[str, int] = {}
        for source in rss_sources:
            hub_id = source.get("hub_id")
            if hub_id:
                counts[hub_id] = counts.get(hub_id, 0) + 1

        lines = ["当前会话 RSSHub 源列表："]
        for i, hub in enumerate(hubs, 1):
            note = f"｜{hub['note']}" if hub.get("note") else ""
            lines.append(f"{i}. {hub['url']}{note}｜已创建 RSS：{counts.get(hub['id'], 0)}")
        yield event.plain_result("\n".join(lines))

    async def terminate(self):
        """插件卸载/停用时调用。"""
        if self._cron_task:
            self._cron_task.cancel()
            try:
                await self._cron_task
            except asyncio.CancelledError:
                pass
        async with self._lock:
            self._save_data()

    async def _add_rss_source(
        self,
        scope_id: str,
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
            scope = self._get_scope_locked(scope_id)
            sources = scope["rss_sources"]
            if not self._allow_duplicate() and any(item["url"] == url for item in sources):
                return f"当前会话 RSS 源已存在：{url}"

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
        return f"已添加当前会话 RSS 源 #{index}：{title}{note_text}\nURL：{url}"

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
            summary_html = str(entry.get("summary") or entry.get("description") or "")
            content_blocks = entry.get("content") or []
            content_html_parts = []

            if isinstance(content_blocks, list):
                for block in content_blocks:
                    value = ""
                    if hasattr(block, "get"):
                        value = str(block.get("value") or "")
                    else:
                        value = str(block or "")
                    if value:
                        content_html_parts.append(value)

            content_html = "\n<hr>\n".join(content_html_parts) if content_html_parts else summary_html
            summary_html = self._sanitize_feed_html(summary_html)
            content_html = self._sanitize_feed_html(content_html)

            tags = []
            for tag in entry.get("tags", []) or []:
                if hasattr(tag, "get") and tag.get("term"):
                    tags.append(str(tag.get("term")))
                elif tag:
                    tags.append(str(tag))

            entries.append({
                "id": str(entry.get("id") or entry.get("guid") or entry.get("link") or entry.get("title") or ""),
                "title": str(entry.get("title") or ""),
                "link": str(entry.get("link") or ""),
                "published": str(entry.get("published") or ""),
                "updated": str(entry.get("updated") or ""),
                "author": str(entry.get("author") or ""),
                "summary_html": summary_html,
                "summary_text": self._strip_html(summary_html),
                "content_html": content_html,
                "content_text": self._strip_html(content_html),
                "tags": tags,
            })

        title = str(parsed.feed.get("title") or "")
        link = str(parsed.feed.get("link") or "")
        subtitle = str(parsed.feed.get("subtitle") or parsed.feed.get("description") or "")

        if not title and not entries:
            error = "未解析到有效 feed 标题或条目"
            if getattr(parsed, "bozo", False) and getattr(parsed, "bozo_exception", None):
                error += f"：{parsed.bozo_exception}"
            return {"ok": False, "error": error}

        return {
            "ok": True,
            "title": title,
            "link": link,
            "subtitle": self._strip_html(subtitle),
            "entries": entries,
            "raw_bozo": bool(getattr(parsed, "bozo", False)),
        }

    async def _render_entry_image(
        self,
        feed: dict[str, Any],
        entry: dict[str, Any],
        item_index: int,
        shown_count: int,
        total_count: int,
    ) -> str:
        render_data = {
            "feed_title": feed.get("title") or "RSS Entry",
            "shown_count": shown_count,
            "total_count": total_count,
            "entry": {
                "index": item_index,
                "title": entry.get("title") or "",
                "link": entry.get("link") or "",
                "author": entry.get("author") or "",
                "published": entry.get("published") or "",
                "updated": entry.get("updated") or "",
                "entry_id": entry.get("id") or "",
                "tags": entry.get("tags") or [],
                "summary_html": entry.get("summary_html") or "",
                "content_html": entry.get("content_html") or entry.get("summary_html") or "",
            },
        }
        try:
            return await self.html_render(
                RSS_ENTRY_TEMPLATE,
                render_data,
                options={"full_page": True, "type": "png"},
            )
        except Exception as exc:
            logger.error(f"RSS entry render failed: {exc}")
            return ""

    async def _update_source_meta(
        self,
        scope_id: str,
        source_id: str,
        feed: dict[str, Any],
        update_last_entry: bool = False,
    ) -> None:
        async with self._lock:
            scope = self._get_scope_locked(scope_id)
            for item in scope["rss_sources"]:
                if item["id"] == source_id:
                    item["title"] = feed.get("title") or item.get("title") or ""
                    item["link"] = feed.get("link") or item.get("link") or ""
                    item["last_checked"] = self._now_iso()
                    item["last_error"] = ""
                    if update_last_entry and feed.get("entries"):
                        item["last_entry_id"] = self._entry_id(feed["entries"][0])
                    self._save_data()
                    break

    async def _mark_source_error(self, scope_id: str, source_id: str, error: str) -> None:
        async with self._lock:
            scope = self._get_scope_locked(scope_id)
            for item in scope["rss_sources"]:
                if item["id"] == source_id:
                    item["last_checked"] = self._now_iso()
                    item["last_error"] = error[:300]
                    self._save_data()
                    break

    def _ensure_cron_loop(self) -> None:
        if self._cron_task and not self._cron_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
            self._cron_task = loop.create_task(self._cron_loop())
        except RuntimeError:
            logger.warning("RSS cron loop not started: no running event loop yet.")

    async def _cron_loop(self) -> None:
        while True:
            try:
                now = datetime.now(timezone.utc).astimezone().replace(second=0, microsecond=0)
                await self._run_due_cron_jobs(now)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"RSS cron loop error: {exc}")

            current = datetime.now(timezone.utc).astimezone()
            sleep_seconds = max(1, 60 - current.second)
            await asyncio.sleep(sleep_seconds)

    async def _run_due_cron_jobs(self, now: datetime) -> None:
        minute_key = now.strftime("%Y-%m-%d %H:%M")
        due: list[tuple[str, str]] = []

        async with self._lock:
            for scope_id, scope in self._data.get("scopes", {}).items():
                for job in scope.get("cron_jobs", []):
                    if not job.get("enabled", True):
                        continue
                    if job.get("last_run_minute") == minute_key:
                        continue
                    try:
                        cron = CronExpression(str(job.get("cron_expr", "")))
                    except ValueError as exc:
                        job["last_error"] = str(exc)
                        continue
                    if cron.match(now):
                        job["last_run_minute"] = minute_key
                        due.append((scope_id, job["id"]))
            if due:
                self._save_data()

        for scope_id, job_id in due:
            try:
                await self._execute_cron_job(scope_id, job_id)
            except Exception as exc:
                logger.error(f"RSS cron job failed: {exc}")
                async with self._lock:
                    scope = self._get_scope_locked(scope_id)
                    for job in scope["cron_jobs"]:
                        if job["id"] == job_id:
                            job["last_error"] = str(exc)[:300]
                            self._save_data()
                            break

    async def _execute_cron_job(self, scope_id: str, job_id: str) -> None:
        async with self._lock:
            scope = self._get_scope_locked(scope_id)
            job = next((item.copy() for item in scope["cron_jobs"] if item["id"] == job_id), None)
            if not job or not job.get("enabled", True):
                return
            source = next((item.copy() for item in scope["rss_sources"] if item["id"] == job.get("source_id")), None)
            if not source:
                for item in scope["cron_jobs"]:
                    if item["id"] == job_id:
                        item["last_error"] = "关联 RSS 源已不存在"
                        item["enabled"] = False
                        self._save_data()
                        break
                return

        feed = await self._fetch_feed(source["url"])
        if not feed["ok"]:
            async with self._lock:
                scope = self._get_scope_locked(scope_id)
                for item in scope["rss_sources"]:
                    if item["id"] == source["id"]:
                        item["last_checked"] = self._now_iso()
                        item["last_error"] = feed["error"][:300]
                        break
                for item in scope["cron_jobs"]:
                    if item["id"] == job_id:
                        item["last_error"] = feed["error"][:300]
                        break
                self._save_data()
            return

        entries = feed.get("entries", [])
        latest_id = self._entry_id(entries[0]) if entries else ""
        ref_id = job.get("last_entry_id") or source.get("last_entry_id") or ""
        new_entries = self._entries_before_ref(entries, ref_id)

        if not ref_id:
            new_entries = []

        max_push = self._max_push_entries()
        send_entries = new_entries[:max_push]

        if send_entries:
            title = feed.get("title") or source.get("title") or source.get("url")
            await self._send_active_plain(
                scope_id,
                f"RSS 定时推送：{title}\n"
                f"检测到 {len(new_entries)} 条新内容，本次推送 {len(send_entries)} 条。\n"
                f"源地址：{source.get('url')}",
            )
            for i, entry in enumerate(send_entries, 1):
                await self._send_active_plain(scope_id, self._entry_meta_text(entry, i))
                image_url = await self._render_entry_image(feed, entry, i, len(send_entries), len(entries))
                if image_url:
                    await self._send_active_image(scope_id, image_url)
                else:
                    await self._send_active_plain(scope_id, self._entry_text_fallback(entry))

            if len(new_entries) > max_push:
                await self._send_active_plain(
                    scope_id,
                    f"提醒：本次检测到 {len(new_entries)} 条新内容，因 max_push_entries={max_push}，"
                    f"仅推送前 {max_push} 条。"
                )

        async with self._lock:
            scope = self._get_scope_locked(scope_id)
            for item in scope["rss_sources"]:
                if item["id"] == source["id"]:
                    item["title"] = feed.get("title") or item.get("title") or ""
                    item["link"] = feed.get("link") or item.get("link") or ""
                    item["last_checked"] = self._now_iso()
                    item["last_error"] = ""
                    if latest_id:
                        item["last_entry_id"] = latest_id
                    break
            for item in scope["cron_jobs"]:
                if item["id"] == job_id:
                    item["last_error"] = ""
                    if latest_id:
                        item["last_entry_id"] = latest_id
                    break
            self._save_data()

    def _entries_before_ref(self, entries: list[dict[str, Any]], ref_id: str) -> list[dict[str, Any]]:
        if not entries:
            return []
        if not ref_id:
            return []
        new_entries = []
        for entry in entries:
            if self._entry_id(entry) == ref_id:
                return new_entries
            new_entries.append(entry)
        return new_entries

    async def _send_active_plain(self, unified_msg_origin: str, text: str) -> None:
        await self.context.send_message(unified_msg_origin, MessageChain().message(text))

    async def _send_active_image(self, unified_msg_origin: str, image_path_or_url: str) -> None:
        await self.context.send_message(unified_msg_origin, MessageChain().file_image(image_path_or_url))

    def _load_data(self) -> None:
        if not self._data_file.exists():
            self._save_data()
            return
        try:
            loaded = json.loads(self._data_file.read_text(encoding="utf-8"))
            self._data = self._normalize_data(loaded)
            self._save_data()
        except Exception as exc:
            logger.error(f"Failed to load RSS plugin data: {exc}")
            backup = self._data_file.with_suffix(f".broken.{int(datetime.now().timestamp())}.json")
            try:
                self._data_file.rename(backup)
            except Exception:
                pass
            self._data = {"version": DATA_VERSION, "scopes": {}}
            self._save_data()

    def _normalize_data(self, loaded: Any) -> dict[str, Any]:
        if not isinstance(loaded, dict):
            raise ValueError("data root is not dict")

        if "scopes" in loaded and isinstance(loaded["scopes"], dict):
            data = {"version": DATA_VERSION, "scopes": loaded["scopes"]}
        else:
            # v0.1.1 and earlier were global. Keep them in a legacy scope to avoid data loss,
            # but new commands use the current unified_msg_origin scope.
            legacy_scope = {
                "rss_sources": loaded.get("rss_sources", []) if isinstance(loaded.get("rss_sources", []), list) else [],
                "rsshub_sources": loaded.get("rsshub_sources", []) if isinstance(loaded.get("rsshub_sources", []), list) else [],
                "cron_jobs": [],
            }
            data = {"version": DATA_VERSION, "scopes": {"__legacy_global__": legacy_scope}}

        for scope in data["scopes"].values():
            if not isinstance(scope, dict):
                continue
            scope.setdefault("rss_sources", [])
            scope.setdefault("rsshub_sources", [])
            scope.setdefault("cron_jobs", [])
            for job in scope["cron_jobs"]:
                job.setdefault("enabled", True)
                job.setdefault("last_error", "")
                job.setdefault("last_entry_id", "")
                job.setdefault("last_run_minute", "")
        return data

    def _save_data(self) -> None:
        self._data_path.mkdir(parents=True, exist_ok=True)
        tmp = self._data_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self._data_file)

    def _scope_id(self, event: AstrMessageEvent) -> str:
        origin = getattr(event, "unified_msg_origin", "") or ""
        if origin:
            return origin
        try:
            sender = event.get_sender_id()
        except Exception:
            sender = "unknown"
        return f"fallback:{sender}"

    def _get_scope_locked(self, scope_id: str) -> dict[str, Any]:
        scopes = self._data.setdefault("scopes", {})
        if scope_id not in scopes:
            scopes[scope_id] = {"rss_sources": [], "rsshub_sources": [], "cron_jobs": []}
        scope = scopes[scope_id]
        scope.setdefault("rss_sources", [])
        scope.setdefault("rsshub_sources", [])
        scope.setdefault("cron_jobs", [])
        return scope

    def _parse_cron_expr_from_parts(self, parts: list[str]) -> str:
        if len(parts) == 1:
            fields = parts[0].split()
        else:
            fields = parts
        if len(fields) != 5:
            raise ValueError("Cron 表达式必须刚好包含 5 段，例如：/rss cron 1 0/5 * * * *")
        return " ".join(fields)

    def _validate_on_add(self) -> bool:
        return bool(self.config.get("validate_on_add", True))

    def _allow_duplicate(self) -> bool:
        return bool(self.config.get("allow_duplicate", False))

    def _max_get_entries(self) -> int:
        try:
            value = int(self.config.get("max_get_entries", 5))
        except Exception:
            value = 5
        return max(1, min(value, 20))

    def _max_push_entries(self) -> int:
        try:
            value = int(self.config.get("max_push_entries", 5))
        except Exception:
            value = 5
        return max(1, min(value, 20))

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
            return message.split()

    def _entry_id(self, entry: dict[str, Any]) -> str:
        return str(entry.get("id") or entry.get("link") or entry.get("title") or "")

    def _strip_html(self, text: str) -> str:
        text = re.sub(r"<[^>]+>", "", text)
        return html.unescape(re.sub(r"\s+", " ", text)).strip()

    def _sanitize_feed_html(self, text: str) -> str:
        text = re.sub(r"(?is)<(script|style|iframe|object|embed|meta|link)[^>]*>.*?</\1>", "", text)
        text = re.sub(r"(?is)<(script|style|iframe|object|embed|meta|link)[^>]*/?>", "", text)
        text = re.sub(r"(?i)\s+on[a-z]+\s*=\s*(['\"]).*?\1", "", text)
        text = re.sub(r"(?i)\s+on[a-z]+\s*=\s*[^\s>]+", "", text)
        text = re.sub(r"(?i)(href|src)\s*=\s*(['\"])javascript:.*?\2", r'\1="#"', text)
        return text

    def _entry_meta_text(self, entry: dict[str, Any], item_index: int) -> str:
        meta_lines = [
            f"第 {item_index} 条",
            f"标题：{entry.get('title') or '无标题'}",
            f"链接：{entry.get('link') or '无'}",
            f"作者：{entry.get('author') or '无'}",
            f"发布时间：{entry.get('published') or '无'}",
            f"更新时间：{entry.get('updated') or '无'}",
            f"ID：{entry.get('id') or '无'}",
        ]
        if entry.get("tags"):
            meta_lines.append(f"标签：{', '.join(entry['tags'])}")
        if entry.get("summary_text"):
            meta_lines.append(f"摘要：{entry['summary_text'][:200]}")
        return "\n".join(meta_lines)

    def _entry_text_fallback(self, entry: dict[str, Any]) -> str:
        return (
            "内容图片渲染失败，以下为文本预览：\n"
            f"标题：{entry.get('title') or '无标题'}\n"
            f"链接：{entry.get('link') or '无'}\n\n"
            f"Summary / Description:\n{entry.get('summary_text') or '（无）'}\n\n"
            f"Full Content:\n{entry.get('content_text') or entry.get('summary_text') or '（无）'}"
        )

    def _new_id(self) -> str:
        return uuid.uuid4().hex

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    def _now_local_text(self) -> str:
        return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")

    def _format_rss_list_text(self, sources: list[dict[str, Any]], hubs: dict[str, dict[str, Any]]) -> str:
        if not sources:
            return "当前会话暂无 RSS 源。使用 /rss add <url> [备注] 添加。"
        lines = ["当前会话 RSS 源列表："]
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
            "RSS 插件指令（按当前群 / 当前会话独立保存）：\n"
            "/rss add <url> [备注] - 添加 RSS 源\n"
            "/rss rm <index> - 删除第 index 个 RSS 源，并删除关联定时任务\n"
            "/rss addby <rsshub_index> <routing> [备注] - 使用 RSSHub 源和路由添加 RSS\n"
            "/rss rmby <rsshub_index> - 删除某个 RSSHub 源下创建的所有 RSS 源\n"
            "/rss get <index> - 获取第 index 个 RSS 源详情，并逐条发送内容\n"
            "/rss list - 以图片列出当前会话全部 RSS 源\n"
            "/rss check <index|all> [limit] - 检查 RSS 源并显示最新条目\n"
            "/rss cron <index> <cron_expr> - 为 RSS 源添加定时轮询\n"
            "/rss cronlist - 查看当前会话定时任务\n"
            "/rss cronrm <cron_index> - 删除当前会话定时任务\n"
            "/rss help - 查看帮助\n\n"
            "Cron 格式：分钟 小时 日 月 星期；星期 0 表示星期天。\n"
            "示例：/rss cron 1 0/5 * * * *"
        )

    def _rsshub_help_text(self) -> str:
        return (
            "RSSHub 插件指令（按当前群 / 当前会话独立保存）：\n"
            "/rsshub add <url> [备注] - 添加 RSSHub 基础地址\n"
            "/rsshub rm <index> - 删除第 index 个 RSSHub 源\n"
            "/rsshub list - 列出当前会话 RSSHub 源\n"
            "/rsshub help - 查看帮助\n\n"
            "示例：\n"
            "/rsshub add https://rsshub.app 官方 RSSHub\n"
            "/rss addby 1 /github/issue/DIYgod/RSSHub RSSHub Issues"
        )
