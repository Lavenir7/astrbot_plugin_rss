import asyncio
import hashlib
import html
import ipaddress
import json
import re
import time
import uuid
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import feedparser
import httpx

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register
from astrbot.core.utils.astrbot_path import get_astrbot_data_path

PLUGIN_NAME = "astrbot_plugin_rss"
PLUGIN_VERSION = "0.1.2"
DATA_FILE = "rss_data.json"

LIST_TEMPLATE = """
<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<style>
body { margin: 0; padding: 28px; background: #f7f7fb; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; color: #222; }
.card { width: 920px; background: #fff; border-radius: 22px; box-shadow: 0 12px 36px rgba(20,20,50,.12); padding: 28px; }
h1 { margin: 0 0 8px; font-size: 30px; }
.sub { color: #666; margin-bottom: 22px; }
.item { border-top: 1px solid #ececf2; padding: 16px 0; }
.idx { display: inline-block; min-width: 54px; color: #777; font-weight: 700; }
.badge { display: inline-block; padding: 3px 9px; border-radius: 999px; font-size: 13px; background: #eef2ff; color: #4f46e5; margin-right: 8px; }
.url { color: #555; font-size: 14px; word-break: break-all; margin-top: 6px; }
.note { color: #333; margin-top: 4px; }
.empty { padding: 28px; text-align: center; color: #777; border: 1px dashed #ccc; border-radius: 16px; }
</style>
</head>
<body>
<div class="card">
  <h1>{{ title }}</h1>
  <div class="sub">本会话源优先显示，全局源追加到最后。</div>
  {% if items %}
    {% for item in items %}
    <div class="item">
      <div><span class="idx">#{{ item.display_index }}</span><span class="badge">{{ item.scope_label }}</span><strong>{{ item.title }}</strong></div>
      <div class="url">{{ item.url }}</div>
      {% if item.note %}<div class="note">备注：{{ item.note }}</div>{% endif %}
    </div>
    {% endfor %}
  {% else %}
    <div class="empty">暂无 RSS 源</div>
  {% endif %}
</div>
</body>
</html>
"""


def _now_ts() -> int:
    return int(time.time())


def _new_id() -> str:
    return uuid.uuid4().hex[:12]


def _norm_url(url: str) -> str:
    return (url or "").strip()


def _strip_html(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = re.sub(r"<script[\s\S]*?</script>", " ", text, flags=re.I)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _clip_text(text: str, limit: int) -> str:
    text = _strip_html(text)
    limit = max(1, int(limit or 50))
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "…"


def _safe_title(text: str, fallback: str = "未命名") -> str:
    text = _strip_html(text)
    return text or fallback


def _entry_text(entry: Any) -> str:
    parts: List[str] = []
    for key in ("description", "summary"):
        val = entry.get(key)
        if val:
            parts.append(str(val))
    for content in entry.get("content", []) or []:
        if isinstance(content, dict) and content.get("value"):
            parts.append(str(content.get("value")))
    if not parts:
        for key in ("title", "link", "id"):
            val = entry.get(key)
            if val:
                parts.append(str(val))
    return _strip_html("\n".join(parts))


def _entry_id(entry: Any) -> str:
    raw = "|".join(
        str(entry.get(k, "")) for k in ("id", "guid", "link", "title", "published", "updated")
    )
    if not raw.strip("|"):
        raw = repr(entry)
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()


def _parse_tags(entry: Any) -> str:
    tags = entry.get("tags") or []
    names: List[str] = []
    for tag in tags:
        if isinstance(tag, dict):
            term = tag.get("term") or tag.get("label")
            if term:
                names.append(str(term))
    return ", ".join(names)


class CronParseError(ValueError):
    pass


def _parse_cron_field(expr: str, min_value: int, max_value: int) -> set:
    expr = (expr or "").strip()
    if not expr:
        raise CronParseError("Cron 字段不能为空")
    values = set()
    for part in expr.split(","):
        part = part.strip()
        if not part:
            raise CronParseError("Cron 字段包含空片段")
        step = 1
        if "/" in part:
            base, step_s = part.split("/", 1)
            if not step_s.isdigit() or int(step_s) <= 0:
                raise CronParseError(f"步长无效：{part}")
            step = int(step_s)
        else:
            base = part
        if base == "*" or base == "":
            start, end = min_value, max_value
        elif "-" in base:
            start_s, end_s = base.split("-", 1)
            if not (start_s.isdigit() and end_s.isdigit()):
                raise CronParseError(f"范围无效：{part}")
            start, end = int(start_s), int(end_s)
        elif base.isdigit():
            start = int(base)
            end = max_value if "/" in part else start
        else:
            raise CronParseError(f"字段无效：{part}")
        if start < min_value or end > max_value or start > end:
            raise CronParseError(f"字段超出范围：{part}，允许 {min_value}-{max_value}")
        values.update(range(start, end + 1, step))
    return values


def _cron_matches(expr: str, dt: datetime) -> bool:
    fields = expr.split()
    if len(fields) != 5:
        raise CronParseError("Cron 表达式必须是 5 段：分钟 小时 日 月 星期")
    minute, hour, day, month, weekday = fields
    py_weekday_as_cron = (dt.weekday() + 1) % 7
    return (
        dt.minute in _parse_cron_field(minute, 0, 59)
        and dt.hour in _parse_cron_field(hour, 0, 23)
        and dt.day in _parse_cron_field(day, 1, 31)
        and dt.month in _parse_cron_field(month, 1, 12)
        and py_weekday_as_cron in _parse_cron_field(weekday, 0, 6)
    )


def _validate_cron(expr: str) -> str:
    expr = re.sub(r"\s+", " ", (expr or "").strip())
    _cron_matches(expr, datetime.now())
    return expr


def _default_data() -> Dict[str, Any]:
    return {
        "schema_version": 3,
        "global": {"rss": [], "rsshub": []},
        "sessions": {},
        "__legacy_global__": {"rss": [], "rsshub": [], "crons": []},
    }


def _ensure_ids(items: List[Dict[str, Any]]) -> None:
    for item in items:
        if "id" not in item or not item["id"]:
            item["id"] = _new_id()
        if "created_at" not in item:
            item["created_at"] = _now_ts()


@register(PLUGIN_NAME, "L'avenir", "RSS 订阅", PLUGIN_VERSION)
class AstrBotPluginRSS(Star):
    def __init__(self, context: Context, config: Optional[Dict[str, Any]] = None):
        super().__init__(context)
        self.config = config or {}
        self.data_dir = Path(get_astrbot_data_path()) / "plugin_data" / PLUGIN_NAME
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.data_file = self.data_dir / DATA_FILE
        self.data: Dict[str, Any] = self._load_data()
        self._data_lock = asyncio.Lock()
        self._cron_task: Optional[asyncio.Task] = None
        try:
            self._cron_task = asyncio.create_task(self._cron_loop())
        except RuntimeError:
            self._cron_task = None

    async def terminate(self):
        if self._cron_task:
            self._cron_task.cancel()
            try:
                await self._cron_task
            except asyncio.CancelledError:
                pass

    def _cfg(self, key: str, default: Any) -> Any:
        try:
            return self.config.get(key, default)
        except Exception:
            return default

    def _load_data(self) -> Dict[str, Any]:
        data = _default_data()
        if self.data_file.exists():
            try:
                loaded = json.loads(self.data_file.read_text("utf-8"))
                if isinstance(loaded, dict):
                    data = loaded
            except Exception as e:
                logger.error(f"RSS 插件数据读取失败，将使用空数据：{e}")
                data = _default_data()
        data = self._migrate_data(data)
        self._save_data_sync(data)
        return data

    def _migrate_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        # v0.1.1 及更早：顶层 rss/rsshub/crons，保留到 legacy，不自动暴露为全局源。
        if "rss" in data or "rsshub" in data or "crons" in data:
            legacy = {
                "rss": data.get("rss", []) if isinstance(data.get("rss", []), list) else [],
                "rsshub": data.get("rsshub", []) if isinstance(data.get("rsshub", []), list) else [],
                "crons": data.get("crons", []) if isinstance(data.get("crons", []), list) else [],
            }
            data = _default_data()
            data["__legacy_global__"] = legacy
        data.setdefault("schema_version", 3)
        data.setdefault("global", {"rss": [], "rsshub": []})
        data["global"].setdefault("rss", [])
        data["global"].setdefault("rsshub", [])
        data.setdefault("sessions", {})
        data.setdefault("__legacy_global__", {"rss": [], "rsshub": [], "crons": []})
        _ensure_ids(data["global"].get("rss", []))
        _ensure_ids(data["global"].get("rsshub", []))
        for sess in data.get("sessions", {}).values():
            if not isinstance(sess, dict):
                continue
            sess.setdefault("rss", [])
            sess.setdefault("rsshub", [])
            sess.setdefault("crons", [])
            _ensure_ids(sess["rss"])
            _ensure_ids(sess["rsshub"])
            _ensure_ids(sess["crons"])
            for cron in sess["crons"]:
                # v0.1.1 可能只有 source_index，尽力转成 source_id。
                if "source_scope" not in cron:
                    cron["source_scope"] = "session"
                if "source_id" not in cron and "source_index" in cron:
                    idx = int(cron.get("source_index", 0)) - 1
                    if 0 <= idx < len(sess["rss"]):
                        cron["source_id"] = sess["rss"][idx].get("id")
                cron.setdefault("last_seen_ids", [])
                cron.setdefault("enabled", True)
        data["schema_version"] = 3
        return data

    def _save_data_sync(self, data: Optional[Dict[str, Any]] = None) -> None:
        target = data if data is not None else self.data
        tmp = self.data_file.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(target, ensure_ascii=False, indent=2), "utf-8")
        tmp.replace(self.data_file)

    async def _save_data(self) -> None:
        async with self._data_lock:
            self._save_data_sync()

    def _session(self, umo: str) -> Dict[str, Any]:
        sessions = self.data.setdefault("sessions", {})
        sess = sessions.setdefault(umo, {"rss": [], "rsshub": [], "crons": []})
        sess.setdefault("rss", [])
        sess.setdefault("rsshub", [])
        sess.setdefault("crons", [])
        return sess

    def _global(self) -> Dict[str, Any]:
        glob = self.data.setdefault("global", {"rss": [], "rsshub": []})
        glob.setdefault("rss", [])
        glob.setdefault("rsshub", [])
        return glob

    def _combined_rss(self, umo: str) -> List[Dict[str, Any]]:
        sess = self._session(umo)
        glob = self._global()
        combined: List[Dict[str, Any]] = []
        for i, item in enumerate(sess.get("rss", []), start=1):
            combined.append({"scope": "session", "source_index": i, "item": item})
        for i, item in enumerate(glob.get("rss", []), start=1):
            combined.append({"scope": "global", "source_index": i, "item": item})
        return combined

    def _combined_rsshub(self, umo: str) -> List[Dict[str, Any]]:
        sess = self._session(umo)
        glob = self._global()
        combined: List[Dict[str, Any]] = []
        for i, item in enumerate(sess.get("rsshub", []), start=1):
            combined.append({"scope": "session", "source_index": i, "item": item})
        for i, item in enumerate(glob.get("rsshub", []), start=1):
            combined.append({"scope": "global", "source_index": i, "item": item})
        return combined

    def _resolve_rss(self, umo: str, index: int) -> Optional[Dict[str, Any]]:
        combined = self._combined_rss(umo)
        if index < 1 or index > len(combined):
            return None
        return combined[index - 1]

    def _resolve_rsshub(self, umo: str, index: int) -> Optional[Dict[str, Any]]:
        combined = self._combined_rsshub(umo)
        if index < 1 or index > len(combined):
            return None
        return combined[index - 1]

    def _source_by_id(self, umo: str, scope: str, source_id: str) -> Optional[Dict[str, Any]]:
        container = self._session(umo) if scope == "session" else self._global()
        for i, item in enumerate(container.get("rss", []), start=1):
            if item.get("id") == source_id:
                return {"scope": scope, "source_index": i, "item": item}
        return None

    def _url_exists(self, umo: Optional[str], url: str, source_type: str, scope: str) -> bool:
        url = _norm_url(url)
        if scope == "global":
            candidates = self._global().get(source_type, [])
        else:
            candidates = self._session(umo or "").get(source_type, []) + self._global().get(source_type, [])
        return any(_norm_url(x.get("url")) == url for x in candidates)

    def _validate_url(self, url: str) -> Tuple[bool, str]:
        url = _norm_url(url)
        if not url:
            return False, "URL 不能为空"
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return False, "URL 必须是 http/https 地址"
        if not bool(self._cfg("allow_private_address", False)):
            host = parsed.hostname or ""
            try:
                ip = ipaddress.ip_address(host)
                if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast:
                    return False, "当前配置不允许添加内网/本机地址"
            except ValueError:
                pass
        return True, ""

    def _make_source(self, url: str, note: str = "", **extra: Any) -> Dict[str, Any]:
        item = {
            "id": _new_id(),
            "url": _norm_url(url),
            "note": (note or "").strip(),
            "created_at": _now_ts(),
        }
        item.update(extra)
        return item

    async def _fetch_feed(self, url: str) -> Any:
        headers = {"User-Agent": str(self._cfg("user_agent", "AstrBotRSS/0.1.2"))}
        timeout = float(self._cfg("request_timeout_seconds", 15))
        max_bytes = int(self._cfg("max_response_bytes", 2 * 1024 * 1024))
        async with httpx.AsyncClient(follow_redirects=True, timeout=timeout, headers=headers) as client:
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()
                chunks: List[bytes] = []
                size = 0
                async for chunk in resp.aiter_bytes():
                    size += len(chunk)
                    if size > max_bytes:
                        raise RuntimeError(f"响应体超过限制：{max_bytes} bytes")
                    chunks.append(chunk)
                raw = b"".join(chunks)
        parsed = feedparser.parse(raw)
        if getattr(parsed, "bozo", False) and not parsed.entries:
            raise RuntimeError(f"RSS 解析失败：{getattr(parsed, 'bozo_exception', 'unknown error')}")
        return parsed

    def _feed_meta_text(self, parsed: Any, source: Dict[str, Any], scope_label: str) -> str:
        feed = parsed.feed or {}
        lines = [
            "RSS 源信息",
            f"来源范围：{scope_label}",
            f"订阅备注：{source.get('note') or '无'}",
            f"订阅地址：{source.get('url')}",
            f"Feed 标题：{_safe_title(feed.get('title'))}",
        ]
        if feed.get("link"):
            lines.append(f"Feed 链接：{feed.get('link')}")
        if feed.get("subtitle") or feed.get("description"):
            lines.append(f"Feed 描述：{_strip_html(feed.get('subtitle') or feed.get('description'))}")
        if feed.get("updated"):
            lines.append(f"Feed 更新时间：{feed.get('updated')}")
        lines.append(f"Feed 条目总数：{len(parsed.entries or [])}")
        if source.get("routing"):
            lines.append(f"RSSHub 路由：{source.get('routing')}")
        return "\n".join(lines)

    def _entry_meta_line(self, entry: Any) -> Dict[str, str]:
        return {
            "title": _safe_title(entry.get("title")),
            "link": str(entry.get("link") or entry.get("id") or ""),
            "author": _strip_html(entry.get("author")),
            "published": str(entry.get("published") or ""),
            "updated": str(entry.get("updated") or ""),
            "tags": _parse_tags(entry),
        }

    async def _ai_summary(self, event: Optional[AstrMessageEvent], umo: str, title: str, content: str) -> Optional[str]:
        if not content:
            return None
        level = str(self._cfg("ai_summary_level", "medium"))
        target_map = {"short": "35", "medium": "80", "long": "150", "少": "35", "中": "80", "多": "150"}
        target = target_map.get(level, "80")
        provider_mode = str(self._cfg("ai_provider_mode", "current"))
        provider_id = ""
        try:
            if provider_mode == "specified":
                provider_id = str(self._cfg("ai_provider_id", "")).strip()
            else:
                provider_id = await self.context.get_current_chat_provider_id(umo=umo)
            if not provider_id:
                return None
            prompt = (
                f"请为下面的 RSS 条目写一段中文速览，约 {target} 字。"
                "只输出速览内容，不要使用 Markdown，不要重复标题和链接。\n\n"
                f"标题：{title}\n"
                f"正文：{content[:3500]}"
            )
            resp = await self.context.llm_generate(chat_provider_id=provider_id, prompt=prompt)
            text = getattr(resp, "completion_text", None) or str(resp or "")
            text = _strip_html(text)
            return text or None
        except Exception as e:
            logger.warning(f"RSS AI 速览生成失败，回退到截断模式：{e}")
            return None

    async def _entry_preview(self, event: Optional[AstrMessageEvent], umo: str, entry: Any) -> str:
        content = _entry_text(entry)
        mode = str(self._cfg("preview_write_mode", "truncate"))
        if mode == "ai":
            summary = await self._ai_summary(event, umo, _safe_title(entry.get("title")), content)
            if summary:
                return summary
        return _clip_text(content, int(self._cfg("preview_truncate_chars", 50)))

    async def _format_get_output(self, event: AstrMessageEvent, source_ref: Dict[str, Any]) -> str:
        source = source_ref["item"]
        scope_label = "全局" if source_ref["scope"] == "global" else "当前会话"
        parsed = await self._fetch_feed(source["url"])
        max_entries = max(1, int(self._cfg("max_get_entries", 5)))
        entries = list(parsed.entries or [])
        show_entries = entries[:max_entries]
        parts = [self._feed_meta_text(parsed, source, scope_label), "", f"本次展示：{len(show_entries)} / {len(entries)} 条", ""]
        if not show_entries:
            parts.append("暂无条目。")
            return "\n".join(parts)
        for i, entry in enumerate(show_entries, start=1):
            meta = self._entry_meta_line(entry)
            preview = await self._entry_preview(event, event.unified_msg_origin, entry)
            block = [
                f"{i}. {meta['title']}",
                f"链接：{meta['link'] or '无'}",
            ]
            if meta["published"]:
                block.append(f"发布时间：{meta['published']}")
            elif meta["updated"]:
                block.append(f"更新时间：{meta['updated']}")
            if meta["author"]:
                block.append(f"作者：{meta['author']}")
            if meta["tags"]:
                block.append(f"标签：{meta['tags']}")
            block.append(f"速览：{preview or '无可用内容'}")
            parts.append("\n".join(block))
            parts.append("")
        if len(entries) > max_entries:
            parts.append(f"还有 {len(entries) - max_entries} 条未展示。可在配置项 max_get_entries 中调高最大展示条数。")
        return "\n".join(parts).strip()

    async def _render_list_or_text(self, event: AstrMessageEvent, items: List[Dict[str, Any]], title: str):
        data = {"title": title, "items": items}
        try:
            url = await self.html_render(LIST_TEMPLATE, data, options={"full_page": True})
            yield event.image_result(url)
        except Exception as e:
            logger.warning(f"RSS 列表图片渲染失败，回退文本：{e}")
            lines = [title]
            for item in items:
                lines.append(f"{item['display_index']}. [{item['scope_label']}] {item['title']} - {item['url']} 备注：{item.get('note') or '无'}")
            if not items:
                lines.append("暂无")
            yield event.plain_result("\n".join(lines))

    def _list_items_for_display(self, refs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for display_index, ref in enumerate(refs, start=1):
            item = ref["item"]
            scope_label = "全局" if ref["scope"] == "global" else "当前会话"
            title = item.get("note") or item.get("url") or "未命名"
            items.append(
                {
                    "display_index": display_index,
                    "scope_label": scope_label,
                    "title": html.escape(str(title)),
                    "url": html.escape(str(item.get("url", ""))),
                    "note": html.escape(str(item.get("note", ""))),
                }
            )
        return items

    async def _add_rss_source(self, umo: str, url: str, note: str, scope: str, **extra: Any) -> str:
        ok, msg = self._validate_url(url)
        if not ok:
            return msg
        if not bool(self._cfg("allow_duplicate_url", False)) and self._url_exists(umo, url, "rss", scope):
            return "该 RSS URL 已存在，未重复添加。可在配置中开启 allow_duplicate_url。"
        container = self._global() if scope == "global" else self._session(umo)
        item = self._make_source(url, note, **extra)
        container["rss"].append(item)
        await self._save_data()
        label = "全局" if scope == "global" else "当前会话"
        return f"已添加{label} RSS 源：{url}\n备注：{note or '无'}"

    async def _add_rsshub_source(self, umo: str, url: str, note: str, scope: str) -> str:
        ok, msg = self._validate_url(url)
        if not ok:
            return msg
        if not bool(self._cfg("allow_duplicate_url", False)) and self._url_exists(umo, url, "rsshub", scope):
            return "该 RSSHub URL 已存在，未重复添加。可在配置中开启 allow_duplicate_url。"
        container = self._global() if scope == "global" else self._session(umo)
        container["rsshub"].append(self._make_source(url.rstrip("/"), note))
        await self._save_data()
        label = "全局" if scope == "global" else "当前会话"
        return f"已添加{label} RSSHub 源：{url}\n备注：{note or '无'}"

    def _build_rsshub_route_url(self, base_url: str, routing: str) -> str:
        base = base_url.rstrip("/") + "/"
        route = (routing or "").strip().lstrip("/")
        return urljoin(base, route)

    # ---------------- RSS commands ----------------
    @filter.command_group("rss")
    def rss(self):
        pass

    @rss.command("help")
    async def rss_help(self, event: AstrMessageEvent):
        """查看 RSS 插件帮助"""
        yield event.plain_result(
            "RSS 插件指令：\n"
            "/rss add <url> [备注] - 添加当前会话 RSS 源\n"
            "/rss rm <index> - 删除当前会话第 index 个 RSS 源\n"
            "/rss add-global <url> [备注] - 管理员添加全局 RSS 源\n"
            "/rss rm-global <index> - 管理员删除第 index 个全局 RSS 源\n"
            "/rss addby <rsshub_index> <routing> [备注] - 基于 RSSHub 添加当前会话 RSS 源\n"
            "/rss addby-global <rsshub_global_index> <routing> [备注] - 管理员基于全局 RSSHub 添加全局 RSS 源\n"
            "/rss rmby <rsshub_index> - 删除当前会话中来自该 RSSHub 的 RSS 源\n"
            "/rss rmby-global <rsshub_global_index> - 管理员删除全局中来自该 RSSHub 的 RSS 源\n"
            "/rss get <index> - 输出 RSS 元信息与条目 title/link/速览\n"
            "/rss list - 列出当前会话 RSS 源，并在最后追加全局源\n"
            "/rss list-global - 列出全局 RSS 源\n"
            "/rss cron <index> <cron_expr> - 为当前会话添加定时推送\n"
            "/rss cronlist - 查看当前会话定时任务\n"
            "/rss cronrm <cron_index> - 删除当前会话定时任务"
        )

    @rss.command("add")
    async def rss_add(self, event: AstrMessageEvent, url: str = "", note: str = ""):
        """添加当前会话 RSS 源"""
        if not url:
            yield event.plain_result("用法：/rss add <url> [备注]")
            return
        msg = await self._add_rss_source(event.unified_msg_origin, url, note, "session")
        yield event.plain_result(msg)

    @rss.command("rm")
    async def rss_rm(self, event: AstrMessageEvent, index: int = 0):
        """删除当前会话 RSS 源"""
        sess = self._session(event.unified_msg_origin)
        if index < 1 or index > len(sess.get("rss", [])):
            total = len(self._combined_rss(event.unified_msg_origin))
            yield event.plain_result(
                f"当前会话 RSS 序号无效。本地源数量：{len(sess.get('rss', []))}，列表总数含全局源：{total}。如需删除全局源，请管理员使用 /rss rm-global <index>。"
            )
            return
        item = sess["rss"].pop(index - 1)
        await self._save_data()
        yield event.plain_result(f"已删除当前会话 RSS 源：{item.get('url')}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @rss.command("add-global", alias={"add_global"})
    async def rss_add_global(self, event: AstrMessageEvent, url: str = "", note: str = ""):
        """管理员添加全局 RSS 源"""
        if not url:
            yield event.plain_result("用法：/rss add-global <url> [备注]")
            return
        msg = await self._add_rss_source(event.unified_msg_origin, url, note, "global")
        yield event.plain_result(msg)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @rss.command("rm-global", alias={"rm_global"})
    async def rss_rm_global(self, event: AstrMessageEvent, index: int = 0):
        """管理员删除全局 RSS 源"""
        glob = self._global()
        if index < 1 or index > len(glob.get("rss", [])):
            yield event.plain_result(f"全局 RSS 序号无效。当前全局 RSS 源数量：{len(glob.get('rss', []))}")
            return
        item = glob["rss"].pop(index - 1)
        # 删除引用该全局源的 cron，避免无效轮询。
        for sess in self.data.get("sessions", {}).values():
            sess["crons"] = [c for c in sess.get("crons", []) if not (c.get("source_scope") == "global" and c.get("source_id") == item.get("id"))]
        await self._save_data()
        yield event.plain_result(f"已删除全局 RSS 源：{item.get('url')}")

    @rss.command("addby")
    async def rss_addby(self, event: AstrMessageEvent, rsshub_index: int = 0, routing: str = "", note: str = ""):
        """根据 RSSHub 源和路由添加当前会话 RSS 源"""
        if rsshub_index < 1 or not routing:
            yield event.plain_result("用法：/rss addby <rsshub_index> <routing> [备注]")
            return
        hub_ref = self._resolve_rsshub(event.unified_msg_origin, rsshub_index)
        if not hub_ref:
            yield event.plain_result("RSSHub 序号无效。请用 /rsshub list 查看。")
            return
        url = self._build_rsshub_route_url(hub_ref["item"].get("url", ""), routing)
        msg = await self._add_rss_source(
            event.unified_msg_origin,
            url,
            note,
            "session",
            rsshub_id=hub_ref["item"].get("id"),
            rsshub_scope=hub_ref["scope"],
            routing=routing,
        )
        yield event.plain_result(msg)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @rss.command("addby-global", alias={"addby_global"})
    async def rss_addby_global(self, event: AstrMessageEvent, rsshub_global_index: int = 0, routing: str = "", note: str = ""):
        """管理员根据全局 RSSHub 源和路由添加全局 RSS 源"""
        glob = self._global()
        if rsshub_global_index < 1 or rsshub_global_index > len(glob.get("rsshub", [])) or not routing:
            yield event.plain_result("用法：/rss addby-global <rsshub_global_index> <routing> [备注]")
            return
        hub = glob["rsshub"][rsshub_global_index - 1]
        url = self._build_rsshub_route_url(hub.get("url", ""), routing)
        msg = await self._add_rss_source(
            event.unified_msg_origin,
            url,
            note,
            "global",
            rsshub_id=hub.get("id"),
            rsshub_scope="global",
            routing=routing,
        )
        yield event.plain_result(msg)

    @rss.command("rmby")
    async def rss_rmby(self, event: AstrMessageEvent, rsshub_index: int = 0):
        """删除当前会话中来自指定 RSSHub 的 RSS 源"""
        hub_ref = self._resolve_rsshub(event.unified_msg_origin, rsshub_index)
        if not hub_ref:
            yield event.plain_result("RSSHub 序号无效。请用 /rsshub list 查看。")
            return
        sess = self._session(event.unified_msg_origin)
        before = len(sess.get("rss", []))
        hub_id = hub_ref["item"].get("id")
        hub_scope = hub_ref["scope"]
        sess["rss"] = [x for x in sess.get("rss", []) if not (x.get("rsshub_id") == hub_id and x.get("rsshub_scope") == hub_scope)]
        removed = before - len(sess["rss"])
        await self._save_data()
        yield event.plain_result(f"已删除当前会话中来自该 RSSHub 的 RSS 源 {removed} 个。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @rss.command("rmby-global", alias={"rmby_global"})
    async def rss_rmby_global(self, event: AstrMessageEvent, rsshub_global_index: int = 0):
        """管理员删除全局中来自指定全局 RSSHub 的 RSS 源"""
        glob = self._global()
        if rsshub_global_index < 1 or rsshub_global_index > len(glob.get("rsshub", [])):
            yield event.plain_result("全局 RSSHub 序号无效。请用 /rsshub list-global 查看。")
            return
        hub = glob["rsshub"][rsshub_global_index - 1]
        before = len(glob.get("rss", []))
        glob["rss"] = [x for x in glob.get("rss", []) if not (x.get("rsshub_id") == hub.get("id") and x.get("rsshub_scope") == "global")]
        removed = before - len(glob["rss"])
        await self._save_data()
        yield event.plain_result(f"已删除全局中来自该 RSSHub 的 RSS 源 {removed} 个。")

    @rss.command("get")
    async def rss_get(self, event: AstrMessageEvent, index: int = 0):
        """获取 RSS 源元信息与条目速览"""
        ref = self._resolve_rss(event.unified_msg_origin, index)
        if not ref:
            yield event.plain_result("RSS 序号无效。请用 /rss list 查看；全局源会追加在当前会话源之后。")
            return
        try:
            text = await self._format_get_output(event, ref)
            yield event.plain_result(text)
        except Exception as e:
            logger.error(f"获取 RSS 源失败：{e}")
            yield event.plain_result(f"获取 RSS 源失败：{e}")

    @rss.command("list")
    async def rss_list(self, event: AstrMessageEvent):
        """列出当前会话 RSS 源，并追加全局源"""
        refs = self._combined_rss(event.unified_msg_origin)
        items = self._list_items_for_display(refs)
        async for result in self._render_list_or_text(event, items, "RSS 源列表"):
            yield result

    @rss.command("list-global", alias={"list_global"})
    async def rss_list_global(self, event: AstrMessageEvent):
        """列出全局 RSS 源"""
        refs = [{"scope": "global", "source_index": i, "item": item} for i, item in enumerate(self._global().get("rss", []), start=1)]
        items = self._list_items_for_display(refs)
        async for result in self._render_list_or_text(event, items, "全局 RSS 源列表"):
            yield result

    @rss.command("check")
    async def rss_check(self, event: AstrMessageEvent, index: str = "all", limit: int = 3):
        """手动检查 RSS 源"""
        refs = self._combined_rss(event.unified_msg_origin)
        if not refs:
            yield event.plain_result("暂无 RSS 源。")
            return
        targets = refs
        if index != "all":
            try:
                i = int(index)
                ref = self._resolve_rss(event.unified_msg_origin, i)
                if not ref:
                    yield event.plain_result("RSS 序号无效。")
                    return
                targets = [ref]
            except ValueError:
                yield event.plain_result("用法：/rss check <index|all> [limit]")
                return
        lines = ["RSS 检查结果："]
        for ref in targets:
            item = ref["item"]
            try:
                parsed = await self._fetch_feed(item["url"])
                title = _safe_title((parsed.feed or {}).get("title"))
                lines.append(f"- [{'全局' if ref['scope']=='global' else '当前会话'}] {title}：{len(parsed.entries or [])} 条")
                for entry in list(parsed.entries or [])[: max(1, int(limit))]:
                    lines.append(f"  • {_safe_title(entry.get('title'))} {entry.get('link') or ''}")
            except Exception as e:
                lines.append(f"- {item.get('url')}：失败，{e}")
        yield event.plain_result("\n".join(lines))

    @rss.command("cron")
    async def rss_cron(self, event: AstrMessageEvent, index: int = 0, minute: str = "", hour: str = "", day: str = "", month: str = "", weekday: str = ""):
        """添加当前会话 RSS 定时推送任务"""
        if index < 1 or not all([minute, hour, day, month, weekday]):
            yield event.plain_result("用法：/rss cron <index> <cron_expr>\n示例：/rss cron 1 0/5 * * * *")
            return
        ref = self._resolve_rss(event.unified_msg_origin, index)
        if not ref:
            yield event.plain_result("RSS 序号无效。请用 /rss list 查看。")
            return
        expr_raw = " ".join([minute, hour, day, month, weekday])
        try:
            expr = _validate_cron(expr_raw)
        except Exception as e:
            yield event.plain_result(f"Cron 表达式无效：{e}")
            return
        # 初次添加时记录当前条目，避免历史消息刷屏。
        last_seen_ids: List[str] = []
        try:
            parsed = await self._fetch_feed(ref["item"].get("url"))
            last_seen_ids = [_entry_id(e) for e in list(parsed.entries or [])[:50]]
        except Exception as e:
            logger.warning(f"添加 cron 时初始化 last_seen 失败：{e}")
        sess = self._session(event.unified_msg_origin)
        sess["crons"].append(
            {
                "id": _new_id(),
                "source_scope": ref["scope"],
                "source_id": ref["item"].get("id"),
                "expr": expr,
                "enabled": True,
                "last_seen_ids": last_seen_ids,
                "last_trigger_minute": "",
                "created_at": _now_ts(),
            }
        )
        await self._save_data()
        yield event.plain_result(f"已为当前会话添加定时任务：RSS #{index}，Cron：{expr}\n首次轮询只记录基准，之后发现新内容才推送。")

    @rss.command("cronlist")
    async def rss_cronlist(self, event: AstrMessageEvent):
        """查看当前会话定时任务"""
        sess = self._session(event.unified_msg_origin)
        crons = sess.get("crons", [])
        if not crons:
            yield event.plain_result("当前会话暂无 RSS 定时任务。")
            return
        lines = ["当前会话 RSS 定时任务："]
        for i, cron in enumerate(crons, start=1):
            ref = self._source_by_id(event.unified_msg_origin, cron.get("source_scope", "session"), cron.get("source_id", ""))
            if ref:
                source = ref["item"]
                label = "全局" if ref["scope"] == "global" else "当前会话"
                desc = f"[{label}] {source.get('note') or source.get('url')}"
            else:
                desc = "源已不存在"
            lines.append(f"{i}. {desc}\n   Cron：{cron.get('expr')}，状态：{'启用' if cron.get('enabled', True) else '停用'}")
        yield event.plain_result("\n".join(lines))

    @rss.command("cronrm")
    async def rss_cronrm(self, event: AstrMessageEvent, cron_index: int = 0):
        """删除当前会话定时任务"""
        sess = self._session(event.unified_msg_origin)
        crons = sess.get("crons", [])
        if cron_index < 1 or cron_index > len(crons):
            yield event.plain_result(f"定时任务序号无效。当前任务数：{len(crons)}")
            return
        cron = crons.pop(cron_index - 1)
        await self._save_data()
        yield event.plain_result(f"已删除定时任务：{cron.get('expr')}")

    # ---------------- RSSHub commands ----------------
    @filter.command_group("rsshub")
    def rsshub(self):
        pass

    @rsshub.command("help")
    async def rsshub_help(self, event: AstrMessageEvent):
        """查看 RSSHub 帮助"""
        yield event.plain_result(
            "RSSHub 指令：\n"
            "/rsshub add <url> [备注] - 添加当前会话 RSSHub 源\n"
            "/rsshub rm <index> - 删除当前会话第 index 个 RSSHub 源\n"
            "/rsshub add-global <url> [备注] - 管理员添加全局 RSSHub 源\n"
            "/rsshub rm-global <index> - 管理员删除全局 RSSHub 源\n"
            "/rsshub list - 列出当前会话 RSSHub 源，并追加全局源\n"
            "/rsshub list-global - 列出全局 RSSHub 源"
        )

    @rsshub.command("add")
    async def rsshub_add(self, event: AstrMessageEvent, url: str = "", note: str = ""):
        """添加当前会话 RSSHub 源"""
        if not url:
            yield event.plain_result("用法：/rsshub add <url> [备注]")
            return
        msg = await self._add_rsshub_source(event.unified_msg_origin, url, note, "session")
        yield event.plain_result(msg)

    @rsshub.command("rm")
    async def rsshub_rm(self, event: AstrMessageEvent, index: int = 0):
        """删除当前会话 RSSHub 源"""
        sess = self._session(event.unified_msg_origin)
        if index < 1 or index > len(sess.get("rsshub", [])):
            yield event.plain_result(
                f"当前会话 RSSHub 序号无效。本地 RSSHub 数量：{len(sess.get('rsshub', []))}。如需删除全局 RSSHub，请管理员使用 /rsshub rm-global <index>。"
            )
            return
        hub = sess["rsshub"].pop(index - 1)
        # 不自动删除基于它添加的 RSS；如需批量删除用 /rss rmby。
        await self._save_data()
        yield event.plain_result(f"已删除当前会话 RSSHub 源：{hub.get('url')}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @rsshub.command("add-global", alias={"add_global"})
    async def rsshub_add_global(self, event: AstrMessageEvent, url: str = "", note: str = ""):
        """管理员添加全局 RSSHub 源"""
        if not url:
            yield event.plain_result("用法：/rsshub add-global <url> [备注]")
            return
        msg = await self._add_rsshub_source(event.unified_msg_origin, url, note, "global")
        yield event.plain_result(msg)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @rsshub.command("rm-global", alias={"rm_global"})
    async def rsshub_rm_global(self, event: AstrMessageEvent, index: int = 0):
        """管理员删除全局 RSSHub 源"""
        glob = self._global()
        if index < 1 or index > len(glob.get("rsshub", [])):
            yield event.plain_result(f"全局 RSSHub 序号无效。当前全局 RSSHub 源数量：{len(glob.get('rsshub', []))}")
            return
        hub = glob["rsshub"].pop(index - 1)
        await self._save_data()
        yield event.plain_result(f"已删除全局 RSSHub 源：{hub.get('url')}")

    @rsshub.command("list")
    async def rsshub_list(self, event: AstrMessageEvent):
        """列出当前会话 RSSHub 源，并追加全局源"""
        refs = self._combined_rsshub(event.unified_msg_origin)
        if not refs:
            yield event.plain_result("暂无 RSSHub 源。")
            return
        lines = ["RSSHub 源列表（当前会话优先，全局源追加到最后）："]
        for i, ref in enumerate(refs, start=1):
            item = ref["item"]
            label = "全局" if ref["scope"] == "global" else "当前会话"
            lines.append(f"{i}. [{label}] {item.get('url')} 备注：{item.get('note') or '无'}")
        yield event.plain_result("\n".join(lines))

    @rsshub.command("list-global", alias={"list_global"})
    async def rsshub_list_global(self, event: AstrMessageEvent):
        """列出全局 RSSHub 源"""
        hubs = self._global().get("rsshub", [])
        if not hubs:
            yield event.plain_result("暂无全局 RSSHub 源。")
            return
        lines = ["全局 RSSHub 源列表："]
        for i, item in enumerate(hubs, start=1):
            lines.append(f"{i}. {item.get('url')} 备注：{item.get('note') or '无'}")
        yield event.plain_result("\n".join(lines))

    # ---------------- Cron loop ----------------
    async def _cron_loop(self):
        await asyncio.sleep(3)
        while True:
            try:
                await self._check_crons_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"RSS 定时轮询异常：{e}")
            await asyncio.sleep(max(5, int(self._cfg("cron_check_interval_seconds", 20))))

    async def _check_crons_once(self):
        now = datetime.now()
        minute_key = now.strftime("%Y%m%d%H%M")
        sessions = list(self.data.get("sessions", {}).items())
        for umo, sess in sessions:
            for cron in list(sess.get("crons", [])):
                if not cron.get("enabled", True):
                    continue
                expr = cron.get("expr", "")
                try:
                    if not _cron_matches(expr, now):
                        continue
                except Exception as e:
                    logger.warning(f"跳过无效 Cron：{expr}，{e}")
                    continue
                if cron.get("last_trigger_minute") == minute_key:
                    continue
                cron["last_trigger_minute"] = minute_key
                await self._run_cron_task(umo, cron)
        await self._save_data()

    async def _run_cron_task(self, umo: str, cron: Dict[str, Any]):
        ref = self._source_by_id(umo, cron.get("source_scope", "session"), cron.get("source_id", ""))
        if not ref:
            return
        source = ref["item"]
        try:
            parsed = await self._fetch_feed(source.get("url"))
        except Exception as e:
            logger.warning(f"RSS 定时获取失败：{source.get('url')}，{e}")
            return
        entries = list(parsed.entries or [])
        current_ids = [_entry_id(e) for e in entries[:50]]
        old_ids = set(cron.get("last_seen_ids") or [])
        if not old_ids:
            cron["last_seen_ids"] = current_ids
            return
        new_entries = []
        for entry in entries:
            eid = _entry_id(entry)
            if eid in old_ids:
                break
            new_entries.append(entry)
        cron["last_seen_ids"] = current_ids
        if not new_entries:
            return
        max_push = max(1, int(self._cfg("max_push_entries", 5)))
        selected = list(reversed(new_entries[:max_push]))
        feed_title = _safe_title((parsed.feed or {}).get("title"), "RSS 更新")
        scope_label = "全局" if ref["scope"] == "global" else "当前会话"
        head = f"RSS 更新提醒：{feed_title}\n来源：{scope_label} / {source.get('note') or source.get('url')}\n新内容：{len(new_entries)} 条，本次推送 {len(selected)} 条"
        await self.context.send_message(umo, MessageChain().message(head))
        for entry in selected:
            meta = self._entry_meta_line(entry)
            preview = await self._entry_preview(None, umo, entry)
            text = f"{meta['title']}\n链接：{meta['link'] or '无'}\n速览：{preview or '无可用内容'}"
            await self.context.send_message(umo, MessageChain().message(text))
