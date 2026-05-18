# astrbot_plugin_rss

AstrBot RSS 订阅插件。

## 功能

基础功能：

- `/rsshub add <url> [备注]`：添加 RSSHub 源，可添加备注
- `/rsshub rm <index>`：删除第 `<index>` 个 RSSHub 源
- `/rsshub list`：列出 RSSHub 源，否则用户很难知道 `/rss addby` 和 `/rss rmby` 该使用哪个 index

- `/rss add <url> [备注]`：添加 RSS 源，可添加备注
- `/rss rm <index>`：删除第 `<index>` 个 RSS 源
- `/rss addby <index> <routing> [备注]`：根据 RSSHub 中第 `<index>` 个源和路由添加 RSS 源
- `/rss rmby <index>`：删除第 `<index>` 个 RSSHub 源下创建的所有 RSS 源
- `/rss get <index>`：获取第 `<index>` 个 RSS 源信息和最近条目
- `/rss list`：列出所有 RSS 源，包括备注，并渲染成图片发送
- `/rss check <index|all> [limit]`：手动检查 RSS 源可用性并显示最新条目，便于确认源是否有效

- `/rss cron <index> <cron_expr>`：添加定时任务
- `/rss cronlist`：列出所有定时任务
- `/rss cronrm <cron_index>`：删除第 `<cron_index>` 个定时任务\

- `/rss help`：查看 RSS 指令帮助
- `/rsshub help`：查看 RSSHub 指令帮助

### cron 定时轮询并主动推送新内容

添加定时任务：

```text
/rss cron <index> <cron_expr>
```

cron 格式：

```text
分钟 小时 日 月 星期
```

示例：

```text
/rss cron 1 0 0 * * *     # 表示每天 0 点触发
/rss cron 1 0/5 * * * *   # 表示每 5 分钟触发
/rss cron 1 0 9-18 * * *  # 表示每天 9 点到 18 点触发
/rss cron 1 0 0 1,15 * *  # 表示每月 1 号和 15 号 0 点触发
```

`*` 表示任意值，星期范围是 `0-6`，`0` 表示星期天。

## 安装

将本插件目录放入：

```text
AstrBot/data/plugins/astrbot_plugin_rss
```

然后在 AstrBot WebUI 插件管理中重载插件。

依赖由 `requirements.txt` 声明：

```text
feedparser
httpx
```

## RSSHub 用法示例

```text
/rsshub add https://rsshub.app "官方 RSSHub"
/rss addby 1 /github/issue/DIYgod/RSSHub "RSSHub Issues"
/rss list
/rss get 1
```

## 配置项

- `validate_on_add`：添加时是否校验 RSS 源
- `allow_duplicate`：是否允许同一会话重复添加 URL
- `allow_local_urls`：是否允许 localhost/内网 IP
- `request_timeout`：请求超时时间
- `max_feed_bytes`：最大读取字节数
- `max_get_entries`：`/rss get` 最大展示条数，默认 5
- `max_push_entries`：定时推送每次最多推送新条目数，默认 5
- `user_agent`：请求 RSS 源的 User-Agent
