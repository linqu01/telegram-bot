# 🤖 Telegram Bot (v1.0)

[![Deploy to Cloudflare Workers](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/linqu01/telegram-bot)
![GitHub stars](https://img.shields.io/github/stars/linqu01/telegram-bot?style=social)
![License](https://img.shields.io/badge/License-MIT-blue.svg)
[![Telegram](https://img.shields.io/badge/Telegram-DM-blue?style=social&logo=telegram)](https://t.me/vaghr_wegram_bot)

**Telegram Bot** 是一个基于 **Cloudflare Workers** 的高性能 Telegram 双向私聊机器人。它专为解决 Telegram 上的垃圾广告与骚扰而生，采用纯本地人机验证、独立话题隔离以及高效消息双向转发机制。

无需服务器，利用 Cloudflare 全球边缘网络，即可免费部署一套企业级、私密且稳定的客户服务系统。

---

## 📑 目录

* [✨ 核心特性](#-核心特性)
* [🛠️ 管理员指令](#-管理员指令)
* [🚀 部署教程](#-部署教程)
    * [GitHub 一键连接部署](#github-一键连接部署)
* [❓ 常见问题 (FAQ)](#-常见问题-faq)
* [📈 Star History](#-star-history)

---

## ✨ 核心特性

v1.0 版本专注于**极致性能**与**长期稳定性**。

| 特性 | 描述 |
| :--- | :--- |
| **⚡ 0 延迟验证** | 采用**本地精选常识题库**，秒开秒验，验证成功率 100%，彻底告别网络接口问题。 |
| **🛡️ 智能防骚扰** | 30 天免打扰期 + 短 ID 验证机制，杜绝按钮失效与并发绕过漏洞。 |
| **💬 独立话题管理** | 自动为每位用户创建独立 Forum Topic，消息完全隔离，管理清晰高效。 |
| **👮 隐形指令系统** | 自动拦截私聊中的 `/` 指令，仅在管理员群组话题内生效，杜绝骚扰。 |
| **🔒 强大管理指令** | 支持 `/close`、`/open`、`/ban`、`/unban`、`/trust`、`/reset`、`/info`、`/cleanup` 等完整指令集。 |
| **☁️ Serverless 架构** | 纯 Cloudflare Workers 运行，0 成本、无服务器、无需运维、抗高并发。 |
| **📸 多媒体完美支持** | 文本、图片、视频、文档、动图等全格式双向转发，不丢失任何信息。 |

---

## 🛠️ 管理员指令

> **注意**：所有指令仅在**管理员群组的话题内**生效。私聊用户发送的 `/` 开头指令会被机器人自动静默拦截，不会打扰管理员。

| 指令       | 作用                  | 适用场景                  |
|------------|-----------------------|---------------------------|
| `/close`   | 强制关闭对话          | 工单处理完成              |
| `/open`    | 重新开启对话          | 误关闭后恢复              |
| `/ban`     | 永久封禁用户          | 恶意刷屏、广告机器人      |
| `/unban`   | 解除封禁              | 给予改过机会              |
| `/trust`   | 永久信任（免验证）    | VIP、熟人、长期客户       |
| `/reset`   | 重置验证状态          | 测试或账号异常            |
| `/info`    | 查看用户信息          | 查询 UID、话题 ID         |
| `/cleanup` | 批量清理失效话题      | 清理被删除的话题记录      |

---

## 🚀 部署教程

### 前置准备
1. **创建 Telegram Bot**：通过 [@BotFather](https://t.me/BotFather) 获取 `BOT_TOKEN`，并在 BotFather 设置中**关闭 Group Privacy**。
2. **准备管理员群组**：创建一个开启 **Topics** 功能的群组，将机器人拉入并设为管理员（授予 “Manage Topics” 权限）。获取群组 ID（必须以 `-100` 开头）。

---

### 部署流程

1. Fork 本仓库到你的 GitHub 账号。
2. 登录 [Cloudflare Dashboard](https://dash.cloudflare.com/) → **Workers & Pages** → **Create Application** → **Connect to Git**。
3. 选择你 Fork 的仓库，配置项目名称和分支，点击 **Save and Deploy**。
4. **绑定 KV 与环境变量**（关键步骤）：
   - 创建 KV Namespace（建议命名为 `TOPIC_MAP`）。
   - 在 Worker **Settings → Variables** 中绑定 KV（变量名必须为 `TOPIC_MAP`）。
   - 添加环境变量：`BOT_TOKEN` 和 `SUPERGROUP_ID`。
5. 部署完成后点击 **Retry deployment** 使配置生效。

---

### 最后一步：激活 Webhook（至关重要）

在浏览器中访问以下链接（替换对应内容）：
https://api.telegram.org/bot<YOUR_TOKEN>/setWebhook?url=<YOUR_WORKER_URL>

示例：`https://api.telegram.org/bot123456:ABCDEF/setWebhook?url=https://你的worker.workers.dev`

成功返回 `{"ok":true}` 即部署完成！

---

## ❓ 常见问题 (FAQ)

**Q1: 点击验证按钮无反应？**  
A: 请确认 Webhook 已正确设置，并执行一次 `deleteWebhook?drop_pending_updates=true` 后重新 setWebhook。

**Q2: 无法创建话题？**  
A: 检查群组 ID 是否以 `-100` 开头、Topics 已开启、机器人拥有 “Manage Topics” 权限。

**Q3: 验证通过后消息不转发？**  
A: 确认所有变量名称正确，尝试删除并重新设置 Webhook。

**Q4: Webhook 设置失败？**  
A: 优先使用默认的 `*.workers.dev` 域名测试，排除自定义域名解析问题。

---

## 🔒 安全说明

> [!IMPORTANT]  
> 请妥善保管 `BOT_TOKEN`，切勿泄露。该 Token 拥有机器人全部操作权限。

---

## 📈 Star History

[![Star History Chart](https://api.star-history.com/svg?repos=linqu01/telegram-bot&type=date&legend=top-left)](https://www.star-history.com/#linqu01/telegram-bot&type=date&legend=top-left)

---

**如果这个项目对你有帮助，请Star ⭐️ 支持**

**Copyright © 2025 linqu01（林渠）**  

