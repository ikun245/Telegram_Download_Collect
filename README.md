# Telegram 下载器

一个专门用于从 Telegram 群组/频道/聊天中采集消息、图片和视频的工具。

## 功能特性
- **账号登录**: 使用您的个人 Telegram 账号登录。
- **交互式选择**: 命令行交互选择要抓取的对话。
- **多种采集模式**: 支持从最早、指定日期、指定消息ID或最新消息开始采集。
- **媒体下载**: 自动下载图片和视频（支持并发下载、实时进度条、小文件优先）。
- **HTML 导出**: 将消息历史导出为美观的 HTML 文件。
- **高性能**: 使用 Python 异步 (Asyncio) 实现高并发下载。
- **代理支持**: 支持 HTTP 和 SOCKS5 代理，方便网络连接。

## 目录结构
```text
ddtg/
├── config.yaml          # 配置文件 (API Key, 代理, 下载设置)
├── README.md            # 说明文档
├── output/              # 输出目录
│   ├── anon.session     # 登录会话文件
│   └── downloads/       # 下载的媒体文件
└── processor/           # 核心代码目录
    ├── main.py          # 主程序入口
    └── requirements.txt # Python 依赖库
```

## 安装与配置

### 1. 前置要求
- Python 3.10+
- Telegram API ID 和 Hash (获取地址:  https://my.telegram.org)

### 2. 安装依赖
```bash
cd processor
pip install -r requirements.txt
```

### 3. 配置文件 (config.yaml)
在运行之前，请务必修改根目录下的 `config.yaml` 文件：

```yaml
app_id: 123456              # 你的 API ID
app_hash: "your_api_hash"   # 你的 API Hash
phone_number: "+8613800000000" # 你的手机号

download_settings:
  max_concurrent_downloads: 5  # 最大并发下载数
  download_path: "./output/downloads" # 下载路径
  rate_limit_ms: 200           # 请求间隔 (毫秒)

proxy:
  enable: false                # 是否启用代理 (true/false)
  type: "socks5"               # 代理类型: "socks5" 或 "http"
  address: "127.0.0.1:7890"    # 代理地址:端口
  user: ""                     # 代理用户名
  password: ""                 # 代理密码

fetch_settings:
  start_type: "earliest"       # 采集起始点: "earliest" (最早), "date" (日期), "message_id" (消息ID)
  start_date: "2025-01-01T00:00:00Z" # 如果 start_type 为 "date"
  start_message_id: 0          # 如果 start_type 为 "message_id"
  target_chat: "me"            # 目标对话: "me", 用户名 (@username), 或链接

export_settings:
  format: "html"               # 导出格式
  output_file: "./output/messages.html" # 导出文件路径
```

## 使用说明

1.  **初始化配置**: 确保 `config.yaml` 已正确填写。
2.  **运行程序**:
    ```bash
    cd processor
    python main.py
    ```
3.  **登录验证**: 首次运行需要输入手机号和 Telegram 发送的验证码。
4.  **选择对话**: 程序会列出你的对话列表，输入序号选择要抓取的对象。
5.  **开始采集**: 程序将自动下载媒体文件并生成 HTML 报告。

## 常见问题

- **下载速度慢**: 尝试在 `config.yaml` 中开启代理或调整并发数。
- **登录失败**: 检查 API ID/Hash 是否正确，或尝试删除 `output/anon.session` 后重试。
