# ShinBot 技术栈与语言分工 (Architecture Stacks)

本文档明确了 ShinBot 框架开发的技术栈及其职能边界。

---

## 1. 目录布局参考 (Directory Layout)

```text
ShinBot/
├── shinbot/              # 程序源代码 (Core Layer)
│   ├── core/             # 逻辑编排引擎 (Pipeline, Auth, Session)
│   ├── models/           # 统一数据模型 (MessageElement AST)
│   ├── builtin_plugins/  # 官方预装插件 (含 Satori 适配器等)
│   │   └── satori_adapter/
│   ├── utils/            # 系统工具类
│   └── api/              # 控制面 API (FastAPI)
├── data/                 # 用户资产层 (Persistence Layer)
│   ├── plugins/          # 外部业务插件代码
│   ├── plugin_data/      # 插件专属的持久化数据库与文件
│   ├── sessions/         # 核心会话存储
│   └── audit/            # 审计日志
├── dashboard/            # Vue 3 管理后台工程
├── main.py               # 引导入口
└── config.toml           # 用户全局配置文件
```

---

## 2. 技术栈分工
... (保持 Vue 3 与 Python 的分工描述)
