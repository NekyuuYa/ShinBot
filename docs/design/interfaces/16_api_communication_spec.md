# ShinBot 技术规范：前后端通信契约 (API Communication)

本文档定义了 ShinBot 核心引擎 (Python) 与管理后台 (Vue 3) 之间的所有通信标准。

---

## 1. 通信基础
- **基础 URL**: `/api/v1`
- **内容格式**: `application/json`
- **鉴权机制**: JWT (JSON Web Token)
    - **Header**: `Authorization: Bearer <token>`
    - **过期处理**: 401 状态码触发前端跳转登录页。

---

## 2. 统一响应格式 (Envelope)

所有接口返回必须遵循以下结构：

```json
{
    "success": true,      // 业务逻辑是否成功
    "data": { ... },      // 成功时的返回数据
    "error": {            // 失败时的错误信息
        "code": "ERROR_CODE",
        "message": "人类可读的错误描述"
    },
    "timestamp": 1678901234
}
```

---

## 3. 核心 API 端点规划

### 3.1 鉴权管理 (Auth)
- `POST /auth/login`: 用户登录，换取 Token。

### 3.2 实例管理 (Instances)
- `GET /instances`: 获取所有机器人列表。
- `POST /instances`: 创建新机器人。
- `PATCH /instances/{id}`: 修改配置。
- `POST /instances/{id}/control`: 指令控制 (start/stop)。

### 3.3 插件管理 (Plugins)
- `GET /plugins`: 获取已安装插件。
- `POST /plugins/reload`: 触发热重载。
- `PATCH /plugins/{id}/config`: 更新插件内部设置。

### 3.4 权限与指令
- `GET /permissions/groups`: 权限组。
- `GET /commands/registry`: 已注册指令快照。

---

## 4. 实时数据流 (WebSocket)

### 4.1 日志流 (`/ws/logs`)
- 实时推送系统各组件产生的日志。

### 4.2 系统状态 (`/ws/system`)
- 推送实例在线数、CPU/内存占用、模型调用频率等指标。

---

## 5. 开发建议
- **后端**: 必须在 FastAPI 中为每个端点提供详细的 `response_model`，确保前端拿到的是符合预期的强类型数据。
- **前端**: 使用 Axios 拦截器统一处理 401 鉴权失效和 500 系统崩溃。
