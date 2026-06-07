# Dashboard Design Review Workflow

本文档定义 ShinBot Dashboard 的设计审计流程。目标是先稳定发现问题，再分批修复，避免只靠一次性人工观感判断。

## 审计入口

在 `dashboard/` 目录运行静态审计：

```bash
pnpm design:audit
```

输出 Markdown 报告到文件：

```bash
pnpm design:audit -- --output ../tmp/dashboard-design-audit.md
```

输出 JSON 供自动化消费：

```bash
pnpm design:audit -- --format json --output ../tmp/dashboard-design-audit.json
```

严格检查 high finding 不回归：

```bash
pnpm design:audit:strict
```

`design:audit:strict` 等价于 `pnpm design:audit -- --max-high 0`。当前只把 high 级别接入门禁；medium/low 仍作为治理 backlog，避免历史样式债务阻塞功能改动。

## 审计维度

### 1. 视觉与布局

- 页面是否按任务流组织，而不是堆叠 summary、filter、card、list。
- 标题、统计、筛选、主内容、危险操作是否有明确主次。
- 卡片是否只用于实体、重复项、modal 或真正需要框定的工具。
- 管理后台页面应保持紧凑、可扫描，避免营销式大卡片和过度装饰。
- 移动端是否存在长 ID、chip、按钮组、表格列挤压或溢出。

### 2. 样式与 token

- 除 `dashboard/src/theme/themes.ts` 和 `dashboard/src/styles/_variables.scss` 外，不新增 raw hex 色值。
- 组件内的 `box-shadow`、`filter: drop-shadow()`、裸 `border-radius` 优先迁到 `$shadow-*`、`$radius-*` 或 mixin。
- 重复使用的 `rgba(var(--v-theme-*), opacity)` 应沉淀为语义 token，例如 text、border、surface。
- 裸 `linear-gradient` / `radial-gradient` 默认需要说明用途，常用 surface 应进 mixin。
- Vue style 优先使用 `<style scoped lang="scss">`，方便复用 ShinBot style token。

### 3. 组件复用

- View 文件主要负责编排；超过 300 行需要 review，超过 600 行通常要拆。
- 同一 UI 流程在两个以上页面重复，且差异主要是文案、字段或 slot 内容，应抽组件或 composable。
- 常见抽象候选：`SummaryMetricBand`、`FilterToolbar`、`SearchLayoutToolbar`、`EmptyState`、`ResourceCardShell`、`ConfigResourcePageFrame`。
- 抽象稳定结构，不抽业务判断；normalize、validation、API 调用留在领域 composable。
- 通用组件 API 命名保持一致，例如 `items`、`loading`、`emptyConfig`、`getItemKey`、`v-model:visible`。

## 多子代理流程

主代理负责控制节奏和合并结论。子代理只做边界清晰的并行审计，避免重复工作。

### Step 1: 主代理准备基线

1. 运行 `pnpm design:audit`。
2. 查看 `dashboard/src/theme`、`dashboard/src/styles`、高行数 view/component。
3. 形成本轮审计范围，例如 `Commands/Tools`、`Instances/MessagePlatforms`、`Permissions`。

### Step 2: 并行派发子代理

视觉代理：

```text
只读审计 dashboard 指定页面/组件。关注信息架构、视觉层级、卡片滥用、控件语义、移动端溢出风险。输出高风险文件、具体原因、修复优先级。不要改文件。
```

CSS/token 代理：

```text
只读审计 dashboard 指定页面/组件。关注 raw 色值、局部 shadow/radius/gradient、重复 mixin、未使用 style token 的地方。输出可自动扫描规则、典型违规位置、迁移建议。不要改文件。
```

复用代理：

```text
只读审计 dashboard 指定页面/组件。关注重复 summary/filter/list/card/dialog/table 模式、超长 view、通用组件 API 不一致。输出可抽象组件、候选文件、分阶段拆分路线。不要改文件。
```

视觉验证代理只在 dev server 可用时启用：

```text
打开本地 dashboard，分别截桌面和移动端关键页面。检查文本溢出、重叠、按钮换行、卡片嵌套和首屏信息密度。输出截图路径和问题清单。不要改文件。
```

### Step 3: 主代理归并

主代理把子代理结果按以下格式归并：

```text
severity: high | medium | low
category: visual-density | style-system | color-token | component-reuse | maintainability | responsive
file: dashboard/src/...
line: number
problem: 具体问题
recommendation: 建议修复
batch: P0 | P1 | P2 | P3
```

去重原则：

- 同一文件同一原因只保留一条主问题，相关位置作为例子。
- 自动扫描命中的问题，如果没有实际设计影响，降为 low 或记录为技术债。
- 会影响移动端使用、可维护性或主题切换的问题优先级上调。

## 修复批次

### P0: 建立规则与基线

- 保留 `pnpm design:audit` 作为固定入口。
- 记录当前 high/medium 问题数量。
- 先不把 warning 接入 CI fail。

### P1: 低风险公共模式

- 从 `Commands.vue` / `Tools.vue` 开始。
- 抽 `SummaryMetricBand`、`FilterToolbar`、`EmptyState`、`CommandCard`、`CommandListRow`。
- 验证 view 文件行数下降，card/list 分支更清晰。

### P2: 配置资源页

- 治理 `Instances.vue` / `MessagePlatforms.vue`。
- 抽 `ConfigValidationAlert`、`SearchLayoutToolbar`、`ResourceCardShell` 或 `ConfigResourcePageFrame`。
- 领域差异通过 slots 和 composable 保留。

### P3: 长文件拆分

- 拆 `Permissions.vue` 为 group list、group editor、binding panel、command override panel。
- 拆 `SessionDetailPanel.vue` 为 stats、history、state、summary panel。
- 整理 `ProviderEditor.vue` 的 editor card 和 inline editor 结构。

### P4: 样式系统收敛

- 去掉重复 mixin，例如重复的 `analysis-section-panel`。
- 把高频 opacity、surface、border、shadow、radius 迁到 token。
- 将普通 CSS style block 迁为 SCSS。

### P5: 自动化门禁

- 使用 `pnpm design:audit:strict` 防止 high finding 回归。
- 先只 gate high；medium/low 通过报告持续暴露，不在当前阶段阻塞构建。
- 后续当某类 medium 基线收敛后，再新增对应 `--max-*` 或分类门禁。

## 验收

每个修复批次至少执行：

```bash
pnpm design:audit
pnpm design:audit:strict
pnpm build
```

涉及页面布局的批次还要做桌面和移动端截图检查。验收关注：

- high findings 是否减少或有明确豁免。
- 页面是否少了重复 card/filter/summary 手写结构。
- 新增样式是否没有 raw 色值、裸 shadow、裸 radius。
- 移动端没有文本重叠和按钮不可用问题。
