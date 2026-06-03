# ICEM Dashboard

**ICEM** = *Index · Commodity · Exogenous Monitor*（指数·原料·传导监测台）

静态 ECharts 看板：十一战略赛道指数、期货原料、上游板块传导、主题 ETF、宏观骨架（PMI / 能源 / 融资余额）与 T+1~5 方向预测（**不展示内部模型名称**）。

## 在线查看

仓库启用 [GitHub Pages](https://docs.github.com/pages) 后，访问：

`https://NYCMadison.github.io/icem-dashboard/`

本地也可直接打开 `index.html`（需同目录下的 `data.js`）。

## 仓库内容

| 文件 | 说明 |
|------|------|
| `index.html` | 看板页面 |
| `data.js` | 由构建脚本生成的数据包（`window.ICEM_DATA`） |
| `config_display.json` | 期货、板块、宏观系列的中文展示标签 |
| `build_icem_data.py` | 从 `sector_valuation_analysis` 数据仓聚合生成 `data.js` |

## 本地预览

```bash
git clone https://github.com/NYCMadison/icem-dashboard.git
cd icem-dashboard
open index.html   # macOS
# 或: python -m http.server 8080  后浏览器打开 http://localhost:8080
```

## 重新生成 data.js

构建脚本依赖上一级工程 **`sector_valuation_analysis`**（`data/raw`、`data/warehouse`、`data/processed` 及可选 V15 指标仓）。在完整工程内执行：

```bash
cd sector_valuation_analysis/dashboard
conda run -n ikb_env python build_icem_data.py
```

生成后可将新的 `data.js` 提交到本仓库以更新 Pages。

## 看板模块

1. **十一赛道总览** — 收盘、RSI、PE 分位、T+1 方向与预测价  
2. **期货原料全景** — 主要期货最新价、涨跌与迷你走势  
3. **赛道钻取** — 指数走势、预测区间、关联原料、上游板块、主题 ETF  
4. **宏观骨架** — PMI、能源指数、融资余额（含页面内释义）  
5. **数据健康** — 关键数据文件存在性与 commodity 最新日期  

## License

MIT — see repository license file if present.
