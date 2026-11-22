# Prometheus Alertmanager & Rancher Monitoring 知識庫 (GitHub Pages 靜態網站)

本目錄 (`docs/`) 構成了一個**完整、免編譯、可直接透過 GitHub Pages 託管**的現代化靜態知識庫與互動架構看板。

---

## 📁 網站目錄結構

```
docs/
├── .nojekyll                                 # 指示 GitHub Pages 停用 Jekyll，完整託管原生靜態檔案
├── index.html                                # 網站主首頁：整合完整架構指南、即時搜尋、主題切換與內嵌預覽
├── alertmanager-rancher-explainer.html       # [Archify] 單頁互動知識總覽 (Explainer)
├── alertmanager-architecture.html            # [Archify] 系統拓撲架構互動圖 (Architecture)
├── alertmanager-ha-sequence.html             # [Archify] HA 階梯延遲去重時序圖 (Sequence)
├── alertmanager-design-and-rancher-integration.md # 原始繁體中文架構與實作長篇 Markdown
└── README.md                                 # 本部署說明手冊
```

---

## 🚀 如何在 GitHub Pages 上發布

您可以選擇以下任一種方式發布本靜態網站：

### 方法 A：透過分支目錄託管 (最推薦 · 免 CI 時間 · 零設定)

1. 將本專案推送 (push) 至您的 GitHub 倉庫。
2. 進入倉庫頁面，點選頂部 **Settings**（設定）分頁。
3. 在左側導覽列中點選 **Pages**。
4. 在 **Build and deployment** > **Source** 下拉選單選擇：
   - **Deploy from a branch**
5. 在 **Branch** 選項中選擇：
   - 分支：`main`（或您當前的主分支）
   - 資料夾：`/docs`
6. 點擊 **Save**。
7. 約等待 1~2 分鐘後，即可透過 `https://<你的帳號>.github.io/<倉庫名稱>/` 瀏覽本網站！

### 方法 B：透過 GitHub Actions 自動發布

本倉庫已預先配置好 GitHub Actions 工作流：[`.github/workflows/deploy-pages.yml`](../.github/workflows/deploy-pages.yml)。
- 進入倉庫 **Settings** > **Pages**。
- 將 **Source** 切換為 **GitHub Actions**。
- 當 `docs/` 目錄有任何提交並推送到 `main` 分支時，Actions 將自動觸發並完成部署。

---

## 💻 本地離線預覽 (Local Preview)

由於所有 HTML、CSS 與 JavaScript 均為純靜態設計，您可隨時在本地預覽：

### 方式 1：直接雙擊開啟
直接使用任何瀏覽器開啟 `docs/index.html`（或以 `file:///` 協議打開）。

### 方式 2：使用 Python 內建 HTTP 伺服器
```bash
# 在專案根目錄下執行：
python3 -m http.server 8080 --directory docs

# 瀏覽器訪問：
http://localhost:8080
```

---

## 🎨 核心功能與 Archify 互動套件

1. **頂部 Archify 互動看板**：
   - 包含三套由 Archify 產出的高階動態圖表（支援深/淺色主題與 SVG/WebM 匯出）：
     - **🌟 單頁知識總覽**：`alertmanager-rancher-explainer.html`
     - **📐 系統架構拓撲**：`alertmanager-architecture.html`
     - **⏱️ HA 階梯去重時序**：`alertmanager-ha-sequence.html`
   - 支援「**在新分頁開啟 ↗**」與「**內嵌即時預覽 (Modal View) 🔍**」兩種互動方式。
2. **完整技術指南手冊**：
   - 包含 Alertmanager 模組對照、`aggrGroup` 時間參數、Gossip 協議、Position 階梯等待演算法、Rancher Monitoring v2 多租戶 CRD 隔離、GitOps 三層 Namespace 注入實踐、生產環境清單與除錯工具流。
3. **沉浸式體驗**：
   - 側邊欄即時關鍵字過濾搜尋（Search Filter）。
   - 一鍵切換深色 / 淺色主題（自動記住設定）。
   - 程式碼區塊（YAML / Go / Bash）一鍵複製與語法突顯。
