# Alertmanager 架構設計與 Rancher 整合指南 {#alertmanager-guide}

Alertmanager 透過無中心 Gossip 叢集實作「Fail-Open」高可用性，並結合 Prometheus Operator 提供宣告式多租戶告警管理。

- 依據標籤計算 GroupKey，透過三個計時器達成告警聚合降噪。
- Memberlist 協議在節點間同步 nflog 去重日誌與靜音資料。
- 節點位置排序偏置等待時間，先發者廣播發送紀錄防止重複。
- Rancher 藉由 CRD 強制注入 namespace 標籤實現專案級隔離。

@answer scope="Alertmanager v0.27+ · Rancher Monitoring v2 · HA Gossip"
@meta locale=zh-TW
@source repo_src title="Alertmanager 原始碼" url=https://github.com/prometheus/alertmanager

## 告警管線如何完成路由匹配與分組發送？ {#pipeline}

@workflow id=alert-flow title="告警生命週期處理流"
| lane ingest | 接收與路由 |
| lane dispatch | 聚合調度 |
| lane notify | 管線過濾與發送 |
| rcv | 接收告警 | external | ingest |
| tree | 遍歷路由樹 | backend | ingest |
| aggr | 計算 GroupKey | backend | dispatch |
| wait | group_wait 緩衝 | backend | dispatch |
| filter | 抑制與靜音過濾 | backend | notify |
| dedup | nflog 去重檢查 | backend | notify |
| send | 調用端點發送 | backend | notify |
| rcv > tree | POST /alerts |
| tree > aggr | 命中路由 |
| aggr > wait | 存入 store |
| wait > filter | 觸發 flush |
| filter > dedup | 未被靜音 |
| dedup > send | 首次發送 | emphasis |

@steps id=pipeline-steps
| step-rcv | 接收告警 | Prometheus 透過 HTTP POST 推送活躍與恢復告警。 | alert-flow | rcv |
| step-tree | 遍歷路由樹 | 以 DFS 深度優先比對標籤，支援 continue 多路轉發。 | alert-flow | tree |
| step-wait | group_wait 緩衝 | 新群組等待設定時間，聚合突發的多筆關聯告警。 | alert-flow | wait |
| step-filter | 抑制與靜音過濾 | 依據 inhibit_rules 與 Silence 規則剔除衍生告警。 | alert-flow | filter |
| step-send | 調用端點發送 | 經由 nflog 去重驗證後，調用 Webhook 或 PagerDuty。 | alert-flow | send |

## 無中心 HA 叢集如何杜絕重複通知？ {#ha-clustering}

@sequence id=ha-flow title="階梯等待與 Gossip 廣播時序"
| actor prom | Prometheus | cloud |
| actor am0 | Alertmanager 0 | backend |
| actor am1 | Alertmanager 1 | backend |
| actor slack | Slack | external |
| prom | am0 | 推送告警 |
| prom | am1 | 推送告警 |
| am0 | slack | HTTP POST (發送通知) | emphasis |
| slack | am0 | 200 OK | return |
| am0 | am1 | 廣播 nflog 紀錄 (:9094) | dashed |
| am1 | am0 | 狀態校驗 (t=15s) |
| am0 | am1 | 已發送 (靜默攔截) | return |

@steps id=ha-steps
| step-h0 | 多播直連 | Prometheus 同時將告警推送到叢集所有節點。 | ha-flow | prom |
| step-h1 | 階梯等待 | 節點依名稱排序計算等待時間：Position 乘以 15 秒。 | ha-flow | am0 |
| step-h2 | 先發廣播 | 節點 0 等待 0 秒立即發送，並透過 Gossip 廣播日誌。 | ha-flow | am0 |
| step-h3 | 接替去重 | 節點 1 等待 15 秒醒來，查驗日誌確認已發送即拋棄。 | ha-flow | am1 |

## Rancher Monitoring 兩種配置模式如何取捨？ {#rancher-modes}

| 評估面向 | 全域 Secret 模式 | 命名空間 AlertmanagerConfig |
| --- | --- | --- |
| 適用對象 | 基礎設施與平台維運團隊 | 業務開發與微服務應用團隊 |
| 隔離機制 | 無隔離，可存取全叢集告警 {caution:權限集中} | 自動注入 namespace 標籤匹配 {good:嚴格隔離} |
| 變更影響 | 格式錯誤將影響整個叢集 {bad:全域風險} | 僅限於當前命名空間 {good:爆炸半徑小} |
| 維護方式 | 修改 cattle-monitoring-system Secret | 宣告式 YAML 提交至 GitOps |

## 生產維運排錯與部署檢核表 {#ops-checklist}

- [ ] Prometheus 直連所有 Alertmanager 實例，嚴禁架設負載均衡器。
- [ ] 節點間 TCP 與 UDP 9094 埠均已放行，確保心跳與狀態同步。
- [ ] 為 Alertmanager StatefulSet 掛載 PVC 持久化儲存目錄。
- [ ] `--cluster.advertise-address` 明確設定為 IP 位址而非網域名稱。
- [ ] 根因告警配置節點級抑制規則，防止主機宕機引發告警風暴。
