# Project context restored from earlier TCBA conversations

**Identity**: HDB Carpark Availability Prediction — Azure final, local-first implementation.

## Locked scope
- Source A: https://api.data.gov.sg/v1/transport/carpark-availability, accepts `date_time` in Singapore time for historical observations.
- Source B: https://data.gov.sg/datasets/d_23f946fa557947f93a8043bbef41dd09/view, HDB carpark metadata.
- Initial model: 200 selected carparks, car-lot type `C` only; collect the full raw source snapshot in Bronze.
- Hourly model features; target is occupancy rate one hour ahead.
- Initial historical data: four to six weeks; data splits chronological, never random.
- Compare persistence and seasonal baselines to HistGradientBoostingRegressor; try LightGBM only if justified.
- Retrain every three days using a rolling window; candidate promotion gated on a **same-window** comparison against incumbent, with rollback support.
- Track MAE primary, RMSE and R² secondary; report slices by carpark/hour/weekend.
- Local Bronze JSON, Silver Delta, Gold Delta/Parquet; local DuckDB as SQL exploration.
- Final Azure ADLS Gen2, Synapse Spark, Synapse Serverless SQL (Parquet-compatible Gold export), Azure Functions, Synapse Pipelines, Power BI, Managed Identities + RBAC, Azure Cost Management, GitHub Actions.
- Azure for Students credit limit $100; spend target under $80 (historical estimates are not current quotes).

## Confirmed Azure progress from earlier chats
- Prior RBAC lab: custom HDB Data Engineer role on bronze, HDB Data Analyst reader on gold; ML engineer needs distinct read-on-silver and write-on-artifacts scopes.
- Non-human workload identities are implemented only when the corresponding cloud service exists.
- RBAC exercises and course midterm experiments are **not** proof this final project's cloud infrastructure is already deployed.

## Important corrections to earlier plan
1. Four weeks of hourly backfill is **672 hourly API calls/JSON files**, not >5,000 files; 200 carparks x 672 hours ≈134,400 candidate observations before filtering.
2. Six weeks of data are more appropriate for a distinct four-week train + one-week validation + one-week test study. With only four weeks, define a different chronological split.
3. Recomputing rolling features for inference must use only information available as of time t; don't confuse forecast time t+1h with input hour t.
4. Historical-same-hour target for next hour from current t is a *different index* than previous day's current hour; implement carefully in the modeling phase.
5. An incumbent model and candidate must be evaluated on the **same** newly held-out validation window before promotion; do not compare scores from incomparable older windows or allow a routinely worse model by default.
6. A 5-minute ingest cadence is not the same as 1-hour model features; normalize to hourly after collecting raw.
7. First local ETL refreshes the complete Silver dataset. Incremental Delta MERGE, Bronze->Gold, streaming, retraining, API, and dashboards remain for subsequent phases.
