# Data dictionary

Home currency is **BOB** (boliviano). All bronze columns are strings (verbatim); types
below are the silver/gold types.

## Landing zone (daily drop, one set per business date)

| File | Format | Contents |
|---|---|---|
| `customers_YYYYMMDD.csv` | CSV | Customer master snapshot |
| `accounts_YYYYMMDD.csv` | CSV | Account master snapshot |
| `transactions_YYYYMMDD.jsonl` | JSON lines | One object per transaction |
| `fx_rates_YYYYMMDD.json` | JSON | `{date, base: "BOB", rates_to_bob: {ccy: rate}}` |

## Bronze (append-only; all columns String)

Each bronze table mirrors its landing file plus lineage columns:

| Column | Meaning |
|---|---|
| `_ingested_at` | UTC timestamp of the append |
| `_source_file` | Landing file name (also used to detect late records) |
| `_run_id` | Pipeline run that ingested the row |

## Silver

### silver.customers (key: `customer_id`)

| Column | Type | Notes |
|---|---|---|
| customer_id | String | `CUST-00001` |
| full_name | String | trimmed |
| doc_id | String | NIT-like document id; `not_null` (error) |
| birth_date | Date | `not_null` (error); bad dates become null via strict=False cast |
| city | String | Bolivian cities |
| segment | String | retail / premium / business (warn) |
| risk_rating | String | low / medium / high (warn) |
| created_at | Date | |

### silver.accounts (key: `account_id`)

| Column | Type | Notes |
|---|---|---|
| account_id | String | `ACC-000001` |
| customer_id | String | FK → silver.customers (error; orphans quarantined) |
| account_type | String | checking / savings / credit |
| currency | String | BOB / USD (warn) |
| opened_at | Date | |
| status | String | active / dormant / closed (warn) |

### silver.transactions (key: `txn_id`)

| Column | Type | Notes |
|---|---|---|
| txn_id | String | `TXN-YYYYMMDD-000001`; `not_null` + `unique` (error) |
| account_id | String | FK → silver.accounts (error) |
| ts | Datetime(us) | event timestamp |
| event_date | Date | `ts::date` |
| amount | Float64 | original currency; `not_null` + `> 0` (error) |
| currency | String | BOB / USD / EUR (error) |
| amount_bob | Float64 | `amount * rate_to_bob`; null ⇒ `fx_rate_missing` (warn) |
| txn_type | String | deposit / withdrawal / payment / transfer_in / transfer_out |
| channel | String | ATM / POS / web / branch (warn) |
| counterparty | String | |
| merchant_category | String | groceries, fuel, … |
| is_flagged | Boolean | fraud flag from source (~0.5% baseline) |
| is_late | Boolean | event_date < landing-file date (warn) |

### silver.fx_rates (key: `rate_date` + `currency`)

| Column | Type | Notes |
|---|---|---|
| rate_date | Date | |
| currency | String | BOB (=1.0), USD, EUR |
| rate_to_bob | Float64 | BOB per 1 unit of currency |

## Quarantine

Mirror of the silver schema plus:

| Column | Meaning |
|---|---|
| `_reject_reason` | Semicolon-joined violated rule ids (e.g. `amount_not_null;account_exists`) |
| `_quarantined_at` | UTC timestamp |

Conservation invariant, checked every run: distinct bronze keys = silver rows + quarantine rows.

## Gold

### gold.dim_customer (SCD1) / gold.dim_account / gold.dim_date

Straightforward dimensions; `dim_date` spans the transaction ledger with
year/quarter/month/weekday/is_weekend attributes.

### gold.fact_transactions - grain: one row per transaction

`txn_id`, `date_key` → dim_date, `account_id` → dim_account, `customer_id` → dim_customer
(resolved through the account), `amount_orig`, `currency`, `amount_bob`, `txn_type`,
`channel`, `merchant_category`, `is_flagged`, `is_late`.

### gold.kpi_daily - grain: one row per event_date

| Column | Meaning |
|---|---|
| txn_count, volume_bob, avg_txn_bob | Activity measures (BOB) |
| active_accounts | Distinct accounts transacting |
| fraud_flag_rate_pct | % flagged; reconciliation alarms above 3× the 0.5% baseline |
| late_txns | Late-arriving records that day |
| atm/pos/web/branch_txns | Channel breakdown |
| quarantine_rate_pct | Quarantined / (quarantined + clean) transactions |
