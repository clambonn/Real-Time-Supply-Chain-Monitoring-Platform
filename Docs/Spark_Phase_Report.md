# Spark Phase (Phase 5) – Completion Report

## 1. Verdict
Spark code, validation, Gold contract, tests and documentation are complete and verified **in a local-filesystem
sandbox**. The HDFS/Docker execution could **not** be performed in the environment used for this work
(no Docker daemon, no HDFS). One run through Docker is therefore still required before presenting the
phase as done in your environment: `docker compose -f docker-compose.hadoop.yml run --rm -e HADOOP_USER_NAME=hadoop spark-runner bash run_pipeline.sh`
(expected: validation exit 0 and `GOLD VERIFICATION: 51 passed, 0 failed`).

Progress by workstream (only what was actually verified is counted as done):

| Workstream | Done |
|---|---|
| Audit | 100% |
| Validation & QA report | 100% (local) |
| 4 analytical jobs | 100% (local) |
| Gold verification / contract | 100% (local) |
| Documentation | 100% |
| Execution on HDFS in Docker | 0% – needs your environment |

## 2. Audit findings (defects found in the original code)
1. **Recursive file search** (`find_file`) in all 4 jobs – replaced by fixed paths in `common.py`. It could silently pick another file with the same name.
2. **Reviews read without `multiLine`** – quoted comments contain line breaks; the validation counted wrong rows and could split records.
3. **`inferSchema`** everywhere – types guessed per run; replaced by explicit conversion with unparseable values counted.
4. **Validation covered only 3 of 9 datasets**, nulls only as a total, payments key skipped ("N/A"), reviews checked on `review_id` alone (wrongly implies 814 duplicates), no date/referential/business checks, no report, no exit code.
5. **Inconsistent late rule**: delivery job used calendar days, seller job timestamps (7,826 vs 6,534 late orders on the same population).
6. **Delivery job** counted orders canceled-with-date as delivered (6 orders) and did not flag date anomalies.
7. **Payment comparison** compared payments with item price only; ~98.3k of 98.7k orders showed a "difference" equal to freight. Inner join dropped 775 orders without items and 1 without payment.
8. **Product job** joined translation before aggregating without checking key uniqueness; missing categories produced NULL group keys.
9. **Silver freshness never verified** (`spark_audit.ps1` only checks existence). Now compared by row count + content hash.
10. No Gold verification existed; Gold could be stale or inflated unnoticed. No handoff contract documented.
11. Outdated: `Docs/Spark_Analytics.md` (now rewritten). `project_clean.zip` inside the ZIP was a 0-byte file (removed).
No Git metadata was present in the archive. Silver orders differ from Bronze only by the estimated date format (`yyyy-MM-dd`), as expected from cleaning.

## 3. Roadmap compliance checklist
| Requirement (roadmap Phase 5 / DoD) | Existing | Gap | Action | Verification | Status |
|---|---|---|---|---|---|
| Connect Spark to HDFS | `hdfs://namenode:9000`, Docker service | Not testable here | Kept infrastructure, central config | Not executed on HDFS | **Pending your run** |
| Read datasets | 3 datasets in validation, 4 jobs | wrong parsing (multiline), search | Explicit paths/types, all 9 read | Local run, row counts match source | Done (local) |
| Transformations, joins, aggregations | present | grain/rule bugs (section 2) | Fixed, anti-multiplication checks | Totals reconciled with inputs, pandas cross-check | Done (local) |
| Analytical outputs for Kafka/Airflow | Parquet outputs | no contract | Contract in doc + `verify_gold.py` | 51 checks | Done (local) |
| Define outputs/conventions before Kafka/Airflow | none | undocumented | `Docs/Spark_Analytics.md` §4–5 | Read-through vs real schemas | Done |
| Setup/run instructions testable by another member | partial | no runner | `run_pipeline.sh`, runbook | Executed locally from empty Gold | Done (local); other-member test pending |
| Input/output formats, schemas, paths, dependencies documented | partial | outdated | rewritten | schemas taken from actual Parquet | Done |
| Basic validation / test evidence | none | none | QA report, verify_gold, negative tests | see §5 | Done (local) |
| Demo and handoff | – | – | – | – | Team action |
Not implemented (outside Spark): Kafka, Airflow.

## 4. Files
Changed: `spark/read_and_validate.py` (rewritten), `spark/delivery_performance.py`, `seller_performance.py`, `product_category.py`, `payment_analysis.py` (rewritten), `Docs/Spark_Analytics.md` (rewritten), `docker-compose.hadoop.yml` (one added line: `./Data:/data:ro` volume on `spark-runner`, needed for the Silver freshness check; nothing else touched), `README.md` (pointer to Spark docs).
Added: `spark/common.py`, `spark/verify_gold.py`, `spark/run_pipeline.sh`, `spark/reports/{qa_report.md,qa_results.csv,last_run_log.txt}`, `Docs/Spark_Phase_Report.md`.
Removed from ZIP: empty `project_clean.zip`. Unchanged: Kafka, SQL, Snowflake, notebooks, data, Dockerfile, `spark_audit.ps1`, requirements.
Note: Python files now use LF line endings (originals were CRLF); no functional effect.

## 5. Test evidence (actually executed, local filesystem lake = copy of `Data/raw` + `Data/cleaned`)
Environment: Python 3.12.3, PySpark 4.0.0, OpenJDK 21, Linux. Full output: `spark/reports/last_run_log.txt`.
* `bash run_pipeline.sh` from an empty `analytics/` folder: exit 0; validation `PASS 72 / INFO 9 / WARNING 15 / FAIL 0`; all four jobs completed; `GOLD VERIFICATION: 51 passed, 0 failed`.
* Gold row counts read back: delivery detail 96,470 / monthly 23 / state 27; seller summary 3,095 / detail 112,650; products 32,951 / categories 74; payment types 5 / order comparison 99,441.
* Independent pandas cross-check (separate code path): evaluable orders 96,470 and late 6,534 equal Spark; per-seller sales, order counts, delivered and late counts: max difference 0; totals items 112,650, price 13,591,643.70.
* Negative tests: altering one value in a copy of Silver orders → `FAIL ... content hash DIFFERENT` (exit 1); deleting one Gold dataset → `verify_gold` FAIL (exit 1).
* Not executed: any run against HDFS/Docker, the Docker image build, `spark_audit.ps1`, execution by a second team member, PySpark 4.3.0/Java 17.

## 6. Unresolved issues
1. Run the pipeline once inside Docker/HDFS and attach the output (required by the roadmap's Definition of Done). Silver on HDFS may be stale: the check will tell.
2. Source-data anomalies listed in `Docs/Spark_Analytics.md` §6 are reported, not corrected; the cleaning phase owners should decide (e.g. 166 orders with carrier date before purchase).
3. Streamed orders in SQL Server are not part of the HDFS snapshot.

## 7. Starting the next phase
Read `Docs/Spark_Analytics.md` §4 (contract) and §5. Kafka pair: join events to Gold on `order_id`/`seller_id`. Airflow pair: one task per script of `run_pipeline.sh`, same order, exit code = task status. Run `verify_gold.py` after every refresh.
