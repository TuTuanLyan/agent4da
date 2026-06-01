# Agent FastAPI

Current frontend backend: `app/backend/api/main.py`, Docker service
`agent-api`, container `agent4da`, Swagger at `http://localhost:8083/docs`.
The older `localhost:8001` notes below refer to the standalone legacy agent.

Tai lieu nay mo ta phan AI Agent theo Solution Design Specification: Text-to-SQL
cho lop Gold ecommerce, chi doc du lieu, co guardrail truoc khi truy van Trino,
va expose API FastAPI de thu tai `localhost:8001/docs`.

## 1. Muc tieu

- Nhan cau hoi tu nhien bang tieng Viet/English.
- Doc semantic metadata tu `iceberg.metadata.semantic_table_catalog` va
  `iceberg.metadata.semantic_column_catalog`.
- Neu Trino/metadata chua san sang, API metadata co fallback sang dinh nghia tinh
  trong `code/spark/gold/metadata_definitions.py`.
- Sinh SQL Trino bang LLM, chi cho phep `SELECT` hoac `WITH ... SELECT`.
- Thuc thi SQL qua Trino tren Gold Iceberg tables.
- Tra ve insight, profile ket qua, chart spec, bang du lieu, SQL da chay va
  `missing_info` neu ket qua chua du de tra loi.

## 2. Luong Agent

```text
guard_question
  -> load_metadata
  -> build_prompt
  -> generate_sql
  -> guard_sql
  -> execute_sql
  -> profile_result
  -> plan_chart
  -> generate_insight
  -> build_final_response
```

Neu Trino bao loi SQL, Agent dua loi vao prompt va thu sinh lai SQL toi da 3 lan.
Neu guard cau hoi hoac guard SQL chan yeu cau, Agent dung som va khong goi Trino.

## 3. Bao mat va bat bien du lieu

- Agent chi duoc doc du lieu. Khong co endpoint nao ghi/sua/xoa du lieu.
- Cau hoi co y dinh pha data nhu xoa bang, xoa du lieu, cap nhat, them du lieu,
  hoac yeu cau bo qua quy trinh truy van se bi tu choi truoc khi goi LLM.
- SQL sinh ra phai la mot statement duy nhat va bat dau bang `SELECT` hoac `WITH`.
- Cac keyword `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `TRUNCATE`,
  `CREATE`, `MERGE`, `CALL`, `GRANT`, `REVOKE`, `EXECUTE` bi chan.
- Neu SQL khong an toan, API tra loi loi va xoa `generated_sql`, khong thuc thi.
- LLM chi duoc tom tat dua tren `query_result`; neu thieu thong tin thi ghi trong
  `analysis.missing_info`, khong duoc bia so lieu.

## 4. API chinh

Mo Swagger UI:

```text
http://localhost:8001/docs
```

Endpoints:

- `GET /health` va `GET /api/v1/health`: kiem tra API dang song.
- `GET /api/v1/metadata`: xem danh sach bang/cot Agent co the dung.
- `GET /api/v1/schema-context`: xem context metadata dua vao prompt.
- `POST /api/v1/guard/question`: kiem tra cau hoi co bi chan hay khong.
- `POST /api/v1/guard/sql`: kiem tra SQL co an toan/read-only hay khong.
- `POST /ask` va `POST /api/v1/ask`: chay toan bo Agent.

Vi du body cho `/api/v1/ask`:

```json
{
  "question": "Doanh thu theo ngay trong thang 1 nam 2020",
  "max_retries": 3
}
```

Output chinh gom:

- `status`: `success` hoac `error`.
- `readonly`: luon la `true`.
- `sql`: SQL da duoc guard va thuc thi.
- `result.rows`: du lieu tra ve tu Trino.
- `analysis.insight_summary`: tom tat ngan gon.
- `analysis.insight_error`: loi rieng cua buoc sinh insight neu LLM bi timeout
  hoac rate limit; loi nay khong lam mat du lieu bang da truy van thanh cong.
- `analysis.missing_info`: thong tin con thieu neu ket qua chua du.
- `visualization.chart_spec`: goi y bieu do cho frontend.
- `blocks`: cac khoi hien thi cho UI chat.

## 5. Cach chay local

Dung conda env da co dependency:

```bash
cd /home/lyan/Project/BigData/Agent4DA
/opt/miniconda/envs/agent4daenv/bin/python code/agent/main_agent.py
```

`main_agent.py` tu dong nap cac file env neu ton tai:
`envs/endpoint.env`, `envs/groq.env`, `envs/postgre.env`, `envs/iceberg.env`.
Neu khong dung cac file nay, co the export thu cong `GROQ_API_KEY`,
`TRINO_HOST`, `TRINO_PORT`, `TRINO_USER` truoc khi chay.

Sau do mo:

```text
http://localhost:8001/docs
```

Neu chay bang `uvicorn`, nen chay tu `code/agent` de tranh thu muc local `trino/`
shadow Python package `trino`:

```bash
cd /home/lyan/Project/BigData/Agent4DA/code/agent
AGENT_API_PORT=8001 /opt/miniconda/envs/agent4daenv/bin/python -m uvicorn main_agent:app --host 0.0.0.0 --port 8001
```

## 6. Luu y van hanh

- `/api/v1/metadata` co the tra `source=static_definitions` neu Trino metadata
  chua truy cap duoc. Khi do `/ask` van can Trino de thuc thi SQL that.
- Neu `GROQ_API_KEY` chua duoc set, `/health` van OK nhung `/ask` se loi o buoc
  sinh SQL/insight.
- Neu cau hoi qua mo ho hoac ket qua truy van chua du, xem `missing_info.items`
  de biet can bo sung metric, khoang thoi gian, chieu phan tich, hoac can truy
  van bang metadata/Gold khac.
