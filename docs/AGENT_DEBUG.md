# Agent Debug MVP

## 1. Muc Tieu

Script `code/agent/debug_agent.py` la MVP de debug Agent doc semantic metadata qua Trino, chon bang/cot/metric lien quan, build compact schema context, goi Groq LLM de sinh Trino SQL, guard SQL, query Trino va print trace day du.

Ban nay uu tien de doc va de debug, chua phai production Agent.

## 2. Env Can Co

- `GROQ_API_KEY`: bat buoc de goi Groq OpenAI-compatible API.
- `TRINO_HOST`: default `localhost`.
- `TRINO_PORT`: default `8082`.
- `TRINO_USER`: default `agent4da`.
- `AGENT_MODEL`: default `llama-3.3-70b-versatile`.
- `AGENT_SUMMARIZE`: default `false`; neu `true` thi goi LLM lan 2 de tom tat ket qua.

Dependency Python:

```bash
pip install trino openai
```

`agent4da.env.yml` da co `openai` va duoc bo sung `trino`.

## 3. Cach Chay

```bash
python code/agent/debug_agent.py "Top 5 thương hiệu có doanh thu cao nhất là gì?"
python code/agent/debug_agent.py "Phiên nào có doanh thu cao nhất?"
python code/agent/debug_agent.py "Doanh thu theo ngày là bao nhiêu?"
python code/agent/debug_agent.py --no-execute "Top 10 sản phẩm theo doanh thu"
```

Neu khong truyen question, script dung default:

```text
Top 5 thương hiệu có doanh thu cao nhất là gì?
```

## 4. Y Nghia Output Debug

- `USER QUESTION`: cau hoi dau vao.
- `METADATA LOADED`: so rows doc duoc tu 4 semantic metadata tables.
- `SELECTED TABLE SCORES`: diem rule-based cho tung bang.
- `SELECTED TABLES`: bang duoc dua vao context cho LLM.
- `SELECTED COLUMNS`: cot duoc dua vao context, group theo bang.
- `SELECTED METRICS`: metric lien quan.
- `COMPACT SCHEMA CONTEXT`: schema context ngan gui cho LLM.
- `LLM RAW RESPONSE`: output goc tu Groq.
- `GENERATED SQL`: SQL da parse tu response.
- `SQL GUARD`: PASS/FAIL va ly do.
- `TRINO RESULT SAMPLE`: cot va rows fetch duoc.
- `COPY THIS SQL`: SQL co the copy sang Trino CLI.
- `FINAL ANSWER`: ket qua text ngan.

## 5. Copy SQL Chay Lai Trong Trino CLI

```bash
docker exec -it trino trino
```

Paste SQL tu section `COPY THIS SQL`.

## 6. Gioi Han MVP

- Metadata selection con rule-based don gian.
- Chua co memory.
- Chua dung LangGraph/tool orchestration.
- Chua retry sua SQL tu dong.
- Chua luu query logs.
- Chua co API server/FastAPI.

## 7. Huong Nang Cap

- Them LangGraph voi cac node `load_metadata`, `select_context`, `generate_sql`, `execute_sql`, `summarize`.
- Tach tool `retrieve_metadata` va `execute_sql`.
- Them SQL repair khi Trino bao loi.
- Them query log append table.
- Cache metadata trong backend.
