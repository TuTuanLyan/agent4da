# Agent API

Current frontend backend: `app/backend/api/main.py`, Docker service
`agent-api`, container `agent4da`, Swagger at `http://localhost:8083/docs`.
This document below is the legacy standalone `code/agent/main_agent.py` API.

Tai lieu nay mo ta API agent moi trong `code/agent/main_agent.py`. Muc tieu la
co Swagger de demo Text-to-SQL, context theo session, table data va chart
suggestion de frontend/backend app map tiep.

## 1. Chay API

Chay tu repo root:

```bash
/opt/miniconda/envs/agent4daenv/bin/python code/agent/main_agent.py
```

Hoac chay uvicorn tu `code/agent`:

```bash
cd /home/lyan/Project/BigData/Agent4DA/code/agent
AGENT_API_PORT=8001 /opt/miniconda/envs/agent4daenv/bin/python -m uvicorn main_agent:app --host 0.0.0.0 --port 8001
```

Mo Swagger:

```text
http://localhost:8001/docs
```

API tu load env neu co:

- `envs/endpoint.env`
- `envs/groq.env`
- `envs/postgre.env`
- `envs/iceberg.env`

## 2. API chinh

### `POST /api/v1/agent/context/init`

Tao schema/table `app_context` trong PostgreSQL. API nay nen goi truoc khi demo.
Code ghi qua Trino catalog `postgres`, nen can Trino va PostgreSQL dang chay.

### `GET /api/v1/agent/context/health`

Kiem tra context store co dung duoc khong.

### `POST /api/v1/agent/sessions`

Tao chat session moi.

Request:

```json
{
  "user_id": "default",
  "session_name": "Demo"
}
```

Response tra `session.session_id`. Frontend giu id nay.

### `GET /api/v1/agent/sessions/{session_id}/context`

Load compact context cua session. Dung khi nguoi dung quay lai doan chat.

### `GET /api/v1/agent/sessions/{session_id}/messages`

Load lich su message de render chat.

### `GET /api/v1/agent/sessions/{session_id}/queries`

Load query logs de debug SQL.

### `POST /api/v1/agent/ask`

API agent chinh.

Request toi thieu:

```json
{
  "question": "Sản phẩm nào có lượt xem nhiều nhất thuộc brand nào?",
  "user_id": "default"
}
```

Request follow-up:

```json
{
  "question": "Vẽ biểu đồ cột nhưng bỏ qua samsung",
  "session_id": "SESSION_ID_TU_REQUEST_TRUOC",
  "user_id": "default",
  "max_sql_retries": 3,
  "max_requery_rounds": 1,
  "chart_type": "auto"
}
```

Response quan trong:

```json
{
  "request_id": "...",
  "session_id": "...",
  "status": "success",
  "answer_kind": "data_answer",
  "text_answer": "...",
  "sql": "SELECT ...",
  "result": {
    "columns": ["product_id", "brand", "views"],
    "rows": [{"product_id": 123, "brand": "apple", "views": 456}],
    "row_count": 1,
    "is_truncated": false
  },
  "chart_suggestion": {
    "chart_type": "bar",
    "x": "product_id",
    "y": "views",
    "reason": "..."
  },
  "analysis": {
    "insight_summary": "...",
    "missing_info": {
      "has_missing_info": false,
      "items": []
    }
  },
  "context": {
    "compact": {},
    "updated_at": "..."
  }
}
```

`answer_kind` co the la:

- `data_answer`: co data va tra loi binh thuong.
- `clarification`: can nguoi dung hoi ro hon.
- `no_data`: Gold khong co data/cot de tra loi chinh xac.
- `error`: co loi guard, SQL, Trino hoac LLM.

## 3. Chart contract cho FE

Frontend can 3 phan:

- `result.columns`: ten cot.
- `result.rows`: data that dang list object.
- `chart_suggestion`: `chart_type`, `x`, `y`.

Quy uoc:

- `x` va `y` luon la ten cot co trong `result.columns`.
- Pie: FE map `name = row[x]`, `value = row[y]`.
- Toggle chart type o FE co the giu nguyen `x/y`.
- Neu backend khong chon duoc chart, `chart_type` la `table` hoac `none`.

## 4. App context

Schema PostgreSQL: `app_context`.

Bang tao tu dong:

- `users`
- `chat_sessions`
- `chat_messages`
- `session_contexts`
- `langgraph_checkpoints`
- `ai_query_logs`

Context compact luu theo session de cau hoi sau co the bo sung ngu canh:

- cau hoi truoc
- SQL truoc
- columns va sample rows truoc
- chart suggestion truoc
- summary ngan cua cac turn gan nhat

Frontend nen:

1. Tao session bang `/api/v1/agent/sessions`.
2. Goi `/api/v1/agent/ask` voi `session_id`.
3. Khi quay lai chat, goi `/api/v1/agent/sessions/{session_id}/context`.
4. Tiep tuc goi `/api/v1/agent/ask` voi cung `session_id`.

## 5. Case test Swagger

### Case 1: Top product theo views, co brand

`POST /api/v1/agent/ask`

```json
{
  "question": "Sản phẩm nào có lượt xem nhiều nhất thuộc brand nào?",
  "user_id": "default",
  "max_sql_retries": 3,
  "max_requery_rounds": 1,
  "chart_type": "auto"
}
```

Ky vong:

- `answer_kind = data_answer`
- `result.columns` co `product_id`, `brand`, metric views/view_count.
- `sql` doc bang Gold, thuong la `gold.daily_product_summary`.
- `chart_suggestion.x/y` nam trong `result.columns`.

### Case 2: Follow-up ve bieu do va loai Samsung

Dung `session_id` cua Case 1:

```json
{
  "question": "Vẽ biểu đồ cột nhưng bỏ qua samsung",
  "session_id": "SESSION_ID_CUA_CASE_1",
  "user_id": "default",
  "max_sql_retries": 3,
  "max_requery_rounds": 1,
  "chart_type": "auto"
}
```

Ky vong:

- Agent dung context cua Case 1.
- SQL duoc re-run va co filter loai brand samsung neu metadata/data phu hop.
- `chart_suggestion.chart_type` uu tien `bar`.

### Case 3: Hoi ten san pham khong co trong data

Dung `session_id` cua Case 1:

```json
{
  "question": "Sản phẩm ở câu hỏi trước tên gì?",
  "session_id": "SESSION_ID_CUA_CASE_1",
  "user_id": "default"
}
```

Ky vong:

- `status = success`
- `answer_kind = no_data`
- `text_answer` noi ro Gold chi co `product_id`, khong co `product_name`.
- Khong duoc bia ten san pham.

### Case 4: Cau hoi chi yeu cau chart nhung khong co context

```json
{
  "question": "Vẽ biểu đồ cột",
  "user_id": "default"
}
```

Ky vong:

- `answer_kind = clarification`
- Agent hoi nguoi dung muon ve theo chi so nao.

## 6. Gioi han hien tai

- LLM dang dung Groq qua OpenAI-compatible API.
- Chua co auth trong agent API; app backend se xu ly sau.
- Chua co API rename/delete session.
- Default SQL limit: `AGENT_DEFAULT_SQL_LIMIT=100`.
- Default rows tra FE: `AGENT_MAX_RESPONSE_ROWS=100`.
- Re-query khi thieu field dang la heuristic don gian theo keyword.
