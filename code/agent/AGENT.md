# Agent4DA Agent API

Thu muc `code/agent/` chua FastAPI + LangGraph agent cho demo Text-to-SQL tren
Gold data layer cua repo.

## Muc tieu hien tai

- Nhan cau hoi tu nhien bang tieng Viet/English.
- Load semantic metadata cua Gold tu Trino, fallback sang metadata tinh trong
  `code/spark/gold/metadata_definitions.py`.
- Sinh SQL Trino bang Groq, guard SQL chi doc, chay query qua Trino.
- Tra ve text answer, table rows, columns va chart suggestion de FE ve bieu do.
- Luu app context theo `session_id` trong PostgreSQL schema `app_context`.
- Ho tro follow-up question bang compact context cua doan chat.

## API chinh

- `POST /api/v1/agent/ask`: API chinh cho chat voi context.
- `POST /api/v1/agent/sessions`: tao session chat moi.
- `GET /api/v1/agent/sessions/{session_id}/context`: lay compact context.
- `GET /api/v1/agent/sessions/{session_id}/messages`: lay message history.
- `GET /api/v1/agent/sessions/{session_id}/queries`: lay query logs.
- `GET /api/v1/agent/context/health`: kiem tra app_context store.
- `POST /api/v1/agent/context/init`: tao schema/table app_context.

API cu van giu de test nhanh:

- `POST /ask`
- `POST /api/v1/ask`
- `GET /api/v1/metadata`
- `GET /api/v1/schema-context`
- `POST /api/v1/guard/question`
- `POST /api/v1/guard/sql`

## Node trong graph

Flow hien tai:

```text
guard_question
  -> load_metadata
  -> check_answerability
  -> build_prompt
  -> generate_sql
  -> guard_sql
  -> execute_sql
  -> profile_result
  -> validate_result
  -> plan_chart
  -> generate_insight
  -> build_final_response
```

Y nghia nhanh:

- `guard_question`: chan cau hoi pha data/prompt injection don gian.
- `load_metadata`: nap metadata Gold.
- `check_answerability`: tra `clarification` hoac `no_data` som neu Gold khong co
  du lieu can thiet, vi du hoi ten san pham trong khi Gold chi co `product_id`.
- `build_prompt`: dua schema + app context + retry/requery context vao prompt.
- `generate_sql`: goi Groq sinh SQL.
- `guard_sql`: chi cho SELECT/WITH SELECT va tu them LIMIT mac dinh.
- `execute_sql`: chay SQL qua Trino, retry khi SQL loi.
- `profile_result`: nhan dien columns, numeric/time/categorical.
- `validate_result`: neu query thieu cot quan trong ma Gold co, yeu cau re-run SQL.
- `plan_chart`: chon chart suggestion co `chart_type`, `x`, `y`.
- `generate_insight`: LLM tom tat dua tren query result.
- `build_final_response`: chuan hoa JSON tra ve cho FE/BE.

## Tool wrappers

`code/agent/tools/agent_tools.py` dinh nghia cac tool don gian:

- `get_schema_tool`
- `validate_sql_tool`
- `query_trino_tool`

Chung dang la wrapper Python de code ro rang; graph hien dieu phoi truc tiep bang
node/service thay vi dung tool-calling phuc tap.

## App context

Context duoc luu trong PostgreSQL thong qua Trino catalog `postgres`, schema
`app_context`. Cac bang duoc tao tu dong:

- `users`
- `chat_sessions`
- `chat_messages`
- `session_contexts`
- `langgraph_checkpoints`
- `ai_query_logs`

Context compact luu cac thong tin gan nhat:

- `last_question`
- `last_sql`
- `last_result_columns`
- `last_result_sample`
- `last_chart_suggestion`
- `conversation_summary`
- `turns`

FE chi can giu `session_id`. Khi nguoi dung quay lai doan chat, goi API context
de load compact context va goi tiep `/api/v1/agent/ask` voi cung `session_id`.

## Chart contract

Response chinh luon co:

```json
{
  "result": {
    "columns": ["brand", "views"],
    "rows": [{"brand": "apple", "views": 123}],
    "row_count": 1,
    "is_truncated": false
  },
  "chart_suggestion": {
    "chart_type": "bar",
    "x": "brand",
    "y": "views",
    "reason": "..."
  }
}
```

`chart_suggestion.x` va `chart_suggestion.y` luon duoc validate de khop voi
`result.columns`. FE co the toggle bar/line/pie/table/scatter tren cung `x/y`.

## Gioi han demo

- Tam thoi dung Groq, chua them Gemini fallback.
- Context store di qua Trino PostgreSQL connector de tranh them dependency Python.
- Auth/user management de app backend xu ly sau; agent chi dung `user_id=default`
  neu khong truyen.
- Re-query khi ket qua thieu thong tin la heuristic don gian, uu tien de demo de
  hieu hon la framework doanh nghiep phuc tap.
