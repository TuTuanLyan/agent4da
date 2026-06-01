import json

from graph.sql_graph import graph


QUESTIONS = [
    "Nhãn hàng nào có lượt xem nhiều nhất trong 2020 nêu ra 8 nhãn hàng nổi bật sắp xếp từ cao đến thấp và tổng số lượt xem, product/category được nhiều người xem nhất tương ứng của nhãn hàng",
    "Top 8 nhãn hàng có tổng lượt xem cao nhất trong năm 2020",
    "Nhan hang nao duoc xem nhieu nhat trong thang 1 nam 2020",
    "Brand nao duoc them vao gio hang nhieu nhat? Ke ten 4 brand",
    "Doanh thu theo ngay trong thang 1 nam 2020",
    "Cho toi xem 10 dong trong bang daily_product_summary",
]


def preview_rows(rows, limit=5):
    rows = rows or []
    return rows[:limit]


def print_json(title, value):
    print(f"\n{title}:")
    print(json.dumps(value, ensure_ascii=False, default=str, indent=2))


def preview_text(text, limit=300):
    text = text or ""
    if len(text) <= limit:
        return text

    return text[:limit] + "..."


def summarize_blocks(blocks):
    summary = []

    for block in blocks or []:
        item = {
            "type": block.get("type"),
            "title": block.get("title")
        }

        if block.get("type") in ["error", "insight", "sql"]:
            item["content_preview"] = preview_text(block.get("content"))

        if block.get("type") == "chart":
            spec = dict(block.get("spec") or {})
            data = spec.get("data") or []
            spec["data"] = preview_rows(data, limit=3)
            spec["data_row_count"] = len(data)
            item["spec"] = spec

        if block.get("type") == "table":
            rows = block.get("rows") or []
            item["columns"] = block.get("columns") or []
            item["row_count"] = len(rows)
            item["rows_preview"] = preview_rows(rows, limit=3)

        summary.append(item)

    return summary


def summarize_final_answer(final_answer):
    final_answer = final_answer or {}
    result = final_answer.get("result") or {}

    return {
        "keys": list(final_answer.keys()),
        "status": final_answer.get("status"),
        "result": {
            "row_count": result.get("row_count"),
            "columns": result.get("columns")
        },
        "blocks": summarize_blocks(final_answer.get("blocks"))
    }


def main():
    for index, question in enumerate(QUESTIONS, start=1):
        print(f"\n=== QUESTION {index} ===")
        print(question)

        state = graph.invoke({"user_question": question})

        print("\nSQL:")
        print(state.get("generated_sql") or "")

        print_json("RESULT PREVIEW", preview_rows(state.get("query_result")))

        print_json("CHART SPEC", state.get("chart_spec"))

        print("\nINSIGHT SUMMARY:")
        print(state.get("insight_summary") or "")

        print_json(
            "FINAL ANSWER SUMMARY",
            summarize_final_answer(state.get("final_answer"))
        )

        if state.get("error"):
            print("\nERROR:")
            print(state["error"])


if __name__ == "__main__":
    main()
