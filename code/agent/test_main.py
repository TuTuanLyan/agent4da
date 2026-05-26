from graph.sql_graph import graph


QUESTIONS = [
    "Nhan hang nao duoc xem nhieu nhat trong thang 1 nam 2020",
    "Brand nao duoc them vao gio hang nhieu nhat? Ke ten 4 brand",
    "Danh muc nao co doanh thu cao nhat?",
    "Doanh thu theo ngay trong thang 1 nam 2020",
]


def preview_rows(rows, limit=5):
    rows = rows or []
    return rows[:limit]


def main():
    for index, question in enumerate(QUESTIONS, start=1):
        print(f"\n=== QUESTION {index} ===")
        print(question)

        state = graph.invoke({"user_question": question})

        print("\nSQL:")
        print(state.get("generated_sql") or "")

        if state.get("error"):
            print("\nERROR:")
            print(state["error"])
            continue

        print("\nRESULT PREVIEW:")
        print(preview_rows(state.get("query_result")))


if __name__ == "__main__":
    main()
