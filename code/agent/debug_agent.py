import argparse

from graph.sql_graph import graph


DEFAULT_QUESTION = "Doanh thu theo ngay trong thang 1 nam 2020"


def print_section(title, value=None):
    print(f"\n=== {title} ===")
    if value is not None:
        print(value)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run the semantic metadata SQL agent.")
    parser.add_argument("question", nargs="*", help="Question to ask the agent.")
    parser.add_argument(
        "--show-context",
        action="store_true",
        help="Print the loaded schema context.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    question = " ".join(args.question).strip() or DEFAULT_QUESTION

    state = graph.invoke({"user_question": question})

    print_section("QUESTION", question)
    if args.show_context:
        print_section("SCHEMA CONTEXT", state.get("schema_context", ""))
    print_section("GENERATED SQL", state.get("generated_sql", ""))

    if state.get("error"):
        print_section("ERROR", state["error"])
        return 1

    print_section("QUERY RESULT", state.get("query_result", []))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
