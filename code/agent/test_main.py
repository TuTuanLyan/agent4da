from graph.sql_graph import graph


question = """
Brand nao da duoc nguoi dung them vao gio hang nhieu nhat? Ke ten 4 brand
"""

result = graph.invoke({
    "user_question": question
})

print("\nGenerated SQL by LLM:")
print(result["generated_sql"])

print("\nQuery Result:")
print(result["query_result"])