from langgraph.graph import StateGraph, START, END

from state.agent_state import AgentState

from nodes.load_metadata_node import load_metadata_node
from nodes.build_prompt_node import build_prompt_node
from nodes.text2sql_node import generate_sql_node
from nodes.guard_sql_node import guard_sql_node
from nodes.execute_sql_node import execute_sql_node


builder = StateGraph(AgentState)

builder.add_node("load_metadata", load_metadata_node)

builder.add_node("build_prompt", build_prompt_node)

builder.add_node("generate_sql", generate_sql_node)

builder.add_node("guard_sql", guard_sql_node)

builder.add_node("execute_sql", execute_sql_node)


builder.add_edge(START, "load_metadata")

builder.add_edge("load_metadata", "build_prompt")

builder.add_edge("build_prompt", "generate_sql")

builder.add_edge("generate_sql", "guard_sql")

builder.add_edge("guard_sql", "execute_sql")

builder.add_edge("execute_sql", END)


graph = builder.compile()