from langgraph.graph import StateGraph, START, END

from state.agent_state import AgentState

from nodes.guard_question_node import guard_question_node
from nodes.load_metadata_node import load_metadata_node
from nodes.check_answerability_node import check_answerability_node
from nodes.build_prompt_node import build_prompt_node
from nodes.text2sql_node import generate_sql_node
from nodes.guard_sql_node import guard_sql_node
from nodes.execute_sql_node import execute_sql_node
from nodes.profile_result_node import profile_result_node
from nodes.validate_result_node import validate_result_node
from nodes.plan_chart_node import plan_chart_node
from nodes.generate_insight_node import generate_insight_node
from nodes.build_final_response_node import build_final_response_node


def route_after_question_guard(state):
    if state.get("error"):
        return "build_final_response"
    return "load_metadata"


def route_after_sql_guard(state):
    if state.get("error"):
        return "build_final_response"
    return "execute_sql"


def route_after_execute(state):
    if not state.get("error"):
        return "profile_result"

    retry_count = int(state.get("retry_count") or 0)
    max_retries = int(state.get("max_retries") or 3)
    if retry_count < max_retries:
        return "build_prompt"

    return "generate_insight"


def route_after_answerability(state):
    if state.get("stop_after_answerability"):
        return "build_final_response"
    return "build_prompt"


def route_after_result_validation(state):
    if state.get("requery_requested"):
        return "build_prompt"
    return "plan_chart"


builder = StateGraph(AgentState)

builder.add_node("guard_question", guard_question_node)

builder.add_node("load_metadata", load_metadata_node)

builder.add_node("check_answerability", check_answerability_node)

builder.add_node("build_prompt", build_prompt_node)

builder.add_node("generate_sql", generate_sql_node)

builder.add_node("guard_sql", guard_sql_node)

builder.add_node("execute_sql", execute_sql_node)

builder.add_node("profile_result", profile_result_node)

builder.add_node("validate_result", validate_result_node)

builder.add_node("plan_chart", plan_chart_node)

builder.add_node("generate_insight", generate_insight_node)

builder.add_node("build_final_response", build_final_response_node)


builder.add_edge(START, "guard_question")

builder.add_conditional_edges(
    "guard_question",
    route_after_question_guard,
    {
        "load_metadata": "load_metadata",
        "build_final_response": "build_final_response",
    },
)

builder.add_edge("load_metadata", "check_answerability")

builder.add_conditional_edges(
    "check_answerability",
    route_after_answerability,
    {
        "build_prompt": "build_prompt",
        "build_final_response": "build_final_response",
    },
)

builder.add_edge("build_prompt", "generate_sql")
builder.add_edge("generate_sql", "guard_sql")

builder.add_conditional_edges(
    "guard_sql",
    route_after_sql_guard,
    {
        "execute_sql": "execute_sql",
        "build_final_response": "build_final_response",
    },
)

builder.add_conditional_edges(
    "execute_sql",
    route_after_execute,
    {
        "build_prompt": "build_prompt",
        "profile_result": "profile_result",
        "generate_insight": "generate_insight",
    },
)

builder.add_edge("profile_result", "validate_result")

builder.add_conditional_edges(
    "validate_result",
    route_after_result_validation,
    {
        "build_prompt": "build_prompt",
        "plan_chart": "plan_chart",
    },
)

builder.add_edge("plan_chart", "generate_insight")

builder.add_edge("generate_insight", "build_final_response")

builder.add_edge("build_final_response", END)


graph = builder.compile()
