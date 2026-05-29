from typing import Any

from nlu_parser import parse_nlu


def route_intent(question: str) -> dict[str, Any]:
    return parse_nlu(question)
