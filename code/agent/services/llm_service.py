import os

try:  # observability is optional and must never break LLM calls
    from services.obs_metrics import observe_llm
except Exception:  # pragma: no cover
    from contextlib import contextmanager

    @contextmanager
    def observe_llm(kind):
        yield


_CLIENT = None

DEFAULT_MODEL = "llama-3.3-70b-versatile"


def get_llm_client():
    global _CLIENT

    if _CLIENT is None:
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            raise ValueError("Missing GROQ_API_KEY environment variable.")

        from openai import OpenAI

        _CLIENT = OpenAI(
            api_key=api_key,
            base_url=os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1"),
            timeout=float(os.getenv("GROQ_TIMEOUT_SECONDS", "45")),
            max_retries=int(os.getenv("GROQ_MAX_RETRIES", "1")),
        )

    return _CLIENT


def generate_sql(prompt):
    model = os.getenv("GROQ_MODEL", DEFAULT_MODEL)

    with observe_llm("sql"):
        response = get_llm_client().chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0
        )

    return response.choices[0].message.content


def generate_text(prompt):
    model = os.getenv("GROQ_MODEL", DEFAULT_MODEL)

    with observe_llm("text"):
        response = get_llm_client().chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.2
        )

    return response.choices[0].message.content
