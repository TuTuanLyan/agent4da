import os
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

try:  # observability is optional and must never break LLM calls
    from services.obs_metrics import observe_llm
except Exception:  # pragma: no cover
    from contextlib import contextmanager

    @contextmanager
    def observe_llm(kind):
        yield


_CLIENTS = {}
_PROVIDER_OVERRIDE = ContextVar("llm_provider_override", default=None)
_MODEL_OVERRIDE = ContextVar("llm_model_override", default=None)

DEFAULT_GROQ_MODEL = "llama-3.3-70b-versatile"
DEFAULT_GEMINI_MODEL = "gemini-2.5-flash"
GROQ_BASE_URL = "https://api.groq.com/openai/v1"
GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    api_key: str
    base_url: str
    model: str
    timeout: float
    max_retries: int


def _split_env(name):
    return [item.strip() for item in os.getenv(name, "").split(",") if item.strip()]


def _gemini_keys():
    keys = _split_env("GEMINI_API_KEYS")
    single_key = os.getenv("GEMINI_API_KEY", "").strip()
    if single_key:
        keys.insert(0, single_key)
    return list(dict.fromkeys(keys))


def _provider_order():
    provider = (_PROVIDER_OVERRIDE.get() or os.getenv("AGENT_LLM_PROVIDER", "auto")).strip().lower()
    if provider == "gemini":
        return ["gemini"]
    if provider == "groq":
        return ["groq"]
    return ["gemini", "groq"]


def _configs_for_provider(provider):
    model_override = _MODEL_OVERRIDE.get()
    if provider == "gemini":
        return [
            ProviderConfig(
                name="gemini",
                api_key=api_key,
                base_url=os.getenv("GEMINI_BASE_URL", GEMINI_BASE_URL),
                model=model_override or os.getenv("GEMINI_MODEL", DEFAULT_GEMINI_MODEL),
                timeout=float(os.getenv("GEMINI_TIMEOUT_SECONDS", "45")),
                max_retries=int(os.getenv("GEMINI_MAX_RETRIES", "0")),
            )
            for api_key in _gemini_keys()
        ]

    if provider == "groq":
        api_key = os.getenv("GROQ_API_KEY", "").strip()
        if not api_key:
            return []
        return [
            ProviderConfig(
                name="groq",
                api_key=api_key,
                base_url=os.getenv("GROQ_BASE_URL", GROQ_BASE_URL),
                model=model_override or os.getenv("GROQ_MODEL", DEFAULT_GROQ_MODEL),
                timeout=float(os.getenv("GROQ_TIMEOUT_SECONDS", "45")),
                max_retries=int(os.getenv("GROQ_MAX_RETRIES", "1")),
            )
        ]

    return []


def _configured_providers():
    configs = []
    for provider in _provider_order():
        configs.extend(_configs_for_provider(provider))
    if not configs:
        raise ValueError(
            "Missing LLM credentials. Configure GEMINI_API_KEYS/GEMINI_API_KEY or GROQ_API_KEY."
        )
    return configs


@contextmanager
def llm_runtime(provider=None, model=None):
    provider_token = _PROVIDER_OVERRIDE.set(provider)
    model_token = _MODEL_OVERRIDE.set(model)
    try:
        yield
    finally:
        _MODEL_OVERRIDE.reset(model_token)
        _PROVIDER_OVERRIDE.reset(provider_token)


def _get_llm_client(config):
    cache_key = (config.name, config.api_key, config.base_url)
    if cache_key not in _CLIENTS:

        from openai import OpenAI

        _CLIENTS[cache_key] = OpenAI(
            api_key=config.api_key,
            base_url=config.base_url,
            timeout=config.timeout,
            max_retries=config.max_retries,
        )

    return _CLIENTS[cache_key]


def _generate(prompt, *, kind, temperature):
    last_error = None

    for config in _configured_providers():
        try:
            with observe_llm(kind):
                response = _get_llm_client(config).chat.completions.create(
                    model=config.model,
                    messages=[
                        {
                            "role": "user",
                            "content": prompt,
                        }
                    ],
                    temperature=temperature,
                )
            return response.choices[0].message.content
        except Exception as exc:  # noqa: BLE001
            last_error = exc

    raise RuntimeError(f"All configured LLM providers failed: {last_error}") from last_error


def generate_sql(prompt):
    return _generate(prompt, kind="sql", temperature=0)


def generate_text(prompt):
    return _generate(prompt, kind="text", temperature=0.2)
