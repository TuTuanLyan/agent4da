import os
from openai import OpenAI


def smoke_test(name, api_key, base_url, model):
    if not api_key:
        print(f"{name}: missing API key")
        return

    client = OpenAI(api_key=api_key, base_url=base_url, timeout=20, max_retries=0)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "user", "content": "Reply with exactly: ok"}
        ],
        temperature=0,
    )
    print(f"{name}: {response.choices[0].message.content}")


smoke_test(
    "gemini",
    (os.getenv("GEMINI_API_KEYS", "").split(",")[0] or os.getenv("GEMINI_API_KEY", "")).strip(),
    os.getenv("GEMINI_BASE_URL", "https://generativelanguage.googleapis.com/v1beta/openai/"),
    os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
)
smoke_test(
    "groq",
    os.getenv("GROQ_API_KEY", "").strip(),
    os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1"),
    os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile"),
)
