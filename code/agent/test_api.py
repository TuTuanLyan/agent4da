import os

from dotenv import load_dotenv
from groq import Groq

load_dotenv()

api_key = os.getenv("GROQ_API_KEY")
if not api_key:
    raise RuntimeError("GROQ_API_KEY is not set")

client = Groq(api_key=api_key)

response = client.chat.completions.create(
    model=os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile"),
    messages=[
        {"role": "user", "content": "Hãy viết một đoạn code Python đơn giản để in ra 'Hello, World!'"}
    ],
)

print(response.choices[0].message.content)
