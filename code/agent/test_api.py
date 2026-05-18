import os
from openai import OpenAI

api_key = os.getenv("GROQ_API_KEY")

client = OpenAI(
    api_key=api_key,
    base_url="https://api.groq.com/openai/v1"
)

response = client.chat.completions.create(
    model="llama-3.3-70b-versatile",
    messages=[
        {"role": "user", "content": "Hãy viết một đoạn code Python đơn giản để in ra 'Hello, World!'"}
    ]
)

print(response.choices[0].message.content)