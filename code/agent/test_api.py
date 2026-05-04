from openai import OpenAI

client = OpenAI(
    api_key="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",
    base_url="https://api.groq.com/openai/v1"
)

response = client.chat.completions.create(
    model="llama-3.3-70b-versatile",
    messages=[
        {"role": "user", "content": "Hãy viết một đoạn code Python đơn giản để in ra 'Hello, World!'"}
    ]
)

print(response.choices[0].message.content)