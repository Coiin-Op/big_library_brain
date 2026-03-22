import requests

response = requests.post(
    "http://localhost:1234/v1/chat/completions",
    json={
        "model": "qwen2.5-coder-3b-instruct",
        "messages": [{"role": "user", "content": "say hello"}],
        "temperature": 0.1,
    },
)

result = response.json()

print("\n--- AI Answer ---\n")
print(result["choices"][0]["message"]["content"])
