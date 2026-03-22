import subprocess
import requests

# ask the user a question
question = input("Ask something: ")

# run recoll search
recoll_cmd = [r"D:\Recoll\recollq.exe", "-F", "url abstract", "-n", "5", question]

result = subprocess.run(recoll_cmd, capture_output=True, text=True)

search_results = result.stdout

print("\n--- Recoll results ---\n")
print(search_results)

# build prompt for the AI
prompt = f"""
User question:
{question}

Relevant search results from my files:

{search_results}

Based on these results, answer the question.
"""

# send to LM Studio local API
response = requests.post(
    "http://localhost:1234/v1/chat/completions",
    json={
        "model": "mistral-7b-instruct-v0.3.Q4_K_M.gguf",
        # "model": "llm/mistralai/mistral-7b-instruct-v0.3",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
    },
)

print("\n--- AI Answer ---\n")
data = response.json()

if "choices" in data:
    print("\n--- AI Answer ---\n")
    print(data["choices"][0]["message"]["content"])
else:
    print("\n--- ERROR ---\n")
    print(data)
