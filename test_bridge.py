import os
import ollama


MODEL_NAME = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:32b").strip()

# We add a 'system' message to give the AI its context
response = ollama.chat(model=MODEL_NAME, messages=[
  {
    'role': 'system',
    'content': 'You are a technical assistant for a Product Owner in Bengaluru. The user owns a high-end 2026 PC with an RTX 5070 Ti 16GB and 64GB RAM. Do not argue about the hardware specs.'
  },
  {
    'role': 'user',
    'content': 'Confirm you understand the hardware specs and suggest a short Python snippet to check VRAM usage.',
  },
])
print(response['message']['content'])