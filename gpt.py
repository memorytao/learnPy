import openai
import os

API_KEY = "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"
ORGANIZE_KEY = "org-78PZ5wZ6HwjTE2qsdC1MpT4E"

openai.api_key = API_KEY

completion = openai.ChatCompletion.create(
    model="gpt-3.5-turbo",
    messages=[
        {"role": "user", "content": "can you show login by spring boot"}
    ]
)

print(completion.choices[0].message.content)
