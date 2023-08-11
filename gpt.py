import openai
import os


openai.api_key = API_KEY

completion = openai.ChatCompletion.create(
    model="gpt-3.5-turbo",
    messages=[
        {"role": "user", "content": "can you show login by spring boot"}
    ]
)

print(completion.choices[0].message.content)
