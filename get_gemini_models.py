from google import genai
from dotenv import load_dotenv

load_dotenv()  # Must be called before helios imports that read env vars

from helios.config import GEMINI

client = genai.Client(api_key=GEMINI.api_key)

models = client.models.list()
for model in models:
    # if "gemini-2.5" not in model.name and "gemini-3" not in model.name:
    #     continue

    if "deep" not in model.name:
        continue


    print(
        f"Model: {model.name}, Version: {model.version}, Description: {model.description}, "
        f"input_token_limit: {model.input_token_limit}, output_token_limit: {model.output_token_limit}\n"
    )
