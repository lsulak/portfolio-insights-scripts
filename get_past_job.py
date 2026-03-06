from google import genai
from dotenv import load_dotenv

load_dotenv()  # Must be called before helios imports that read env vars

from helios.config import GEMINI

INTERACTION_ID = "v1_ChdXQ0dxYVozWkNvYjZuc0VQNVlTUTBBSRIXV0NHcWFaM1pDb2I2bnNFUDVZU1EwQUk"

client = genai.Client(api_key=GEMINI.api_key)

previous_interaction = client.interactions.get(id=INTERACTION_ID)
print(previous_interaction)

# previous_interaction = client.interactions.delete(INTERACTION_ID)
cancelled = client.interactions.cancel(id=INTERACTION_ID)

print(cancelled)
print("OK, deleted previous interaction")
