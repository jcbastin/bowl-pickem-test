import os
from dotenv import load_dotenv

load_dotenv()
print("Key loaded:", os.getenv("CFBD_API_KEY"))

