from google import genai
from dotenv import load_dotenv
from LLM import client, VideoDatabaseAnalyzer
from handlers import bot
import asyncio

va = VideoDatabaseAnalyzer()

if __name__ == "__main__":
    asyncio.run(bot.run())


