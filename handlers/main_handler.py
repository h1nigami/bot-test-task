import os

import logging


from dotenv import load_dotenv

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

from LLM import VideoDatabaseAnalyzer
from aiogram.client.default import DefaultBotProperties

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)





class TelegramBot:
    """Класс Telegram бота"""
    
    def __init__(self, token: str, analyzer: VideoDatabaseAnalyzer):
        """
        Инициализация бота
        
        Args:
            token: Токен Telegram бота от @BotFather
            analyzer: Экземпляр VideoDatabaseAnalyzer
        """
        self.token = token
        self.analyzer = analyzer
        
        self.bot = Bot(token=token,
                       default=DefaultBotProperties(
                           parse_mode="HTML"
                       ))
        self.dp = Dispatcher()
        
        # Регистрация обработчиков
        self._register_handlers()
    
    def _register_handlers(self):
        """Регистрация обработчиков команд и сообщений"""
        
        @self.dp.message(Command("start"))
        async def start_command(message: Message):
            """Обработчик команды /start"""
            welcome_text = """
            👋 <b>Привет! Я бот-анализатор статистики видео.</b>
            
            Я могу отвечать на вопросы о статистике видео на естественном языке:
            
            📊 <b>Примеры запросов:</b>
            • Сколько всего просмотров у всех видео?
            • Какое среднее количество лайков?
            • Сколько видео создано в августе 2025?
            • Сколько лайков у видео [ID видео]?
            • Какой общий прирост просмотров?
            • Кто загрузил больше всего видео?
            
            Просто задайте вопрос, и я найду ответ в базе данных!
            """
            await message.answer(welcome_text)
        
        @self.dp.message(Command("help"))
        async def help_command(message: Message):
            """Обработчик команды /help"""
            help_text = """
            ℹ️ <b>Помощь по использованию бота:</b>
            
            <b>Формат запросов:</b>
            Задавайте вопросы на русском языке о статистике видео:
            - Количества (сколько, сколько всего)
            - Суммы (общая сумма, всего)
            - Средние значения (среднее, в среднем)
            - Приросты (прирост, изменение)
            - Максимумы/минимумы (максимальный, минимальный)
            
            <b>Примеры:</b>
            • "сколько всего просмотров?"
            • "какое среднее количество комментариев?"
            • "сколько лайков у видео abc123?"
            • "какой прирост просмотров за последний месяц?"
            
            <b>Доступные команды:</b>
            /start - начать работу
            /help - эта справка
            /stats - статистика базы данных
            """
            await message.answer(help_text)
        
        @self.dp.message(Command("stats"))
        async def stats_command(message: Message):
            """Показать статистику базы данных"""
            stats = self.analyzer.db_schema.get("statistics", {})
            
            stats_text = "<b>📈 Статистика базы данных:</b>\n\n"
            
            for table_name, table_stats in stats.items():
                stats_text += f"<b>{table_name}:</b>\n"
                stats_text += f"  • Записей: {table_stats.get('row_count', 0)}\n"
                
                
                
                stats_text += "\n"
            
            await message.answer(stats_text)
        
        @self.dp.message()
        async def handle_text_message(message: Message):
            """Обработчик текстовых сообщений"""
            user_question = message.text.strip()
            
            # Показываем статус "печатает..."
            await message.chat.do('typing')
            
            try:
                
                # Получаем ответ от анализатора
                result = await self.analyzer.generate_sql_and_answer(user_question=user_question)
                
                if result["success"]:
                    # Формируем финальный ответ
                    response_text = f"""{result['final_answer'].replace(" ", "")}"""
                    
                    # Редактируем исходное сообщение с результатом
                    await message.answer(response_text)
                    
                    # Логируем успешный запрос
                    logger.info(f"Успешный запрос: {user_question}")
                    logger.info(f"SQL: {result['sql_query']}")
                    logger.info(f"Ответ: {result['final_answer']}")
                    
                else:
                    error_text = f"""
❌ <b>Не удалось обработать запрос</b>

Причина: {result.get('error', 'Неизвестная ошибка')}

Попробуйте:
1. Переформулировать вопрос
2. Использовать более простые формулировки
3. Проверить примеры в /help
                    """
                    await message.answer(error_text)
                    
            except Exception as e:
                logger.error(f"Ошибка в обработке сообщения: {e}")
                await message.answer("⚠️ Произошла ошибка при обработке запроса.")
    
    async def run(self):
        """Запуск бота"""
        logger.info("Бот запускается...")
        await self.dp.start_polling(self.bot)


analyzer = VideoDatabaseAnalyzer()

bot = TelegramBot(token=os.getenv("BOT_TOKEN"), analyzer=analyzer)