import asyncio
import logging
import os
import sqlite3
from typing import Any, Dict
from dotenv import load_dotenv

load_dotenv()
print("HTTP_PROXY =", os.environ.get("HTTP_PROXY"))
print("HTTPS_PROXY =", os.environ.get("HTTPS_PROXY"))

from google import genai

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



client = genai.Client(
    location="us-central1",
)


class VideoDatabaseAnalyzer:
    """Класс для анализа базы данных видео через естественный язык"""
    
    def __init__(self, db_path: str = "database/videos.db"):
        """
        Инициализация анализатора базы данных.
        
        Args:
            db_path: Путь к файлу базы данных SQLite
        """
        self.db_path = db_path
        self.db_schema = self._get_database_schema()
        
        # Инициализация Gemini клиента
        self.llm_client = client
        
        # Промпт-инженер для LLM
        self.system_prompt = self._create_system_prompt()
        self.lock = asyncio.Lock()
    
    def _get_database_schema(self) -> Dict[str, Any]:
        """Получение полной схемы базы данных"""
        schema = {
            "tables": {},
            "relationships": [],
            "statistics": {}
        }
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Получаем список таблиц
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
            tables = cursor.fetchall()
            
            for table_info in tables:
                table_name = table_info[0]
                schema["tables"][table_name] = {
                    "columns": [],
                    "description": self._get_table_description(table_name)
                }
                
                # Получаем информацию о колонках
                cursor.execute(f"PRAGMA table_info({table_name});")
                columns = cursor.fetchall()
                
                for col in columns:
                    col_id, col_name, col_type, notnull, default_val, pk = col
                    schema["tables"][table_name]["columns"].append({
                        "name": col_name,
                        "type": col_type,
                        "primary_key": bool(pk),
                        "nullable": not bool(notnull),
                        "description": self._get_column_description(table_name, col_name)
                    })
            
            # Получаем статистику по таблицам для контекста
            for table_name in schema["tables"].keys():
                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                count = cursor.fetchone()[0]
                schema["statistics"][table_name] = {
                    "row_count": count,
                    "sample_data": self._get_sample_data(table_name, cursor, limit=3)
                }
            
            conn.close()
            return schema
            
        except Exception as e:
            logger.error(f"Ошибка при получении схемы БД: {e}")
            return schema
    
    def _get_table_description(self, table_name: str) -> str:
        """Описание таблиц для LLM"""
        descriptions = {
            "videos": """
                Основная таблица с видео. Содержит информацию о загруженных видео, их статистику и метаданные.
                Ключевые поля: 
                - video_id: уникальный идентификатор видео
                - views_count: количество просмотров видео
                - likes_count: количество лайков
                - reports_count: количество жалоб
                - comments_count: количество комментариев
                - creator_id: идентификатор создателя
                - video_created_at: когда видео было создано на платформе
                - created_at: когда запись была добавлена в нашу базу
                - updated_at: когда запись последний раз обновлялась
            """,
            "snapshots": """
                Таблица снапшотов (снимков статистики) видео. Каждый снапшот фиксирует состояние статистики видео 
                в определённый момент времени. Связь с videos через video_id.
                Ключевые поля:
                - delta_views_count: прирост просмотров с предыдущего снапшота
                - delta_likes_count: прирост лайков
                - delta_comments_count: прирост комментариев
                - delta_reports_count: прирост жалоб
                - created_at: когда снапшот был сделан
            """
        }
        return descriptions.get(table_name, "Таблица без описания")
    
    def _get_column_description(self, table_name: str, column_name: str) -> str:
        """Описание колонок для LLM"""
        descriptions = {
            ("videos", "video_id"): "Уникальный идентификатор видео (UUID)",
            ("videos", "views_count"): "Общее количество просмотров видео (целое число)",
            ("videos", "likes_count"): "Общее количество лайков видео (целое число)",
            ("videos", "comments_count"): "Общее количество комментариев под видео (целое число)",
            ("videos", "reports_count"): "Количество жалоб на видео (целое число)",
            ("videos", "creator_id"): "Идентификатор создателя видео",
            ("videos", "video_created_at"): "Дата и время создания видео на платформе (DATETIME)",
            ("snapshots", "delta_views_count"): "Прирост просмотров между снапшотами (может быть положительным или отрицательным)",
            ("snapshots", "delta_likes_count"): "Прирост лайков между снапшотами",
            ("snapshots", "delta_comments_count"): "Прирост комментариев между снапшотами",
            ("snapshots", "delta_reports_count"): "Прирост жалоб между снапшотами",
            ("snapshots", "created_at"): "Дата и время создания снапшота (DATETIME)"
        }
        return descriptions.get((table_name, column_name), "Колонка без описания")
    
    def _get_sample_data(self, table_name: str, cursor, limit: int = 3) -> list:
        """Получение примеров данных из таблицы для контекста"""
        try:
            cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit};")
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        except:
            return []
    
    def _create_system_prompt(self) -> str:
        """Создание промпта для LLM с описанием схемы БД"""
        
        # Формируем описание схемы
        schema_description = "СХЕМА БАЗЫ ДАННЫХ:\n\n"
        
        for table_name, table_info in self.db_schema["tables"].items():
            schema_description += f"ТАБЛИЦА: {table_name}\n"
            schema_description += f"Описание: {table_info['description']}\n"
            schema_description += "Колонки:\n"
            
            for col in table_info["columns"]:
                schema_description += f"  - {col['name']} ({col['type']})"
                if col['primary_key']:
                    schema_description += " [PRIMARY KEY]"
                if col['description']:
                    schema_description += f": {col['description']}"
                schema_description += "\n"
            
            schema_description += "\n"
        
        # Примеры запросов и ответов (few-shot learning)
        examples = """
        ПРИМЕРЫ ЗАПРОСОВ И ОТВЕТОВ:
        
        Вопрос: "сколько всего просмотров у всех видео?"
        SQL: SELECT SUM(views_count) as total_views FROM videos;
        Ответ: 150000
        
        Вопрос: "какое среднее количество лайков на видео?"
        SQL: SELECT AVG(likes_count) as avg_likes FROM videos;
        Ответ: 125
        
        Вопрос: "сколько видео было создано в августе 2025?"
        SQL: SELECT COUNT(*) as video_count FROM videos WHERE strftime('%Y-%m', video_created_at) = '2025-08';
        Ответ: 42
        
        Вопрос: "какой общий прирост просмотров по всем снапшотам?"
        SQL: SELECT SUM(delta_views_count) as total_delta_views FROM snapshots;
        Ответ: 50000
        
        Вопрос: "сколько лайков у видео с id ecd8a4e4-1f24-4b97-a944-35d17078ce7c?"
        SQL: SELECT likes_count FROM videos WHERE video_id = 'ecd8a4e4-1f24-4b97-a944-35d17078ce7c';
        Ответ: 35
        
        Вопрос: "какой создатель загрузил больше всего видео?"
        SQL: SELECT creator_id, COUNT(*) as video_count FROM videos GROUP BY creator_id ORDER BY video_count DESC LIMIT 1;
        Ответ: creator123 (15 видео)

         Вопрос: "Суммарный прирост просмотров создателя X за 28 ноября 2025 с 10:00 до 15:00"
        SQL: SELECT COALESCE(SUM(s.delta_views_count), 0) FROM videos v JOIN snapshots s ON v.video_id = s.video_id WHERE v.creator_id = 'X' AND DATE(s.created_at) = '2025-11-28' AND TIME(s.created_at) BETWEEN '10:00:00' AND '15:00:00';
        Ответ: 1200
    
        Вопрос: "На сколько просмотров выросли видео автора Y в период времени с A до B даты Z"
        SQL: SELECT SUM(s.delta_views_count) FROM videos v INNER JOIN snapshots s ON v.video_id = s.video_id WHERE v.creator_id = 'Y' AND strftime('%Y-%m-%d', s.created_at) = 'Z' AND strftime('%H:%M', s.created_at) BETWEEN 'A' AND 'B';
        Ответ: 850
    
        Вопрос: "Прирост просмотров по всем видео креатора за конкретный день и время"
        SQL: SELECT COALESCE(SUM(delta_views_count), 0) FROM snapshots WHERE video_id IN (SELECT video_id FROM videos WHERE creator_id = 'ID_КРЕАТОРА') AND created_at >= '2025-11-28T10:00:00' AND created_at <= '2025-11-28T15:00:00';
        Ответ: 950
        """
        
        prompt = f"""
        Ты — экспертный SQL-анализатор. Твоя задача:
        1. Понимать вопросы на естественном русском языке о статистике видео
        2. Генерировать ТОЧНЫЕ SQL-запроcы к базе данных SQLite
        3. Возвращать ОДНО ЧИСЛО в ответе (счётчик, сумму, среднее, максимум, минимум)
        
        {schema_description}
        
        {examples}
        
        ВАЖНЫЕ ПРАВИЛА:
        1. Всегда используй таблицы и колонки ТОЛЬКО из описанной схемы
        2. Для дат используй функции: strftime('%Y-%m', column) для месяца, date(column) для даты
        3. Для агрегации используй: SUM(), COUNT(), AVG(), MAX(), MIN()
        4. Всегда возвращай ОДНО ЧИСЛО в результате
        5. Если запрос требует текстового ответа, преобразуй его в число или формат "число (пояснение)"
        6. Форматируй большие числа: используй 150000 вместо 150,000
        7. Для фильтрации по дате и времени используй ИЛИ DATE() + TIME(), ИЛИ strftime()
        8. Для временного интервала используй BETWEEN, а не несколько условий OR
        9. Всегда заключай сложные условия в скобки: WHERE (условие1) AND (условие2 OR условие3)
        10. Для точных временных интервалов используй: created_at >= 'YYYY-MM-DDTHH:MM:SS' AND created_at <= 'YYYY-MM-DDTHH:MM:SS'
        11. Пример правильного интервала: TIME(created_at) BETWEEN '10:00:00' AND '15:00:00'
        12. Пример НЕПРАВИЛЬНОГО интервала: strftime('%H', created_at) = '10' OR strftime('%H', created_at) = '11' ...
        
        ОБРАЗЕЦ ДЛЯ ВРЕМЕННЫХ ЗАПРОСОВ:
        Вопрос: "суммарный прирост за период с X до Y"
        Правильный SQL: SELECT SUM(delta_views_count) FROM таблица WHERE условие_пользователя AND created_at >= 'начальная_дата' AND created_at <= 'конечная_дата'

        СТРУКТУРА ОТВЕТА:
        1. Сначала SQL-запрос в одну строку
        2. Затем пустая строка
        3. Затем числовой ответ
        
        ВОПРОС ПОЛЬЗОВАТЕЛЯ: {{user_question}}
        """
        
        return prompt
    
    async def generate_sql_and_answer(self, user_question: str) -> Dict[str, Any]:
        """
        Генерация SQL запроса и получение ответа из БД
        
        Returns:
            Словарь с SQL запросом, ответом и флагом успеха
        """
        async with self.lock:
            try:
                # Подставляем вопрос пользователя в промпт
                full_prompt = self.system_prompt.replace("{user_question}", user_question)
                
                # Отправляем запрос к Gemini
                response = self.llm_client.models.generate_content(
                    model="gemma-3-27b-it",  # или другая модель
                    contents=full_prompt
                )
                
                response_text = response.text.strip()
                
                # Парсим ответ: SQL и результат разделены пустой строкой
                parts = response_text.split('\n\n')
                
                if len(parts) >= 2:
                    sql_query = parts[0].strip()
                    llm_answer = parts[1].strip()
                    
                    # Валидируем SQL запрос (базовая проверка)
                    sql_query_lower = sql_query.lower()
                    if not (sql_query_lower.startswith('select ') and 
                            'from ' in sql_query_lower):
                        raise ValueError("Сгенерирован не SELECT запрос")
                    
                    # Выполняем SQL запрос
                    actual_result = self._execute_sql_query(sql_query)
                    
                    return {
                        "success": True,
                        "sql_query": sql_query,
                        "llm_suggested_answer": llm_answer,
                        "actual_result": actual_result,
                        "final_answer": self._format_answer(actual_result)
                    }
                else:
                    raise ValueError("Неверный формат ответа от LLM")
                    
            except Exception as e:
                logger.error(f"Ошибка при обработке запроса '{user_question}': {e}")
                return {
                    "success": False,
                    "error": str(e),
                    "sql_query": None,
                    "final_answer": "Не удалось обработать запрос. Попробуйте сформулировать иначе."
                }
    
    def _execute_sql_query(self, sql_query: str) -> Any:
        """Безопасное выполнение SQL запроса"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Выполняем запрос
            cursor.execute(sql_query)
            result = cursor.fetchone()
            
            conn.close()
            
            # Извлекаем первое значение из результата
            if result:
                return result[0]
            return None
            
        except sqlite3.Error as e:
            logger.error(f"Ошибка SQL: {e}, Запрос: {sql_query}")
            raise
    
    def _format_answer(self, result: Any) -> str:
        """Форматирование ответа для пользователя"""
        if result is None:
            return "Данные не найдены"
        
        # Преобразуем в число если возможно
        try:
            if isinstance(result, (int, float)):
                # Форматируем большие числа
                if result >= 1000:
                    return f"{result:,}".replace(",", " ")
                return str(result)
            else:
                return str(result)
        except:
            return str(result)