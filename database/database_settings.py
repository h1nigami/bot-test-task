import json
import sqlite3
from datetime import datetime
from typing import Dict, List, Any, Optional


class DataBaseManager:
    def __init__(self, db_name: str):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.create_tables()

    def create_tables(self) -> bool:
        """Создание таблиц для видео и снапшотов"""
        try:
            # Таблица для видео
            videos_query = """
            CREATE TABLE IF NOT EXISTS videos (
                video_id TEXT PRIMARY KEY,
                video_created_at DATETIME,
                views_count INTEGER DEFAULT 0,
                likes_count INTEGER DEFAULT 0,
                reports_count INTEGER DEFAULT 0,
                comments_count INTEGER DEFAULT 0,
                creator_id TEXT,
                created_at DATETIME,
                updated_at DATETIME
            )
            """
            self.cursor.execute(videos_query)

            # Таблица для снапшотов
            snapshots_query = """
            CREATE TABLE IF NOT EXISTS snapshots (
                snapshot_id TEXT PRIMARY KEY,
                video_id TEXT,
                views_count INTEGER DEFAULT 0,
                likes_count INTEGER DEFAULT 0,
                reports_count INTEGER DEFAULT 0,
                comments_count INTEGER DEFAULT 0,
                delta_views_count INTEGER DEFAULT 0,
                delta_likes_count INTEGER DEFAULT 0,
                delta_reports_count INTEGER DEFAULT 0,
                delta_comments_count INTEGER DEFAULT 0,
                created_at DATETIME,
                updated_at DATETIME,
                FOREIGN KEY (video_id) REFERENCES videos (video_id)
            )
            """
            self.cursor.execute(snapshots_query)
            
            # Создаем индексы для ускорения запросов
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_videos_creator ON videos(creator_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_videos_created ON videos(video_created_at)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_video ON snapshots(video_id)")
            
            self.conn.commit()
            print("Таблицы успешно созданы")
            return True
            
        except Exception as e:
            print(f"Ошибка при создании таблиц: {e}")
            return False

    def insert_video(self, video_data: Dict[str, Any]) -> bool:
        """Вставка данных видео в таблицу videos"""
        try:
            query = """
            INSERT OR REPLACE INTO videos 
            (video_id, video_created_at, views_count, likes_count, 
             reports_count, comments_count, creator_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            self.cursor.execute(query, (
                video_data.get('id'),
                video_data.get('video_created_at'),
                video_data.get('views_count', 0),
                video_data.get('likes_count', 0),
                video_data.get('reports_count', 0),
                video_data.get('comments_count', 0),
                video_data.get('creator_id'),
                video_data.get('created_at'),
                video_data.get('updated_at')
            ))
            return True
            
        except Exception as e:
            print(f"Ошибка при вставке видео {video_data.get('id')}: {e}")
            return False

    def insert_snapshot(self, snapshot_data: Dict[str, Any], video_id: str) -> bool:
        """Вставка данных снапшота в таблицу snapshots"""
        try:
            query = """
            INSERT OR REPLACE INTO snapshots 
            (snapshot_id, video_id, views_count, likes_count, reports_count, 
             comments_count, delta_views_count, delta_likes_count, 
             delta_reports_count, delta_comments_count, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            self.cursor.execute(query, (
                snapshot_data.get('id'),
                video_id,
                snapshot_data.get('views_count', 0),
                snapshot_data.get('likes_count', 0),
                snapshot_data.get('reports_count', 0),
                snapshot_data.get('comments_count', 0),
                snapshot_data.get('delta_views_count', 0),
                snapshot_data.get('delta_likes_count', 0),
                snapshot_data.get('delta_reports_count', 0),
                snapshot_data.get('delta_comments_count', 0),
                snapshot_data.get('created_at'),
                snapshot_data.get('updated_at')
            ))
            return True
            
        except Exception as e:
            print(f"Ошибка при вставке снапшота {snapshot_data.get('id')}: {e}")
            return False


    def process_json_file(self, json_file_path: str, batch_size: int = 1000) -> Dict[str, int]:
        """Обработка JSON файла с видео данными"""
        stats = {
            'total_videos': 0,
            'total_snapshots': 0,
            'successful_videos': 0,
            'failed_videos': 0
        }
        
        try:
            print(f"Чтение файла {json_file_path}...")
            with open(json_file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            videos = data.get('videos', [])
            stats['total_videos'] = len(videos)
            
            print(f"Найдено {stats['total_videos']} видео")
            
            # Обработка видео батчами
            for i in range(0, len(videos), batch_size):
                batch = videos[i:i + batch_size]
                print(f"Обработка батча {i//batch_size + 1}/{(len(videos)-1)//batch_size + 1}")
                
                for video in batch:
                    if self.insert_video_with_snapshots(video):
                        stats['successful_videos'] += 1
                        stats['total_snapshots'] += len(video.get('snapshots', []))
                    else:
                        stats['failed_videos'] += 1
                
                # Коммит после каждого батча
                self.conn.commit()
                
                print(f"Обработано {i + len(batch)}/{len(videos)} видео")
            
            print("Все данные успешно загружены в базу")
            return stats
            
        except FileNotFoundError:
            print(f"Файл {json_file_path} не найден")
            return stats
        except json.JSONDecodeError as e:
            print(f"Ошибка при чтении JSON файла: {e}")
            return stats
        except Exception as e:
            print(f"Неожиданная ошибка: {e}")
            return stats

    def get_database_stats(self) -> Dict[str, Any]:
        """Получение статистики по базе данных"""
        try:
            stats = {}
            
            # Количество видео
            self.cursor.execute("SELECT COUNT(*) FROM videos")
            stats['videos_count'] = self.cursor.fetchone()[0]
            
            # Количество снапшотов
            self.cursor.execute("SELECT COUNT(*) FROM snapshots")
            stats['snapshots_count'] = self.cursor.fetchone()[0]
            
            # Количество уникальных создателей
            self.cursor.execute("SELECT COUNT(DISTINCT creator_id) FROM videos")
            stats['unique_creators'] = self.cursor.fetchone()[0]
            
            # Статистика по просмотрам
            self.cursor.execute("""
                SELECT 
                    SUM(views_count) as total_views,
                    AVG(views_count) as avg_views,
                    MAX(views_count) as max_views
                FROM videos
            """)
            views_stats = self.cursor.fetchone()
            stats['total_views'] = views_stats[0] or 0
            stats['avg_views'] = views_stats[1] or 0
            stats['max_views'] = views_stats[2] or 0
            
            return stats
            
        except Exception as e:
            print(f"Ошибка при получении статистики: {e}")
            return {}

    def close(self):
        """Закрытие соединения с базой данных"""
        self.conn.close()


def main():
    # Конфигурация
    JSON_FILE_PATH = "videos.json"  
    DB_FILE_PATH = "database/videos.db"
    
    # Создаем директорию для базы данных, если её нет
    import os
    os.makedirs(os.path.dirname(DB_FILE_PATH), exist_ok=True)
    
    # Инициализация базы данных
    print("Инициализация базы данных...")
    db = DataBaseManager(DB_FILE_PATH)
    
    # Обработка JSON файла
    print("\nНачало обработки JSON файла...")
    stats = db.process_json_file(JSON_FILE_PATH)
    
    # Вывод статистики обработки
    print("\n=== Статистика обработки ===")
    print(f"Всего видео в файле: {stats.get('total_videos', 0)}")
    print(f"Успешно обработано: {stats.get('successful_videos', 0)}")
    print(f"Не удалось обработать: {stats.get('failed_videos', 0)}")
    print(f"Всего снапшотов: {stats.get('total_snapshots', 0)}")
    
    # Получение и вывод статистики базы данных
    print("\n=== Статистика базы данных ===")
    db_stats = db.get_database_stats()
    if db_stats:
        print(f"Видео в базе: {db_stats.get('videos_count', 0)}")
        print(f"Снапшотов в базе: {db_stats.get('snapshots_count', 0)}")
        print(f"Уникальных создателей: {db_stats.get('unique_creators', 0)}")
        print(f"Всего просмотров: {db_stats.get('total_views', 0):,}")
        print(f"Среднее просмотров на видео: {db_stats.get('avg_views', 0):.1f}")
        print(f"Максимум просмотров: {db_stats.get('max_views', 0):,}")
    
    # Пример запроса для получения данных
    print("\n=== Пример запроса данных ===")
    try:
        db.cursor.execute("""
            SELECT v.video_id, v.views_count, v.likes_count, COUNT(s.snapshot_id) as snapshots_count
            FROM videos v
            LEFT JOIN snapshots s ON v.video_id = s.video_id
            GROUP BY v.video_id
            ORDER BY v.views_count DESC
            LIMIT 5
        """)
        
        top_videos = db.cursor.fetchall()
        print("Топ 5 видео по просмотрам:")
        for video in top_videos:
            print(f"  ID: {video[0]}, Просмотры: {video[1]}, Лайки: {video[2]}, Снапшотов: {video[3]}")
            
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
    
    # Закрытие соединения
    db.close()
    print("\nОбработка завершена!")


if __name__ == "__main__":
    main()