import os
from dotenv import load_dotenv
import asyncio
import re
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)

from sqlalchemy import Column, Integer, BigInteger, Text as SQLText, TIMESTAMP, ForeignKey, select
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# ====================================================
# Определяем модели (таблицы) для базы данных через SQLAlchemy
# ====================================================

Base = declarative_base()


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False)
    balans = Column(Integer, default=0)
    links = relationship("YTUrlLink", back_populates="user", cascade="all, delete-orphan")


class YTUrlLink(Base):
    __tablename__ = 'yt_url_links'
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, nullable=False)
    url_link = Column(SQLText, nullable=False)
    url_name = Column(SQLText, nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'))
    user = relationship("User", back_populates="links")
    views = relationship("YTView", back_populates="url_link_obj", cascade="all, delete-orphan")


class YTView(Base):
    __tablename__ = 'yt_views'
    id = Column(Integer, primary_key=True)
    datetime = Column(TIMESTAMP, nullable=False)
    url_link_id = Column(Integer, ForeignKey('yt_url_links.id', ondelete="CASCADE"), nullable=False)
    views = Column(BigInteger, nullable=False)
    url_link_obj = relationship("YTUrlLink", back_populates="views")


# ====================================================
# Менеджер базы данных (SQLite с aiosqlite)
# ====================================================

class DatabaseManager:
    def __init__(self, dsn: str):
        self.engine = create_async_engine(dsn, echo=False, future=True)
        self.async_session = async_sessionmaker(self.engine, expire_on_commit=False)

    async def init(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def register_user(self, telegram_id: int):
        async with self.async_session() as session:
            async with session.begin():
                result = await session.execute(select(User).filter(User.telegram_id == telegram_id))
                user = result.scalar_one_or_none()
                if not user:
                    user = User(telegram_id=telegram_id, balans=0)
                    session.add(user)

    async def add_tracking_link(self, telegram_id: int, url_link: str, url_name: str):
        async with self.async_session() as session:
            async with session.begin():
                result = await session.execute(select(User).filter(User.telegram_id == telegram_id))
                user = result.scalar_one_or_none()
                link = YTUrlLink(telegram_id=telegram_id, url_link=url_link, url_name=url_name, user=user)
                session.add(link)
                await session.flush()  # гарантирует генерацию link.id
                return link.id

    async def get_tracking_links(self, telegram_id: int):
        async with self.async_session() as session:
            result = await session.execute(select(YTUrlLink).filter(YTUrlLink.telegram_id == telegram_id))
            links = result.scalars().all()
            return links

    async def delete_tracking_link(self, link_id: int):
        async with self.async_session() as session:
            async with session.begin():
                result = await session.execute(select(YTUrlLink).filter(YTUrlLink.id == link_id))
                link = result.scalar_one_or_none()
                if link:
                    await session.delete(link)

    async def insert_view_record(self, url_link_id: int, views: int):
        async with self.async_session() as session:
            async with session.begin():
                record = YTView(datetime=datetime.utcnow(), url_link_id=url_link_id, views=views)
                session.add(record)

    async def get_view_stats(self, url_link_id: int):
        async with self.async_session() as session:
            result = await session.execute(
                select(YTView).filter(YTView.url_link_id == url_link_id).order_by(YTView.datetime)
            )
            stats = result.scalars().all()
            return stats

    async def get_latest_view(self, url_link_id: int):
        async with self.async_session() as session:
            result = await session.execute(
                select(YTView).filter(YTView.url_link_id == url_link_id).order_by(YTView.datetime.desc()).limit(1)
            )
            record = result.scalar_one_or_none()
            return record.views if record else 0

    async def get_all_users(self):
        async with self.async_session() as session:
            result = await session.execute(select(User))
            users = result.scalars().all()
            return users


# ====================================================
# Менеджер для работы с YouTube API
# ====================================================

class YouTubeAPIManager:
    def __init__(self, api_key: str):
        self.api_key = api_key

    def extract_video_id(self, url: str):
        patterns = [
            r'(?:v=|\/)([0-9A-Za-z_-]{11}).*',
        ]
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None

    async def get_video_views(self, url: str):
        # Оставляем для обратной совместимости, если потребуется запрос по одному видео
        video_id = self.extract_video_id(url)
        if not video_id:
            return None
        api_url = (
            f'https://www.googleapis.com/youtube/v3/videos?part=statistics&id='
            f'{video_id}&key={self.api_key}'
        )
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as resp:
                if resp.status != 200:
                    logging.error(f"Ошибка при запросе YouTube API для video_id {video_id}")
                    return None
                data = await resp.json()
                items = data.get('items', [])
                if not items:
                    logging.error(f"Не найдены данные по video_id {video_id}")
                    return None
                stats = items[0].get('statistics', {})
                try:
                    return int(stats.get('viewCount', 0))
                except ValueError:
                    return 0

    async def get_videos_views(self, video_ids: list[str]) -> dict[str, int]:
        """Запрашивает просмотры для списка video_ids (до 50 за один запрос)"""
        if not video_ids:
            return {}
        ids_string = ",".join(video_ids)
        api_url = f'https://www.googleapis.com/youtube/v3/videos?part=statistics&id={ids_string}&key={self.api_key}'
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as resp:
                if resp.status != 200:
                    logging.error(f"Ошибка при запросе YouTube API для видео: {ids_string}")
                    return {}
                data = await resp.json()
                items = data.get('items', [])
                result = {}
                for item in items:
                    vid = item.get('id')
                    stats = item.get('statistics', {})
                    try:
                        result[vid] = int(stats.get('viewCount', 0))
                    except ValueError:
                        result[vid] = 0
                return result



# ====================================================
# Состояния для FSM (добавление нового видео)
# ====================================================

class AddVideoStates(StatesGroup):
    waiting_for_url = State()
    waiting_for_name = State()


# ====================================================
# Основной класс бота (aiogram 3.x)
# ====================================================

class YouTubeBot:
    def __init__(self, bot_token: str, db: DatabaseManager, yt_api: YouTubeAPIManager):
        self.bot = Bot(token=bot_token)
        self.dp = Dispatcher(storage=MemoryStorage())
        self.db = db
        self.yt_api = yt_api

        self.main_menu = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="Отслеживать новое видео")],
                [KeyboardButton(text="Мои отслеживания"), KeyboardButton(text="FAQ")]
            ],
            resize_keyboard=True
        )
        self.back_menu = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Вернуться назад")]],
            resize_keyboard=True
        )
        self.register_handlers()

    def register_handlers(self):
        self.dp.message.register(self.start, Command("start"))
        self.dp.message.register(
            self.handle_main_menu,
            F.text.in_(["Отслеживать новое видео", "Мои отслеживания", "FAQ"])
        )
        self.dp.message.register(self.process_new_video_link, AddVideoStates.waiting_for_url)
        self.dp.message.register(self.process_new_video_name, AddVideoStates.waiting_for_name)
        self.dp.message.register(self.back_to_menu, F.text == "Вернуться назад")
        self.dp.callback_query.register(self.handle_link_selection, F.data.startswith("link_"))
        self.dp.callback_query.register(
            self.handle_link_actions,
            lambda callback: callback.data and (callback.data.startswith("analytics_") or callback.data.startswith("stop_"))
        )

    async def start(self, message: types.Message, state: FSMContext):
        telegram_id = message.from_user.id
        await self.db.register_user(telegram_id)
        await message.answer("Добро пожаловать!", reply_markup=self.main_menu)

    async def handle_main_menu(self, message: types.Message, state: FSMContext):
        text = message.text
        if text == "Отслеживать новое видео":
            await message.answer("Пришлите ссылку на видео или Shorts на YouTube", reply_markup=self.back_menu)
            await state.set_state(AddVideoStates.waiting_for_url)
        elif text == "Мои отслеживания":
            telegram_id = message.from_user.id
            links = await self.db.get_tracking_links(telegram_id)
            if not links:
                await message.answer("У вас нет отслеживаемых ссылок.", reply_markup=self.main_menu)
                return
            # Создаем клавиатуру с пустым списком inline_keyboard
            inline_kb = InlineKeyboardMarkup(inline_keyboard=[])
            for link in links:
                btn = InlineKeyboardButton(text=link.url_name, callback_data=f"link_{link.id}")
                inline_kb.inline_keyboard.append([btn])
            await message.answer("Выберите ссылку для работы", reply_markup=inline_kb)
        elif text == "FAQ":
            await message.answer("FAQ: Здесь будет информация.", reply_markup=self.main_menu)

    async def back_to_menu(self, message: types.Message, state: FSMContext):
        await state.clear()
        await message.answer("Главное меню", reply_markup=self.main_menu)

    async def process_new_video_link(self, message: types.Message, state: FSMContext):
        await state.update_data(url_link=message.text)
        await message.answer("Пришлите название для этой ссылки", reply_markup=self.back_menu)
        await state.set_state(AddVideoStates.waiting_for_name)

    async def process_new_video_name(self, message: types.Message, state: FSMContext):
        data = await state.get_data()
        url_link = data.get("url_link")
        url_name = message.text
        telegram_id = message.from_user.id
        await self.db.add_tracking_link(telegram_id, url_link, url_name)
        await message.answer("Ссылка сохранена", reply_markup=self.main_menu)
        await state.clear()

    async def handle_link_selection(self, callback_query: types.CallbackQuery, state: FSMContext):
        data = callback_query.data  # Например, "link_123"
        link_id = int(data.split("_")[1])
        inline_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Аналитика просмотров", callback_data=f"analytics_{link_id}")],
            [InlineKeyboardButton(text="Перестать отслеживать", callback_data=f"stop_{link_id}")]
        ])
        await self.bot.send_message(callback_query.from_user.id, "Выберите действие для ссылки", reply_markup=inline_kb)
        await callback_query.answer()

    async def handle_link_actions(self, callback_query: types.CallbackQuery, state: FSMContext):
        data = callback_query.data
        if data.startswith("analytics_"):
            link_id = int(data.split("_")[1])
            stats = await self.db.get_view_stats(link_id)
            if not stats:
                await self.bot.send_message(callback_query.from_user.id, "Нет данных по просмотрам для этой ссылки. (данные обновляются раз в 8 часов)")
            else:
                msg = "Статистика просмотров:\n"
                for record in stats:
                    dt_str = record.datetime.strftime("%Y-%m-%d %H:%M:%S")
                    msg += f"{dt_str}: {record.views} просмотров\n"
                await self.bot.send_message(callback_query.from_user.id, msg)
        elif data.startswith("stop_"):
            link_id = int(data.split("_")[1])
            await self.db.delete_tracking_link(link_id)
            await self.bot.send_message(callback_query.from_user.id, "Отслеживание остановлено.")
        await callback_query.answer()

    async def send_daily_summary(self):
        users = await self.db.get_all_users()
        for user in users:
            telegram_id = user.telegram_id
            links = await self.db.get_tracking_links(telegram_id)
            if not links:
                continue
            msg = "Дневная статистика по вашим ссылкам:\n"
            for idx, link in enumerate(links, start=1):
                latest_views = await self.db.get_latest_view(link.id)
                msg += f"{idx}) {link.url_name} - {latest_views} просмотров\n"
            try:
                await self.bot.send_message(telegram_id, msg, reply_markup=self.main_menu)
            except Exception as e:
                logging.error(f"Ошибка отправки дневной статистики пользователю {telegram_id}: {e}")

    async def run(self):
        await self.dp.start_polling(self.bot)


# ====================================================
# Фоновые задачи
# ====================================================

async def periodic_view_update(db: DatabaseManager, yt_api: YouTubeAPIManager):
    while True:
        logging.info("Запуск обновления просмотров...")
        async with db.async_session() as session:
            result = await session.execute(select(YTUrlLink))
            links = result.scalars().all()

        # Словарь: video_id -> список link_id, для которых этот video_id получен
        video_map = {}
        for link in links:
            vid = yt_api.extract_video_id(link.url_link)
            if vid:
                video_map.setdefault(vid, []).append(link.id)

        video_ids = list(video_map.keys())
        batch_size = 50
        # Обрабатываем видео пакетами по 50
        for i in range(0, len(video_ids), batch_size):
            batch = video_ids[i:i + batch_size]
            views_dict = await yt_api.get_videos_views(batch)
            for vid in batch:
                if vid in views_dict:
                    view_count = views_dict[vid]
                    # Для каждого link_id, связанного с этим video id, записываем статистику
                    for link_id in video_map[vid]:
                        await db.insert_view_record(link_id, view_count)
                        logging.info(f"Обновлено для ссылки {link_id} (video {vid}): {view_count} просмотров")
        await asyncio.sleep(8 * 60 * 60)  # 8 часов



async def daily_summary_task(bot_instance: YouTubeBot):
    local_tz = ZoneInfo("Europe/Moscow")  # замените на нужный вам часовой пояс
    while True:
        now = datetime.now(tz=local_tz)
        next_run = now.replace(hour=9, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        seconds_until_next = (next_run - now).total_seconds()
        await asyncio.sleep(seconds_until_next)
        logging.info("Рассылка дневной статистики пользователям...")
        await bot_instance.send_daily_summary()


# ====================================================
# Главная функция
# ====================================================



load_dotenv()
BOT_TOKEN = os.getenv('TG_TOKEN')
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
DATABASE_URL = 'sqlite+aiosqlite:///db.sqlite3'

async def main():
    db_manager = DatabaseManager(DATABASE_URL)
    await db_manager.init()
    yt_api_manager = YouTubeAPIManager(YOUTUBE_API_KEY)
    yt_bot = YouTubeBot(BOT_TOKEN, db_manager, yt_api_manager)

    asyncio.create_task(periodic_view_update(db_manager, yt_api_manager))
    asyncio.create_task(daily_summary_task(yt_bot))
    await yt_bot.run()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Бот выключен')