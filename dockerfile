# Используем официальный slim-образ Python 3.10
FROM python

# Отключаем запись .pyc файлов и буферизацию вывода
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Обновляем пакеты и устанавливаем системные зависимости:
# tzdata - для работы с часовыми поясами (если используется zoneinfo)
# gcc и build-essential - для сборки некоторых пакетов (например, aiosqlite или SQLAlchemy)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        tzdata \
        gcc \
        build-essential && \
    rm -rf /var/lib/apt/lists/*

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем файл зависимостей
COPY requirements.txt .

# Обновляем pip и устанавливаем зависимости
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Копируем весь исходный код в рабочую директорию
COPY . .

# Команда для запуска бота
CMD ["python", "run.py"]
