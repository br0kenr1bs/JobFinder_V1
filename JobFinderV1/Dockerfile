# Используйте образ с предустановленным Python и ChromeDriver
FROM joyzoursky/python-chromedriver:3.9-selenium

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем необходимые файлы в контейнер
COPY . .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем переменные окружения
ENV TELEGRAM_TOKEN=**********:************************************
ENV DB_NAME=postgres
ENV DB_USER=postgres
ENV DB_PASSWORD=****
ENV DB_HOST=db
ENV DB_PORT=****

# Делаем скрипт запуска исполняемым
RUN chmod +x /app/entrypoint.sh

# Запуск скрипта
ENTRYPOINT ["/app/entrypoint.sh"]
