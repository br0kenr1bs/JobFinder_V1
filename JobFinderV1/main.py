import os
import psycopg2
import time
import dotenv
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, StaleElementReferenceException
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
# from aiogram.dispatcher.filters import Command
from aiogram.utils import executor
import asyncio
import concurrent.futures
import random

dotenv.load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

TOKEN = os.environ.get('TELEGRAM_TOKEN')

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

def connect_db():

    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="2778",
        host="db",
        port="5432"
    )
    return conn

def insert_vacancy(conn, company, title, meta_info, salary, skills, link):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO vacancies (company, vacancy, location, salary, skills, link)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id;
        """, (company, title, meta_info, salary, skills, link))
        conn.commit()
        return cur.fetchone()[0]

def parse_habr(query):
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-software-rasterizer')
    options.add_argument('--disable-webgl')
    options.add_argument('--disable-web-security')
    options.add_argument('--disable-features=WebRtcHideLocalIpsWithMdns,WebContentsDelegate::CheckMediaAccessPermission')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--remote-debugging-port=9222')
    options.add_argument('--enable-features=NetworkService,NetworkServiceInProcess')
    options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_experimental_option('prefs', {
        'profile.managed_default_content_settings.images': 2,
        'disk-cache-size': 4096
    })

    driver = webdriver.Chrome(options=options)

    conn = connect_db()

    try:
        driver.get('https://career.habr.com')

        search_input = driver.find_element(By.CSS_SELECTOR, '.l-page-title__input')
        search_input.send_keys(query)
        search_input.send_keys(Keys.RETURN)

        time.sleep(1)

        while True:
            vacancies = driver.find_elements(By.CLASS_NAME, 'vacancy-card__info')
            for vacancy in vacancies:
                try:
                    company_element = vacancy.find_element(By.CLASS_NAME, 'vacancy-card__company-title')
                    company = company_element.text
                except NoSuchElementException:
                    company = 'Компания не указана'

                title_element = vacancy.find_element(By.CLASS_NAME, 'vacancy-card__title')
                title = title_element.text
                link = title_element.find_element(By.TAG_NAME, 'a').get_attribute('href')

                try:
                    meta_element = vacancy.find_element(By.CLASS_NAME, 'vacancy-card__meta')
                    meta_info = meta_element.text
                except NoSuchElementException:
                    meta_info = 'Местоположение не указано'

                try:
                    salary = vacancy.find_element(By.CLASS_NAME, 'vacancy-card__salary').text
                except NoSuchElementException:
                    salary = 'ЗП не указана'

                try:
                    skills = vacancy.find_element(By.CLASS_NAME, 'vacancy-card__skills').text
                except NoSuchElementException:
                    skills = 'Скиллы не указаны'

                vacancy_id = insert_vacancy(conn, company, title, meta_info, salary, skills, link)

                print(f'Компания: {company}\nВакансия: {title}\nСсылка: {link}\nМестоположение и режим работы: {meta_info}\nЗарплата: {salary}\nСкиллы: {skills}')

            try:
                next_button = driver.find_element(By.CSS_SELECTOR, 'a.button-comp--appearance-pagination-button[rel="next"]')
                driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                time.sleep(1)
                
                for _ in range(3):
                    try:
                        driver.execute_script("arguments[0].click();", next_button)
                        break
                    except StaleElementReferenceException:
                        next_button = driver.find_element(By.CSS_SELECTOR, 'a.button-comp--appearance-pagination-button[rel="next"]')
                        time.sleep(1)
                else:
                    break
                
                time.sleep(1)
            except (NoSuchElementException, ElementClickInterceptedException):
                break

    finally:
        driver.quit()
        conn.close()

@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    await message.reply('Используйте /search <запрос>, чтобы искать вакансии.\nДля остального функционала можно задействовать /help')

@dp.message_handler(commands=['help'])
async def start(message: types.Message):
    await message.reply('Краткая сводка по командам\n/start - запуск/перезапуск бота\n/search <запрос> - поиск вакансий по запросу\n/recent - вывод 5 случайных вакансий\n/count - вывод общего кол-ва вакансий в бд\n/grafic - вывод на выбор режима раб. дня\n/search_company - поиск вакансий по компании из бд\n/search_vacancy - поиск вакансий по названию вакансии из бд')

@dp.message_handler(commands=['search'])
async def search(message: types.Message):
    query = message.get_args()
    logging.info(f"Получен запрос для поиска: {query}")
    if not query:
        await message.reply('Пожалуйста, введите запрос после команды /search.')
        return

    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM vacancies;")
        initial_count = cur.fetchone()[0]
    conn.close()

    await message.reply(f'Ищу вакансии для: {query}')
    await run_parse_habr(query)
    await message.reply('Поиск завершен. Проверьте свою базу данных.')

    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute("SELECT company, vacancy, location, salary, skills, link FROM vacancies WHERE id > %s ORDER BY id LIMIT 5;", (initial_count,))
        rows = cur.fetchall()
    conn.close()

    if not rows:
        await message.reply('Новые вакансии не найдены.')
    else:
        await message.reply('Ниже представлены 5 новых вакансий:')
        for row in rows:
            await message.reply(f'Компания: {row[0]}\nВакансия: {row[1]}\nМестоположение: {row[2]}\nЗарплата: {row[3]}\nСкиллы: {row[4]}\nСсылка: {row[5]}\n')

async def run_parse_habr(query: str):
    loop = asyncio.get_event_loop()
    executor = concurrent.futures.ThreadPoolExecutor()
    await loop.run_in_executor(executor, parse_habr, query)

@dp.message_handler(commands=['recent'])
async def recent(message: types.Message):
    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute("SELECT company, vacancy, location, salary, skills, link FROM vacancies ORDER BY RANDOM() LIMIT 5;")
        rows = cur.fetchall()
    conn.close()

    if not rows:
        await message.reply('Вакансии не найдены.')
    else:
        for row in rows:
            await message.reply(f'Компания: {row[0]}\nВакансия: {row[1]}\nМестоположение: {row[2]}\nЗарплата: {row[3]}\nСкиллы: {row[4]}\nСсылка: {row[5]}\n')

@dp.message_handler(commands=['count'])
async def count(message: types.Message):
    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM vacancies;")
        count = cur.fetchone()[0]
    conn.close()
    await message.reply(f'Общее количество вакансий в базе данных: {count}')

@dp.message_handler(commands=['grafic'])
async def grafic(message: types.Message):
    keyboard = InlineKeyboardMarkup(row_width=2)
    buttons = [
        InlineKeyboardButton(text="Неполный рабочий день", callback_data='part_time'),
        InlineKeyboardButton(text="Полный рабочий день", callback_data='full_time')
    ]
    keyboard.add(*buttons)
    await message.reply("Выберите график работы:", reply_markup=keyboard)

@dp.callback_query_handler(lambda c: c.data in ['part_time', 'full_time'])
async def button(callback_query: types.CallbackQuery):
    query_data = callback_query.data

    conn = connect_db()
    with conn.cursor() as cur:
        if query_data == 'part_time':
            cur.execute("SELECT COUNT(*) FROM vacancies WHERE location ILIKE '%Неполный рабочий день%';")
        elif query_data == 'full_time':
            cur.execute("SELECT COUNT(*) FROM vacancies WHERE location ILIKE '%Полный рабочий день%';")
        count = cur.fetchone()[0]
    conn.close()

    await bot.answer_callback_query(callback_query.id)
    await bot.edit_message_text(text=f'Количество вакансий с графиком "{query_data}": {count}',
                                chat_id=callback_query.message.chat.id,
                                message_id=callback_query.message.message_id)

@dp.message_handler(commands=['search_company'])
async def search_by_company(message: types.Message):
    company_name = message.get_args()
    logging.info(f"Получен запрос для поиска по компании: {company_name}")
    if not company_name:
        await message.reply('Пожалуйста, введите название компании после команды /search_company.')
        return

    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute("SELECT company, vacancy, location, salary, skills, link FROM vacancies WHERE company ILIKE %s ORDER BY RANDOM() LIMIT 5;", (f"%{company_name}%",))
        rows = cur.fetchall()
    conn.close()

    if not rows:
        await message.reply(f'Вакансии компании "{company_name}" не найдены.')
    else:
        for row in rows:
            await message.reply(f'Компания: {row[0]}\nВакансия: {row[1]}\nМестоположение: {row[2]}\nЗарплата: {row[3]}\nСкиллы: {row[4]}\nСсылка: {row[5]}\n')

@dp.message_handler(commands=['search_vacancy'])
async def search_by_vacancy(message: types.Message):
    vacancy_query = message.get_args()
    logging.info(f"Получен запрос для поиска по вакансии: {vacancy_query}")
    if not vacancy_query:
        await message.reply('Пожалуйста, введите название вакансии после команды /search_vacancy.')
        return

    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute("SELECT company, vacancy, location, salary, skills, link FROM vacancies WHERE vacancy ILIKE %s ORDER BY RANDOM() LIMIT 5;", (f"%{vacancy_query}%",))
        rows = cur.fetchall()
    conn.close()

    if not rows:
        await message.reply(f'Вакансии по запросу "{vacancy_query}" не найдены.')
    else:
        for row in rows:
            await message.reply(f'Компания: {row[0]}\nВакансия: {row[1]}\nМестоположение: {row[2]}\nЗарплата: {row[3]}\nСкиллы: {row[4]}\nСсылка: {row[5]}\n')

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)