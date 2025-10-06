# final_project
Глушакова Наталия

____

## **Оглавление**

- [Цель проекта](#цель-проекта)
- [Источники данных](#источники-данных)
- [Возможные применения](#возможные-применения)
- [Архитектура](#архитектура)
- [Этапы ETL-процесса](#этапы-etl-процесса)
- [Технологический стек](#технологический-стек)
- [Пайплайн](#пайплайн)
- [Запуск проекта](#запуск-проекта)

 ## **Цель проекта**

Целью является - сбор, обработка и визуализация пользовательских событий интернет-магазина, чтобы предоставить аналитические данные для дальнейшего развития бизнеса и выстраивания маркетинговой стратегии.

____

## **Источники данных**

Проект использует 3 источника данных:

Positionstack API – данные о геолокации исходя из координат. https://positionstack.com/documentation

Open Exchange Rates API – данные с актуальными и историческими курсами обмена валют. https://openexchangerates.org/about

OpenWeather API - предоставляет данные о погоде для любой точки мира. https://openweathermap.org/api

Дополнительно в проекте генерируются данные для моделирования посещений, суммы покупок и координат местоположения.

____

## **Возможные применения**

Финансовый анализ : общая выручка, динамика доходов по периодам, определение наиболее прибыльных товаров/категорий

Влияние погодных условий на посещение платформы

Выявление мест для повышенного внимания

Анализ популярности категорий товаров

____


## **Архитектура**

<img width="840" height="1193" alt="arch" src="https://github.com/user-attachments/assets/08be53c0-4aac-43a0-b91d-b370c6aa6e7c" />

____


## **Этапы ETL-процесса**

Процесс делится на несколько этапов:

1. *Extraction (извлечение)*

Генерация событий (Python, generator.py)
Запись в Kafka.
Cохранение события в RAW (Postgres).

2. *Transformation (обработка)*

Обработка и нормализация данных.

3. *Loading (загрузка)*

 PostgreSQL – хранение сырых данных (raw) и обработанных данных (dds).

 ClickHouse – аналитической витрины для построения дашбордов.

## **Визуализация**

Metabase используется для построения дашбордов с ключевыми метриками:

эффективность товаров/категорий
вовлеченность пользователей
сессии, кампании, гео и девайсы


## **Оркестрация и автоматизация**

Airflow отвечает за оркестрацию ETL-процессов, а также логирование и уведомления о сбоях через Telegram Bot

____


## **Технологический стек**

| Компонент | Используемые технологии |
|:----------------:|:---------:|
| Язык | Python |
| Базы данных | PostgreSQL, ClickHouse | 
| ETL / Оркестрация | Airflow | 
| API | Positionstack, Open Exchange Rates, OpenWeather | 
| Визуализация | Metabase | 
| Уведомления | etl_airflow_alerts_bot | 

____


## **Пайплайн**
### 1. DAG генерации данных

*generate_events_minutely*

Каждую минуту генерирует тестовые данные
Создает пользователей, события, заказы, кампании
Отправляет в Kafka (топик user_events)

  *generator.py/def generate_campaign_event*
 Генерирует событие взаимодействия с маркетинговой кампанией
  *generator.py/def get_currency_rates*
 Получает актуальные курсы валют от API
  *generator.py/def get_random_city*
 Генерирует координаты и город по ним через PositionStack API
  *generator.py/def get_weather*
 Получает данные о погоде по координатам через OpenWeatherMap API
   *generator.py/def generate_user*
 Генерирует набор событий профиля пользователя
  *generator.py/def generate_order*
 Генерирует события заказа и позиции заказа
    *generator.py/def generate_event*
 Генерирует одно событие пользовательской активности
  *generator.py/def produce_batch*
 Отправляет событие в Kafka

### 2. DAG заполнение RAW слоя

<img width="973" height="657" alt="raw" src="https://github.com/user-attachments/assets/664fc1e8-0c34-4ee6-9a54-931eba808c73" />

*kafka_to_postgres*

Каждые 30 минут забирает данные из Kafka
Сохраняет в RAW-слой PostgreSQL (сырые данные)

  *dags_utils.py/def def check_kafka_message*
 Проверяет, есть ли хотя бы одно сообщение в Kafka
  *generator.py/def consume_and_save*
 Читает сообщения из Kafka и кладёт их в соответствующие raw таблицы

### 3. DAG заполнение DDS слоя

<img width="562" height="243" alt="dds" src="https://github.com/user-attachments/assets/87eb7cf6-9b20-434a-9a04-d64352e7d672" />

*raw_to_dds_enhanced*

Каждый час преобразует RAW → DDS
Валидация, очистка, нормализация данных
Создает структурированные таблицы 

  *raw_to_dds.py/def upsert_users_from_raw* 
 объединяет данные пользователей из разных источников
  *raw_to_dds.py/def validate_and_load_events* 
 валидирует и загружает события
  *raw_to_dds.py/def load_orders_from_events* 
 извлекает заказы из событий
  *raw_to_dds.py/def load_products_and_categories* 
 обрабатывает продукты и категории
  *raw_to_dds.py/def clean_and_validate* 
 утилита очистки и валидации данных

### 4. DAG подготовки данных для витрин(Clickhouse)

*dds_to_clickhouse*

  *ch_utils.py/def transfer_table* 
 Переносит данные из PostgreSQL в ClickHouse, денормализация данных и последующая загрузка в витрины

### 5. DAG инициализация DWH

  *init_dwh*
 Выполняет SQL-скрипт init_schema.sql Создает все таблицы и схемы

### 6. DAG инициализация Clickhouse

  *init_clickhouse*
 Выполняет SQL-скрипт init_clickhouse.sql Создает все витрины

### 7. Metabase

<img width="1511" height="1032" alt="dashboard" src="https://github.com/user-attachments/assets/3698b04d-e572-43d9-add3-7290d396f7c0" />

  **- Продажи по категориям**
  **- Ежедневная выручка**
  **- Активные пользователи**
  **- Устройства**


### 8. Запуск проекта

1. Клонирование репозитория

 ```bash 
git clone https://github.com/cherrypie1/final_project.git
```
  
2. Запсутить docker-compose

 ```bash 
docker-compose up
```

3. Настроить перемнные:
**- generator.py**
```bash
CURRENCY_API = 
WEATHER_API_KEY = 
POSITIONSTACK_KEY = 
```
**- telegram_alerts.py**
```bash
TELEGRAM_BOT_TOKEN = 
TELEGRAM_CHAT_ID = 
```
**- Postgres&Clickhouse**
 Переменные находятся в docker=compose.yml
 
4. Запустить dags http://127.0.0.1:8081
     user: admin
     password: airflow
  Запустить Metabase http://localhost:3000
