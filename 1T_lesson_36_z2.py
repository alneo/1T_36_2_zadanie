# Цель задания: использовать Apache Spark для создания синтетического набора данных, который имитирует информацию о покупках в интернет-магазине. Набор данных должен включать в себя информацию о заказах, включая дату заказа, идентификатор пользователя, название товара, количество и цену. Сгенерированные данные будут использованы для последующего анализа покупательской активности и понимания потребительских трендов.
import random
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from google.colab import  drive

spark = SparkSession.builder.appName("1T_36_2_zadanie").getOrCreate()

# 1. Генерация данных:
# 1.1. Создать DataFrame с полями: Дата, UserID, Продукт, Количество, Цена.
# 1.2. Данные для поля Продукт генерируются из списка возможных товаров (не меньше 5 товаров)
# 1.3. Количество и Цена должны генерироваться случайно в заданных пределах.
# 1.4. Дата должна быть в пределах последнего года.
# 1.5. UserID представляет собой случайное число, имитирующее идентификаторы пользователей.
# 1.6. Обратите внимание, что должна быть возможности изменять количество сгенерированных строк. Минимальное количество - 1000 строк.

num_records = 1000
products = ["Карандаш", "Ручка шариковая", "Ластик", "Бумага А4", "Степлер", "Дневник", "Рюкзак"]

def random_date():
    today = datetime.today()
    year_start = datetime(today.year, 1, 1)
    obj = today - year_start
    start = datetime.now() - timedelta(days=obj.days)
    end = datetime.now()
    return start + (end - start) * random.random()

def random_user_id():
    return random.randint(1, 10000)

def random_product():
    return random.choice(products)

def random_quantity():
    return random.randint(1, 10)

def random_price():
    return round(random.uniform(10, 2000), 2)
    
# Создание DataFrame
data = [
    (
        random_date(), 
        random_user_id(), 
        random_product(), 
        random_quantity(), 
        random_price()
    ) for _ in range(num_records)]
schema = ["Дата", "UserID", "Продукт", "Количество", "Цена"]

df = spark.createDataFrame(data, schema)

# 2. Сохранение данных: Сохранить сгенерированный DataFrame в формате CSV для последующего анализа.
file = "zadanie_36_2.csv"
df.write.csv(file, header=True, mode="overwrite")
spark.stop()