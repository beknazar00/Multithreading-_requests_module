import requests
import json
import threading
import time
import psycopg2

product_url = requests.get('https://dummyjson.com/products')

product_list = product_url.json()['products']

class Product:
    def __init__(self):
        self.conn = psycopg2.connect(
            database = "lesson",
            user = "postgres",
            password = "703",
            host = "localhost",
            port = 5432
        )
        self.cursor = self.conn.cursor()
    def __enter__(self):
        self.cursor = self.conn.cursor()
        return self.cursor, self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()
    

    
    def create_product_table(self):
        CREATE_TABLE_Product_json = """CREATE TABLE IF NOT EXISTS product
            (
            id SERIAL PRIMARY KEY,
            title VARCHAR(300) NOT NULL,
            description TEXT,
            category VARCHAR(100) NOT NULL,
            price FLOAT4,
            discountPercentage FLOAT4,
            rating FLOAT4,
            stock FLOAT4,
            tags TEXT,
            sku VARCHAR(100) NOT NULL,
            weight INT
            );"""
        return CREATE_TABLE_Product_json 
    
    @staticmethod
    def save():
        insert_into_query = """ insert into product(title,description,category,price,
        discountPercentage,rating,stock,tags,sku,weight) 
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """
        return insert_into_query

product = Product()
with product as (cursor , conn):
    query = Product.save()
    for product in product_list:
        cursor.execute(query,(product['title'],product['description'],product['category'],
        product['price'],product['discountPercentage'],product['rating'],product['stock'],
        product['tags'],product['sku'],product['weight']))
    conn.commit()