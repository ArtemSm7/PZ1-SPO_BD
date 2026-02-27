from config import config
import mysql.connector

class DB:
    def __init__(self, table):
        self.table = table
        self.conn = mysql.connector.connect(**config)
        self.cursor = self.conn.cursor(dictionary=True)
        
    def create_table(self, columns):
        cols = ', '.join([f"{name} {type}" for name, type in columns.items()])
        query = f"CREATE TABLE IF NOT EXISTS {self.table} ({cols})"
        self.cursor.execute(query)
        self.conn.commit()
        print(f"Таблица {self.table} создана или уже существует")
        
    def drop_table(self, if_exists=True):
        if_exists_str = "IF EXISTS " if if_exists else ""
        query = f"DROP TABLE {if_exists_str}{self.table}"
        self.cursor.execute(query)
        self.conn.commit()
        print(f"Таблица {self.table} удалена")
    
    def create(self, data):
        cols = ', '.join(data.keys())
        vals = tuple(data.values())
        place = ', '.join(['%s'] * len(data))
        self.cursor.execute(f"INSERT INTO {self.table} ({cols}) VALUES ({place})", vals)
        self.conn.commit()
       
    
    def read(self, where=None):
        query = f"SELECT * FROM {self.table}"
        if where:
            cond = ' AND '.join([f"{k}=%s" for k in where.keys()])
            query += f" WHERE {cond}"
            self.cursor.execute(query, tuple(where.values()))
        else:
            self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def update(self, data, where):
        set_clause = ', '.join([f"{k}=%s" for k in data.keys()])
        where_clause = ' AND '.join([f"{k}=%s" for k in where.keys()])
        params = tuple(list(data.values()) + list(where.values()))
        self.cursor.execute(f"UPDATE {self.table} SET {set_clause} WHERE {where_clause}", params)
        self.conn.commit()
    
    def delete(self, where):
        cond = ' AND '.join([f"{k}=%s" for k in where.keys()])
        self.cursor.execute(f"DELETE FROM {self.table} WHERE {cond}", tuple(where.values()))
        self.conn.commit()
    
    def close(self):
        self.cursor.close()
        self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()   
