#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试SQLite数据库查询
"""

import sqlite3
import os

def test_database_queries():
    """测试数据库查询功能"""
    db_path = 'test_sqlite_output/test_geonames.db'
    
    if not os.path.exists(db_path):
        print(f"数据库文件不存在: {db_path}")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 检查表
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"Tables: {tables}")
        
        # 检查索引
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = [row[0] for row in cursor.fetchall()]
        print(f"Indexes: {indexes}")
        
        # 查询states表样本
        cursor.execute("SELECT * FROM states LIMIT 2")
        states_sample = cursor.fetchall()
        print(f"States sample: {states_sample}")
        
        # 查询state_names表样本
        cursor.execute("SELECT * FROM state_names LIMIT 3")
        state_names_sample = cursor.fetchall()
        print(f"State_names sample: {state_names_sample}")
        
        # 测试索引查询性能
        cursor.execute("SELECT * FROM state_names WHERE country_code='US' AND name='California'")
        lookup_result = cursor.fetchall()
        print(f"Lookup test (US, California): {lookup_result}")
        
        # 测试外键约束
        cursor.execute("""
            SELECT sn.country_code, sn.name, s.name as state_name 
            FROM state_names sn 
            JOIN states s ON sn.geonameid = s.geonameid 
            LIMIT 3
        """)
        join_result = cursor.fetchall()
        print(f"Join test (state_names + states): {join_result}")
        
        conn.close()
        print("数据库查询测试完成")
        
    except Exception as e:
        print(f"查询测试出错: {e}")

if __name__ == "__main__":
    test_database_queries()