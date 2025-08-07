#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV地理匹配功能集成测试
简化版本，专注于核心功能验证
"""

import unittest
import os
import sys
import tempfile
import pandas as pd
import sqlite3

# 添加src目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from csv_geo_matcher import CSVGeoMatcher

class TestCSVGeoMatcherIntegration(unittest.TestCase):
    """
    CSV地理匹配器集成测试类
    """
    
    def setUp(self):
        """
        测试前的设置
        """
        # 创建临时目录
        self.temp_dir = tempfile.mkdtemp()
        self.test_db_path = os.path.join(self.temp_dir, 'test_geonames.db')
        self.test_input_csv = os.path.join(self.temp_dir, 'test_input.csv')
        self.test_output_csv = os.path.join(self.temp_dir, 'test_output.csv')
        
        # 创建测试数据库
        self._create_test_database()
        
        # 创建测试CSV文件
        self._create_test_csv_files()
        
        # 默认列名映射
        self.column_mapping = {
            'country_code': 'country_code',
            'state_name': 'state_name',
            'city_name': 'city_name'
        }
        
        self.matcher = None
    
    def tearDown(self):
        """
        测试后的清理
        """
        import shutil
        import time
        
        # 确保所有数据库连接都已关闭
        try:
            if self.matcher and hasattr(self.matcher, 'sqlite_integrator'):
                if self.matcher.sqlite_integrator and hasattr(self.matcher.sqlite_integrator, 'close'):
                    self.matcher.sqlite_integrator.close()
        except:
            pass
        
        # 等待一小段时间确保文件句柄释放
        time.sleep(0.1)
        
        # 清理临时目录
        if os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
            except PermissionError:
                time.sleep(0.5)
                try:
                    shutil.rmtree(self.temp_dir)
                except:
                    print(f"Warning: Could not clean up temp directory {self.temp_dir}")
    
    def _create_test_database(self):
        """
        创建测试数据库
        """
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        # 创建states表
        cursor.execute('''
            CREATE TABLE states (
                geonameid INTEGER PRIMARY KEY,
                name TEXT,
                country_code TEXT,
                admin1_code TEXT,
                population INTEGER
            )
        ''')
        
        # 创建cities表
        cursor.execute('''
            CREATE TABLE cities (
                geonameid INTEGER PRIMARY KEY,
                name TEXT,
                country_code TEXT,
                admin1_code TEXT,
                population INTEGER
            )
        ''')
        
        # 创建state_names表
        cursor.execute('''
            CREATE TABLE state_names (
                geonameid INTEGER,
                name TEXT,
                country_code TEXT,
                FOREIGN KEY (geonameid) REFERENCES states(geonameid)
            )
        ''')
        
        # 创建city_names表
        cursor.execute('''
            CREATE TABLE city_names (
                geonameid INTEGER,
                name TEXT,
                country_code TEXT,
                admin1_code TEXT,
                state_geonameid INTEGER,
                FOREIGN KEY (geonameid) REFERENCES cities(geonameid)
            )
        ''')
        
        # 插入测试数据
        states_data = [
            (5332921, 'California', 'US', 'CA', 39538223),
            (4736286, 'Texas', 'US', 'TX', 29145505)
        ]
        
        cities_data = [
            (5368361, 'Los Angeles', 'US', 'CA', 3971883),
            (4699066, 'Houston', 'US', 'TX', 2320268)
        ]
        
        state_names_data = [
            (5332921, 'California', 'US'),
            (4736286, 'Texas', 'US')
        ]
        
        city_names_data = [
            (5368361, 'Los Angeles', 'US', 'CA', 5332921),
            (4699066, 'Houston', 'US', 'TX', 4736286)
        ]
        
        cursor.executemany('INSERT INTO states VALUES (?, ?, ?, ?, ?)', states_data)
        cursor.executemany('INSERT INTO cities VALUES (?, ?, ?, ?, ?)', cities_data)
        cursor.executemany('INSERT INTO state_names VALUES (?, ?, ?)', state_names_data)
        cursor.executemany('INSERT INTO city_names VALUES (?, ?, ?, ?, ?)', city_names_data)
        
        conn.commit()
        conn.close()
    
    def _create_test_csv_files(self):
        """
        创建测试CSV文件
        """
        # 正常测试数据
        normal_data = [
            {'country_code': 'US', 'state_name': 'California', 'city_name': 'Los Angeles'},
            {'country_code': 'US', 'state_name': 'Texas', 'city_name': 'Houston'}
        ]
        
        df = pd.DataFrame(normal_data)
        df.to_csv(self.test_input_csv, index=False)
    
    def test_basic_functionality(self):
        """
        测试基本功能
        """
        self.matcher = CSVGeoMatcher(
            db_path=self.test_db_path,
            input_csv=self.test_input_csv,
            output_csv=self.test_output_csv,
            column_mapping=self.column_mapping,
            output_dir=self.temp_dir
        )
        
        # 测试初始化
        self.assertEqual(self.matcher.db_path, self.test_db_path)
        
        # 测试CSV数据加载
        data = self.matcher.load_csv_data()
        self.assertIsInstance(data, pd.DataFrame)
        self.assertEqual(len(data), 2)
        
        print("基本功能测试通过")
    
    def test_state_matching(self):
        """
        测试州匹配功能
        """
        self.matcher = CSVGeoMatcher(
            db_path=self.test_db_path,
            input_csv=self.test_input_csv,
            output_csv=self.test_output_csv,
            column_mapping=self.column_mapping,
            output_dir=self.temp_dir
        )
        
        # 测试州匹配
        result = self.matcher.match_state('US', 'California')
        self.assertIsNotNone(result)
        self.assertEqual(result, 5332921)
        
        print("州匹配测试通过")
    
    def test_city_matching(self):
        """
        测试城市匹配功能
        """
        self.matcher = CSVGeoMatcher(
            db_path=self.test_db_path,
            input_csv=self.test_input_csv,
            output_csv=self.test_output_csv,
            column_mapping=self.column_mapping,
            output_dir=self.temp_dir
        )
        
        # 测试城市匹配
        result = self.matcher.match_city(5332921, 'Los Angeles')
        self.assertIsNotNone(result)
        self.assertEqual(result, 5368361)
        
        print("城市匹配测试通过")

if __name__ == '__main__':
    unittest.main(verbosity=2)