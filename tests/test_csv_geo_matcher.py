#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV地理匹配功能测试模块
提供全面的测试用例验证CSV地理匹配功能的正确性
"""

import unittest
import os
import sys
import tempfile
import pandas as pd
import sqlite3
from unittest.mock import patch, MagicMock

# 添加src目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from csv_geo_matcher import CSVGeoMatcher
from sqlite_integrator import SQLiteIntegrator

class TestCSVGeoMatcher(unittest.TestCase):
    """
    CSV地理匹配器测试类
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
    
    def tearDown(self):
        """
        测试后的清理
        """
        import shutil
        import time
        
        # 确保所有数据库连接都已关闭
        try:
            if hasattr(self, 'matcher') and self.matcher and hasattr(self.matcher, 'sqlite_integrator'):
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
                # 如果仍然无法删除，等待更长时间再试
                time.sleep(0.5)
                try:
                    shutil.rmtree(self.temp_dir)
                except:
                    # 如果还是失败，记录但不抛出异常
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
        
        # 插入测试数据
        states_data = [
            (5332921, 'California', 'US', 'CA', 39538223),
            (4736286, 'Texas', 'US', 'TX', 29145505),
            (1816670, 'Beijing', 'CN', '11', 21540000),
            (1809858, 'Shanghai', 'CN', '31', 24870895)
        ]
        
        cities_data = [
            (5368361, 'Los Angeles', 'US', 'CA', 3971883),
            (4699066, 'Houston', 'US', 'TX', 2320268),
            (1816670, 'Beijing', 'CN', '11', 21540000),
            (1796236, 'Shanghai', 'CN', '31', 24870895),
            (5391959, 'San Francisco', 'US', 'CA', 881549)
        ]
        
        cursor.executemany('INSERT INTO states VALUES (?, ?, ?, ?, ?)', states_data)
        cursor.executemany('INSERT INTO cities VALUES (?, ?, ?, ?, ?)', cities_data)
        
        conn.commit()
        conn.close()
    
    def _create_test_csv_files(self):
        """
        创建测试CSV文件
        """
        # 正常测试数据
        normal_data = [
            {'country_code': 'US', 'state_name': 'California', 'city_name': 'Los Angeles'},
            {'country_code': 'US', 'state_name': 'Texas', 'city_name': 'Houston'},
            {'country_code': 'CN', 'state_name': 'Beijing', 'city_name': 'Beijing'},
            {'country_code': 'US', 'state_name': 'California', 'city_name': 'San Francisco'}
        ]
        
        df = pd.DataFrame(normal_data)
        df.to_csv(self.test_input_csv, index=False)
        
        # 创建包含边界情况的测试文件
        edge_case_data = [
            {'country_code': 'US', 'state_name': 'california', 'city_name': 'los angeles'},  # 小写
            {'country_code': 'US', 'state_name': 'TEXAS', 'city_name': 'HOUSTON'},  # 大写
            {'country_code': 'US', 'state_name': '', 'city_name': 'Unknown City'},  # 空州名
            {'country_code': '', 'state_name': 'California', 'city_name': 'Los Angeles'},  # 空国家代码
            {'country_code': 'XX', 'state_name': 'Unknown State', 'city_name': 'Unknown City'},  # 不存在的数据
        ]
        
        edge_df = pd.DataFrame(edge_case_data)
        edge_csv_path = os.path.join(self.temp_dir, 'test_edge_cases.csv')
        edge_df.to_csv(edge_csv_path, index=False)
        self.test_edge_csv = edge_csv_path
    
    def test_initialization_success(self):
        """
        测试正常初始化
        """
        self.matcher = CSVGeoMatcher(
            db_path=self.test_db_path,
            input_csv=self.test_input_csv,
            output_csv=self.test_output_csv,
            column_mapping=self.column_mapping,
            output_dir=self.temp_dir
        )
        
        self.assertEqual(self.matcher.db_path, self.test_db_path)
        self.assertEqual(self.matcher.input_csv, self.test_input_csv)
        self.assertEqual(self.matcher.output_csv, self.test_output_csv)
        self.assertEqual(self.matcher.column_mapping, self.column_mapping)
    
    def test_initialization_missing_db(self):
        """
        测试数据库文件不存在的情况
        """
        with self.assertRaises(FileNotFoundError):
            CSVGeoMatcher(
                db_path='/nonexistent/path/db.sqlite',
                input_csv=self.test_input_csv,
                column_mapping=self.column_mapping
            )
    
    def test_initialization_missing_csv(self):
        """
        测试输入CSV文件不存在的情况
        """
        with self.assertRaises(FileNotFoundError):
            CSVGeoMatcher(
                db_path=self.test_db_path,
                input_csv='/nonexistent/path/input.csv',
                column_mapping=self.column_mapping
            )
    
    def test_initialization_invalid_column_mapping(self):
        """
        测试无效的列名映射
        """
        invalid_mapping = {'country_code': 'country'}  # 缺少必需的键
        
        with self.assertRaises(ValueError):
            CSVGeoMatcher(
                db_path=self.test_db_path,
                input_csv=self.test_input_csv,
                column_mapping=invalid_mapping
            )
    
    def test_load_csv_data(self):
        """
        测试CSV数据加载
        """
        self.matcher = CSVGeoMatcher(
            db_path=self.test_db_path,
            input_csv=self.test_input_csv,
            output_csv=self.test_output_csv,
            column_mapping=self.column_mapping
        )
        
        data = self.matcher.load_csv_data()
        
        self.assertIsInstance(data, pd.DataFrame)
        self.assertEqual(len(data), 4)
        self.assertIn('country_code', data.columns)
        self.assertIn('state_name', data.columns)
        self.assertIn('city_name', data.columns)
    
    def test_match_state_success(self):
        """
        测试州匹配成功
        """
        self.matcher = CSVGeoMatcher(
            db_path=self.test_db_path,
            input_csv=self.test_input_csv,
            output_csv=self.test_output_csv,
            column_mapping=self.column_mapping
        )
        
        result = self.matcher.match_state('US', 'California')
        
        self.assertIsNotNone(result)
        self.assertEqual(result['geonameid'], 5332921)
        self.assertEqual(result['name'], 'California')
    
    def test_match_state_case_insensitive(self):
        """
        测试州匹配大小写不敏感
        """
        matcher = CSVGeoMatcher(
            db_path=self.test_db_path,
            input_csv=self.test_input_csv,
            column_mapping=self.column_mapping
        )
        
        result = matcher.match_state('US', 'california')
        
        self.assertIsNotNone(result)
        self.assertEqual(result['geonameid'], 5332921)
    
    def test_match_state_not_found(self):
        """
        测试州匹配失败
        """
        matcher = CSVGeoMatcher(
            db_path=self.test_db_path,
            input_csv=self.test_input_csv,
            column_mapping=self.column_mapping
        )
        
        result = matcher.match_state('US', 'NonexistentState')
        
        self.assertIsNone(result)
    
    def test_match_city_success(self):
        """
        测试城市匹配成功
        """
        matcher = CSVGeoMatcher(
            db_path=self.test_db_path,
            input_csv=self.test_input_csv,
            column_mapping=self.column_mapping
        )
        
        result = matcher.match_city('US', 'CA', 'Los Angeles')
        
        self.assertIsNotNone(result)
        self.assertEqual(result['geonameid'], 5368361)
        self.assertEqual(result['name'], 'Los Angeles')
    
    def test_match_city_population_priority(self):
        """
        测试城市匹配按人口优先
        """
        # 添加同名城市测试数据
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        # 插入另一个同名但人口较少的城市
        cursor.execute(
            'INSERT INTO cities VALUES (?, ?, ?, ?, ?)',
            (9999999, 'Los Angeles', 'US', 'CA', 100000)
        )
        conn.commit()
        conn.close()
        
        matcher = CSVGeoMatcher(
            db_path=self.test_db_path,
            input_csv=self.test_input_csv,
            column_mapping=self.column_mapping
        )
        
        result = matcher.match_city('US', 'CA', 'Los Angeles')
        
        # 应该返回人口更多的城市
        self.assertEqual(result['geonameid'], 5368361)
        self.assertEqual(result['population'], 3971883)
    
    def test_process_csv_file_success(self):
        """
        测试完整的CSV文件处理
        """
        self.matcher = CSVGeoMatcher(
            db_path=self.test_db_path,
            input_csv=self.test_input_csv,
            output_csv=self.test_output_csv,
            column_mapping=self.column_mapping,
            output_dir=self.temp_dir
        )
        
        result = self.matcher.process_csv_file()
        
        self.assertTrue(result['success'])
        self.assertEqual(result['total_records'], 4)
        self.assertGreater(result['successful_matches'], 0)
        self.assertTrue(os.path.exists(self.test_output_csv))
        
        # 验证输出文件内容
        output_df = pd.read_csv(self.test_output_csv)
        self.assertIn('state_geonameid', output_df.columns)
        self.assertIn('city_geonameid', output_df.columns)
    
    def test_edge_cases_handling(self):
        """
        测试边界情况处理
        """
        matcher = CSVGeoMatcher(
            db_path=self.test_db_path,
            input_csv=self.test_edge_csv,
            column_mapping=self.column_mapping,
            output_dir=self.temp_dir
        )
        
        result = matcher.process_csv_file()
        
        self.assertTrue(result['success'])
        self.assertEqual(result['total_records'], 5)
        
        # 检查统计信息
        self.assertGreater(matcher.detailed_stats['data_quality']['empty_values'], 0)
        self.assertGreater(len(matcher.failed_records), 0)
    
    def test_custom_column_mapping(self):
        """
        测试自定义列名映射
        """
        # 创建使用不同列名的CSV文件
        custom_data = [
            {'country': 'US', 'state': 'California', 'city': 'Los Angeles'},
            {'country': 'US', 'state': 'Texas', 'city': 'Houston'}
        ]
        
        custom_csv = os.path.join(self.temp_dir, 'custom_columns.csv')
        pd.DataFrame(custom_data).to_csv(custom_csv, index=False)
        
        custom_mapping = {
            'country_code': 'country',
            'state_name': 'state',
            'city_name': 'city'
        }
        
        matcher = CSVGeoMatcher(
            db_path=self.test_db_path,
            input_csv=custom_csv,
            column_mapping=custom_mapping,
            output_dir=self.temp_dir
        )
        
        result = matcher.process_csv_file()
        
        self.assertTrue(result['success'])
        self.assertEqual(result['total_records'], 2)
    
    def test_generate_detailed_report(self):
        """
        测试详细报告生成
        """
        matcher = CSVGeoMatcher(
            db_path=self.test_db_path,
            input_csv=self.test_input_csv,
            column_mapping=self.column_mapping,
            output_dir=self.temp_dir
        )
        
        # 先处理数据
        matcher.process_csv_file()
        
        # 生成报告
        report = matcher.generate_detailed_match_report()
        
        self.assertIn('summary', report)
        self.assertIn('state_matching', report)
        self.assertIn('city_matching', report)
        self.assertIn('data_quality', report)
        self.assertIn('performance', report)
        self.assertIn('recommendations', report)
    
    def test_export_failed_records(self):
        """
        测试失败记录导出
        """
        matcher = CSVGeoMatcher(
            db_path=self.test_db_path,
            input_csv=self.test_edge_csv,
            column_mapping=self.column_mapping,
            output_dir=self.temp_dir
        )
        
        # 处理包含失败记录的数据
        matcher.process_csv_file()
        
        # 导出失败记录
        failed_file = matcher.export_failed_records()
        
        self.assertTrue(os.path.exists(failed_file))
        
        # 验证失败记录文件内容
        failed_df = pd.read_csv(failed_file)
        self.assertGreater(len(failed_df), 0)
        self.assertIn('failure_type', failed_df.columns)
        self.assertIn('failure_reason', failed_df.columns)
    
    def test_performance_metrics(self):
        """
        测试性能指标收集
        """
        matcher = CSVGeoMatcher(
            db_path=self.test_db_path,
            input_csv=self.test_input_csv,
            column_mapping=self.column_mapping,
            output_dir=self.temp_dir
        )
        
        result = matcher.process_csv_file()
        
        self.assertTrue(result['success'])
        self.assertGreater(result['processing_time'], 0)
        self.assertGreater(matcher.detailed_stats['performance']['database_queries'], 0)
    
    def test_error_handling_invalid_csv(self):
        """
        测试无效CSV文件的错误处理
        """
        # 创建无效的CSV文件
        invalid_csv = os.path.join(self.temp_dir, 'invalid.csv')
        with open(invalid_csv, 'w') as f:
            f.write('invalid,csv,content\n')
            f.write('missing,columns\n')
        
        matcher = CSVGeoMatcher(
            db_path=self.test_db_path,
            input_csv=invalid_csv,
            column_mapping=self.column_mapping,
            output_dir=self.temp_dir
        )
        
        with self.assertRaises(Exception):
            matcher.load_csv_data()

class TestCSVGeoMatcherIntegration(unittest.TestCase):
    """
    CSV地理匹配器集成测试类
    """
    
    def test_end_to_end_workflow(self):
        """
        测试端到端工作流程
        """
        # 这个测试需要真实的数据库文件
        # 在实际环境中运行
        pass

def create_test_data_files():
    """
    创建测试数据文件
    """
    test_data_dir = os.path.join(os.path.dirname(__file__), 'test_data')
    os.makedirs(test_data_dir, exist_ok=True)
    
    # 创建样本输入CSV文件
    sample_data = [
        {'country_code': 'US', 'state_name': 'California', 'city_name': 'Los Angeles', 'additional_data': 'Sample 1'},
        {'country_code': 'US', 'state_name': 'Texas', 'city_name': 'Houston', 'additional_data': 'Sample 2'},
        {'country_code': 'CN', 'state_name': 'Beijing', 'city_name': 'Beijing', 'additional_data': 'Sample 3'},
        {'country_code': 'US', 'state_name': 'California', 'city_name': 'San Francisco', 'additional_data': 'Sample 4'},
        {'country_code': 'CN', 'state_name': 'Shanghai', 'city_name': 'Shanghai', 'additional_data': 'Sample 5'}
    ]
    
    sample_csv_path = os.path.join(test_data_dir, 'sample_input.csv')
    pd.DataFrame(sample_data).to_csv(sample_csv_path, index=False)
    
    print(f"测试数据文件已创建: {sample_csv_path}")
    
    return sample_csv_path

if __name__ == '__main__':
    # 创建测试数据文件
    create_test_data_files()
    
    # 运行测试
    unittest.main(verbosity=2)