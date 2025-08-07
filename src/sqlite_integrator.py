#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite数据库集成模块

将CSV数据导入SQLite数据库，建立索引和外键约束，支持高效的重复检查查询。
"""

import os
import sqlite3
import sqlalchemy
import pandas as pd
import logging
import math
from typing import Optional, List, Dict, Any, Tuple

class SQLiteIntegrator:
    """SQLite数据库集成器"""
    
    def __init__(self, db_path: str = 'sqlite_output/geonames.db'):
        """初始化SQLite集成器
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        self.engine = None
        
        # 确保输出目录存在
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
            self.logger.info(f"创建数据库目录: {db_dir}")
    
    def get_engine(self) -> sqlalchemy.engine.Engine:
        """获取数据库连接
        
        Returns:
            sqlite3.Connection: 数据库连接对象
        """
        # conn = sqlite3.connect(self.db_path)
        # # 启用外键约束
        # conn.execute("PRAGMA foreign_keys = ON")
        # return conn
        if self.engine is None:
            self.engine = sqlalchemy.create_engine('sqlite:///%s' % self.db_path)
        return self.engine
    
    def get_connection(self) -> sqlite3.Connection:
        return self.get_engine().raw_connection()
    
    def create_schema(self) -> bool:
        """创建数据库表结构
        
        Returns:
            bool: 创建是否成功
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 创建states表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS states (
                    geonameid BIGINT,
                    name TEXT,
                    asciiname TEXT,
                    alternatenames TEXT,
                    latitude FLOAT,
                    longitude FLOAT,
                    feature_class TEXT,
                    feature_code TEXT,
                    country_code TEXT,
                    cc2 TEXT,
                    admin1_code TEXT,
                    admin2_code TEXT,
                    admin3_code TEXT,
                    admin4_code TEXT,
                    population BIGINT,
                    elevation FLOAT,
                    dem BIGINT,
                    timezone TEXT,
                    modification_date TEXT
                )
            """)
            
            # 创建cities表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cities (
                    geonameid BIGINT,
                    name TEXT,
                    asciiname TEXT,
                    alternatenames TEXT,
                    latitude FLOAT,
                    longitude FLOAT,
                    feature_class TEXT,
                    feature_code TEXT,
                    country_code TEXT,
                    cc2 TEXT,
                    admin1_code TEXT,
                    admin2_code TEXT,
                    admin3_code TEXT,
                    admin4_code TEXT,
                    population BIGINT,
                    elevation FLOAT,
                    dem BIGINT,
                    timezone TEXT,
                    modification_date TEXT
                )
            """)
            
            # 创建state_names表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS state_names (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    country_code TEXT NOT NULL COLLATE NOCASE,
                    name TEXT NOT NULL COLLATE NOCASE,
                    admin1_code TEXT NOT NULL,
                    geonameid INTEGER,
                    FOREIGN KEY (geonameid) REFERENCES states(geonameid)
                )
            """)
            
            # 创建city_names表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS city_names (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    country_code TEXT NOT NULL COLLATE NOCASE,
                    admin1_code TEXT NOT NULL,
                    state_geonameid INTEGER,
                    name TEXT NOT NULL COLLATE NOCASE,
                    geonameid INTEGER,
                    FOREIGN KEY (geonameid) REFERENCES cities(geonameid)
                )
            """)
            
            # 创建csc_mapping表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS csc_mapping (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    state_id INTEGER,
                    state_code TEXT,
                    state_name TEXT,
                    country_id INTEGER,
                    country_code TEXT,
                    country_name TEXT,
                    latitude TEXT,
                    longitude TEXT,
                    wikiDataId TEXT,
                    geonameid INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (geonameid) REFERENCES cities(geonameid)
                )
            """)
            
            conn.commit()
            # conn.close()
            
            self.logger.info("数据库表结构创建成功")
            return True
            
        except Exception as e:
            self.logger.error(f"创建数据库表结构时出错: {e}")
            return False
    
    def create_indexes(self) -> bool:
        """创建数据库索引
        
        Returns:
            bool: 创建是否成功
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 为state_names表创建查询索引
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_state_names_lookup 
                ON state_names(country_code, name)
            """)
            
            # 为city_names表创建查询索引
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_city_names_lookup 
                ON city_names(country_code, state_geonameid, name)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_city_names_lookup2 
                ON city_names(state_geonameid, name)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_city_names_lookup3 
                ON city_names(country_code, admin1_code, name)
            """)
            
            # 为重复检查创建额外索引
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_state_names_duplicate 
                ON state_names(country_code, name)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_city_names_duplicate 
                ON city_names(country_code, state_geonameid, name)
            """)
            
            # 为states表创建country_code索引
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_states_country 
                ON states(country_code)
            """)
            
            # 为cities表创建country_code和admin1_code索引
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_cities_country_admin1 
                ON cities(country_code, admin1_code)
            """)
            
            # 为csc_mapping表创建索引
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_csc_mapping_wikidata 
                ON csc_mapping(wikiDataId)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_csc_mapping_geonameid 
                ON csc_mapping(geonameid)
            """)
            
            conn.commit()
            # conn.close()
            
            self.logger.info("数据库索引创建成功")
            return True
            
        except Exception as e:
            self.logger.error(f"创建数据库索引时出错: {e}")
            return False
    
    def import_csv_data(self, csv_dir: str = 'csv_output') -> bool:
        """导入CSV数据到数据库
        
        Args:
            csv_dir: CSV文件目录
            
        Returns:
            bool: 导入是否成功
        """
        try:
            if not os.path.exists(csv_dir):
                self.logger.error(f"CSV目录不存在: {csv_dir}")
                return False
            
            conn = self.get_engine()
            
            # 导入states表
            states_file = os.path.join(csv_dir, 'states.csv')
            if os.path.exists(states_file):
                states_df = pd.read_csv(states_file, na_values=[], keep_default_na=False)
                states_df.to_sql('states', conn, if_exists='append', index=False)
                self.logger.info(f"导入{len(states_df)}条states记录")
            else:
                self.logger.warning(f"states.csv文件不存在: {states_file}")
            
            # 导入cities表
            cities_file = os.path.join(csv_dir, 'cities.csv')
            if os.path.exists(cities_file):
                cities_df = pd.read_csv(cities_file, na_values=[], keep_default_na=False)
                cities_df.to_sql('cities', conn, if_exists='append', index=False)
                self.logger.info(f"导入{len(cities_df)}条cities记录")
                del cities_df
            else:
                self.logger.warning(f"cities.csv文件不存在: {cities_file}")
            
            # 导入state_names表
            state_names_file = os.path.join(csv_dir, 'state_names.csv')
            if os.path.exists(state_names_file):
                state_names_df = pd.read_csv(state_names_file, na_values=[], keep_default_na=False)
                state_names_df.to_sql('state_names', conn, if_exists='append', index=False)
                self.logger.info(f"导入{len(state_names_df)}条state_names记录")
            else:
                self.logger.warning(f"state_names.csv文件不存在: {state_names_file}")
            
            # 导入city_names表
            city_names_file = os.path.join(csv_dir, 'city_names.csv')
            if os.path.exists(city_names_file):
                city_names_df = pd.read_csv(city_names_file, na_values=[], keep_default_na=False)
                city_names_df.to_sql('city_names', conn, if_exists='append', index=False)
                self.logger.info(f"导入{len(city_names_df)}条city_names记录")
            else:
                self.logger.warning(f"city_names.csv文件不存在: {city_names_file}")
            
            # 导入csc_mapping表
            csc_mapping_file = os.path.join(csv_dir, 'csc_mapping.csv')
            if os.path.exists(csc_mapping_file):
                csc_mapping_df = pd.read_csv(csc_mapping_file, na_values=[], keep_default_na=False)
                csc_mapping_df.to_sql('csc_mapping', conn, if_exists='append', index=False)
                self.logger.info(f"导入{len(csc_mapping_df)}条csc_mapping记录")
                del csc_mapping_df
            else:
                self.logger.warning(f"csc_mapping.csv文件不存在: {csc_mapping_file}")
            
            # conn.close()
            
            self.logger.info("CSV数据导入完成")
            return True
            
        except Exception as e:
            self.logger.error(f"导入CSV数据时出错: {e}")
            return False
    
    def setup_database(self, csv_dir: str = 'csv_output') -> bool:
        """设置完整的数据库（创建表结构、导入数据、创建索引）
        
        Args:
            csv_dir: CSV文件目录
            
        Returns:
            bool: 设置是否成功
        """
        try:
            self.logger.info("开始设置SQLite数据库...")
            
            # 1. 创建表结构
            if not self.create_schema():
                return False
            
            # 2. 导入CSV数据
            if not self.import_csv_data(csv_dir):
                return False
            
            # 3. 创建索引
            if not self.create_indexes():
                return False
            
            self.logger.info("SQLite数据库设置完成")
            return True
            
        except Exception as e:
            self.logger.error(f"设置数据库时出错: {e}")
            return False

    def get_database_stats(self) -> Dict[str, Any]:
        """获取数据库统计信息
        
        Returns:
            dict: 数据库统计信息
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            stats = {
                'database_path': self.db_path,
                'database_exists': os.path.exists(self.db_path),
                'tables': {}
            }
            
            if stats['database_exists']:
                stats['database_size_bytes'] = os.path.getsize(self.db_path)
                
                # 获取各表的记录数
                tables = ['states', 'cities', 'state_names', 'city_names']
                for table in tables:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        stats['tables'][table] = {'record_count': count}
                    except:
                        stats['tables'][table] = {'record_count': 0, 'error': 'Table not found'}
            
            # conn.close()
            return stats
            
        except Exception as e:
            self.logger.error(f"获取数据库统计信息时出错: {e}")
            return {'error': str(e)}

    def validate_database(self) -> bool:
        """验证数据库完整性
        
        Returns:
            bool: 验证是否通过
        """
        try:
            if not os.path.exists(self.db_path):
                self.logger.error("数据库文件不存在")
                return False
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 检查表是否存在
            required_tables = ['states', 'cities', 'state_names', 'city_names']
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            existing_tables = [row[0] for row in cursor.fetchall()]
            
            missing_tables = [table for table in required_tables if table not in existing_tables]
            if missing_tables:
                self.logger.error(f"缺少表: {missing_tables}")
                return False
            
            # 检查每个表是否有数据
            for table in required_tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                if count == 0:
                    self.logger.warning(f"表{table}为空")
                else:
                    self.logger.info(f"表{table}包含{count}条记录")
            
            # 检查索引是否存在
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
            indexes = [row[0] for row in cursor.fetchall()]
            expected_indexes = ['idx_state_names_lookup', 'idx_city_names_lookup']
            
            for index in expected_indexes:
                if index in indexes:
                    self.logger.info(f"索引{index}存在")
                else:
                    self.logger.warning(f"索引{index}不存在")
            
            # conn.close()
            
            self.logger.info("数据库验证通过")
            return True
            
        except Exception as e:
            self.logger.error(f"验证数据库时出错: {e}")
            return False
    
    def query_cities_by_admin_codes(self, country_code: str, admin1_code: str, admin2_code: str = None) -> List[Dict[str, Any]]:
        """根据adminCode查询cities表中的记录
        
        Args:
            country_code: 国家代码
            admin1_code: 一级行政区代码
            admin2_code: 二级行政区代码（可选）
            
        Returns:
            List[Dict]: 匹配的城市记录列表
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if admin2_code:
                # 精确匹配：country + admin1 + admin2
                query = """
                    SELECT geonameid, name, asciiname, country_code, admin1_code, admin2_code
                    FROM cities 
                    WHERE country_code = ? AND admin1_code = ? AND admin2_code = ?
                """
                cursor.execute(query, (country_code, admin1_code, admin2_code))
            else:
                # 部分匹配：country + admin1
                query = """
                    SELECT geonameid, name, asciiname, country_code, admin1_code, admin2_code
                    FROM cities 
                    WHERE country_code = ? AND admin1_code = ?
                """
                cursor.execute(query, (country_code, admin1_code))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'geonameid': row[0],
                    'name': row[1],
                    'asciiname': row[2],
                    'country_code': row[3],
                    'admin1_code': row[4],
                    'admin2_code': row[5]
                })
            
            # conn.close()
            return results
            
        except Exception as e:
            self.logger.error(f"查询cities表时出错: {e}")
            return []
    
    def calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """计算两点间的地理距离（公里）
        
        Args:
            lat1, lon1: 第一个点的经纬度
            lat2, lon2: 第二个点的经纬度
            
        Returns:
            float: 距离（公里）
        """
        try:
            # 使用Haversine公式计算距离
            R = 6371  # 地球半径（公里）
            
            lat1_rad = math.radians(lat1)
            lat2_rad = math.radians(lat2)
            delta_lat = math.radians(lat2 - lat1)
            delta_lon = math.radians(lon2 - lon1)
            
            a = (math.sin(delta_lat/2) * math.sin(delta_lat/2) + 
                 math.cos(lat1_rad) * math.cos(lat2_rad) * 
                 math.sin(delta_lon/2) * math.sin(delta_lon/2))
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
            
            return R * c
            
        except Exception as e:
            self.logger.error(f"计算距离时出错: {e}")
            return float('inf')
    
    def match_csc_to_cities(self, csc_record: Dict[str, Any], admin_codes: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """将CSC记录匹配到cities表中的geonameid
        
        Args:
            csc_record: CSC记录，包含id, name, latitude, longitude等
            admin_codes: 从Geonames获取的admin代码，包含admin1_code, admin2_code
            
        Returns:
            Optional[Dict]: 匹配结果，包含matched_geonameid, confidence_score, match_method
        """
        try:
            country_code = csc_record.get('country_code', '')
            admin1_code = admin_codes.get('admin1_code', '')
            admin2_code = admin_codes.get('admin2_code', '')
            csc_lat = float(csc_record.get('latitude', 0))
            csc_lon = float(csc_record.get('longitude', 0))
            
            # 策略1：精确匹配 country + admin1 + admin2
            if admin2_code:
                candidates = self.query_cities_by_admin_codes(country_code, admin1_code, admin2_code)
                if candidates:
                    # 如果只有一个候选，直接返回
                    if len(candidates) == 1:
                        return {
                            'matched_geonameid': candidates[0]['geonameid'],
                            'confidence_score': 0.95,
                            'match_method': 'exact_admin_codes'
                        }
                    
                    # 多个候选时，使用地理距离验证
                    best_match = None
                    min_distance = float('inf')
                    
                    for candidate in candidates:
                        # 这里需要从数据库获取候选城市的经纬度
                        # 暂时使用名称匹配作为辅助
                        if candidate['name'].lower() == csc_record.get('name', '').lower():
                            return {
                                'matched_geonameid': candidate['geonameid'],
                                'confidence_score': 0.90,
                                'match_method': 'exact_admin_codes_name_match'
                            }
                    
                    # 返回第一个匹配（需要进一步优化）
                    return {
                        'matched_geonameid': candidates[0]['geonameid'],
                        'confidence_score': 0.75,
                        'match_method': 'exact_admin_codes_first'
                    }
            
            # 策略2：部分匹配 country + admin1
            if admin1_code:
                candidates = self.query_cities_by_admin_codes(country_code, admin1_code)
                if candidates:
                    # 使用名称匹配
                    for candidate in candidates:
                        if candidate['name'].lower() == csc_record.get('name', '').lower():
                            return {
                                'matched_geonameid': candidate['geonameid'],
                                'confidence_score': 0.70,
                                'match_method': 'partial_admin_codes_name_match'
                            }
                    
                    # 返回第一个匹配（置信度较低）
                    return {
                        'matched_geonameid': candidates[0]['geonameid'],
                        'confidence_score': 0.50,
                        'match_method': 'partial_admin_codes_first'
                    }
            
            # 无法匹配
            return None
            
        except Exception as e:
            self.logger.error(f"匹配CSC记录时出错: {e}")
            return None
    
    def validate_mapping_accuracy(self, mappings: List[Dict[str, Any]]) -> Dict[str, Any]:
        """验证映射准确性
        
        Args:
            mappings: 映射结果列表
            
        Returns:
            Dict: 映射质量报告
        """
        try:
            total_records = len(mappings)
            if total_records == 0:
                return {
                    'total_records': 0,
                    'matched_records': 0,
                    'match_rate': 0.0,
                    'confidence_distribution': {},
                    'method_distribution': {}
                }
            
            matched_records = len([m for m in mappings if m.get('matched_geonameid')])
            match_rate = matched_records / total_records
            
            # 置信度分布
            confidence_ranges = {
                'high (>0.8)': 0,
                'medium (0.6-0.8)': 0,
                'low (<0.6)': 0
            }
            
            # 匹配方法分布
            method_distribution = {}
            
            for mapping in mappings:
                if mapping.get('matched_geonameid'):
                    confidence = mapping.get('confidence_score', 0)
                    method = mapping.get('match_method', 'unknown')
                    
                    # 统计置信度分布
                    if confidence > 0.8:
                        confidence_ranges['high (>0.8)'] += 1
                    elif confidence >= 0.6:
                        confidence_ranges['medium (0.6-0.8)'] += 1
                    else:
                        confidence_ranges['low (<0.6)'] += 1
                    
                    # 统计方法分布
                    method_distribution[method] = method_distribution.get(method, 0) + 1
            
            return {
                'total_records': total_records,
                'matched_records': matched_records,
                'unmatched_records': total_records - matched_records,
                'match_rate': round(match_rate, 4),
                'confidence_distribution': confidence_ranges,
                'method_distribution': method_distribution
            }
            
        except Exception as e:
            self.logger.error(f"验证映射准确性时出错: {e}")
            return {'error': str(e)}
    
    def get_admin_code_mapping_stats(self) -> Dict[str, Any]:
        """获取adminCode映射统计信息
        
        Returns:
            Dict: 映射统计信息
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            stats = {
                'cities_with_admin1': 0,
                'cities_with_admin2': 0,
                'cities_total': 0,
                'unique_admin1_codes': 0,
                'unique_admin2_codes': 0
            }
            
            # 统计cities表中的admin code情况
            cursor.execute("SELECT COUNT(*) FROM cities")
            stats['cities_total'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM cities WHERE admin1_code IS NOT NULL AND admin1_code != ''")
            stats['cities_with_admin1'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM cities WHERE admin2_code IS NOT NULL AND admin2_code != ''")
            stats['cities_with_admin2'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT admin1_code) FROM cities WHERE admin1_code IS NOT NULL AND admin1_code != ''")
            stats['unique_admin1_codes'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT admin2_code) FROM cities WHERE admin2_code IS NOT NULL AND admin2_code != ''")
            stats['unique_admin2_codes'] = cursor.fetchone()[0]
            
            # conn.close()
            return stats
            
        except Exception as e:
            self.logger.error(f"获取adminCode映射统计信息时出错: {e}")
            return {'error': str(e)}


def test_sqlite_integrator():
    """测试SQLite集成模块"""
    print("=== SQLite集成模块测试 ===")
    
    # 配置日志
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    # 创建SQLite集成器
    integrator = SQLiteIntegrator('test_sqlite_output/test_geonames.db')
    
    # 测试创建表结构
    print("\n1. 测试创建表结构:")
    schema_result = integrator.create_schema()
    print(f"表结构创建: {schema_result}")
    
    # 测试导入CSV数据（使用测试数据）
    print("\n2. 测试导入CSV数据:")
    # import_result = integrator.import_csv_data('test_csv_output')
    import_result = integrator.import_csv_data('output/csv_output')
    print(f"数据导入: {import_result}")
    
    # 测试创建索引
    print("\n3. 测试创建索引:")
    index_result = integrator.create_indexes()
    print(f"索引创建: {index_result}")
    
    # 测试数据库验证
    print("\n4. 测试数据库验证:")
    validation_result = integrator.validate_database()
    print(f"数据库验证: {validation_result}")
    
    # 获取数据库统计信息
    print("\n5. 数据库统计信息:")
    stats = integrator.get_database_stats()
    print(f"数据库路径: {stats.get('database_path')}")
    print(f"数据库存在: {stats.get('database_exists')}")
    if 'database_size_bytes' in stats:
        print(f"数据库大小: {stats['database_size_bytes']}字节")
    
    for table, info in stats.get('tables', {}).items():
        print(f"  {table}: {info.get('record_count', 0)}条记录")
    
    # 测试完整设置流程
    print("\n6. 测试完整设置流程:")
    integrator2 = SQLiteIntegrator('test_sqlite_output/complete_test.db')
    # setup_result = integrator2.setup_database('test_csv_output')
    setup_result = integrator2.setup_database('output/csv_output')
    print(f"完整设置: {setup_result}")


if __name__ == "__main__":
    test_sqlite_integrator()