#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite数据库集成模块

将CSV数据导入SQLite数据库，建立索引和外键约束，支持高效的重复检查查询。
"""

import os
import sqlite3
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
        
        # 确保输出目录存在
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
            self.logger.info(f"创建数据库目录: {db_dir}")
    
    def get_connection(self) -> sqlite3.Connection:
        """获取数据库连接
        
        Returns:
            sqlite3.Connection: 数据库连接对象
        """
        conn = sqlite3.connect(self.db_path)
        # 启用外键约束
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    
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
                    geonameid INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    asciiname TEXT,
                    country_code TEXT NOT NULL,
                    admin1_code TEXT
                )
            """)
            
            # 创建cities表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cities (
                    geonameid INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    asciiname TEXT,
                    country_code TEXT NOT NULL,
                    admin1_code TEXT,
                    admin2_code TEXT
                )
            """)
            
            # 创建state_names表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS state_names (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    country_code TEXT NOT NULL,
                    name TEXT NOT NULL,
                    geonameid INTEGER,
                    FOREIGN KEY (geonameid) REFERENCES states(geonameid)
                )
            """)
            
            # 创建city_names表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS city_names (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    country_code TEXT NOT NULL,
                    state_geonameid INTEGER,
                    name TEXT NOT NULL,
                    geonameid INTEGER,
                    FOREIGN KEY (geonameid) REFERENCES cities(geonameid)
                )
            """)
            
            # 创建csc_cities表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS csc_cities (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    state_id INTEGER,
                    state_code TEXT,
                    state_name TEXT,
                    country_id INTEGER,
                    country_code TEXT,
                    country_name TEXT,
                    latitude REAL,
                    longitude REAL,
                    wikiDataId TEXT,
                    matched_geonameid INTEGER,
                    match_confidence REAL,
                    match_method TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (matched_geonameid) REFERENCES cities(geonameid)
                )
            """)
            
            # 创建csc_mapping表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS csc_mapping (
                    csc_id TEXT PRIMARY KEY,
                    wikidata_id TEXT,
                    geonameid INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (geonameid) REFERENCES cities(geonameid)
                )
            """)
            
            conn.commit()
            conn.close()
            
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
            
            # 为csc_cities表创建索引
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_csc_country_code 
                ON csc_cities(country_code)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_csc_matched_geonameid 
                ON csc_cities(matched_geonameid)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_csc_wikidata 
                ON csc_cities(wikiDataId)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_csc_name 
                ON csc_cities(name)
            """)
            
            # 为csc_mapping表创建索引
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_csc_mapping_wikidata 
                ON csc_mapping(wikidata_id)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_csc_mapping_geonameid 
                ON csc_mapping(geonameid)
            """)
            
            conn.commit()
            conn.close()
            
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
            
            conn = self.get_connection()
            
            # 导入states表
            states_file = os.path.join(csv_dir, 'states.csv')
            if os.path.exists(states_file):
                states_df = pd.read_csv(states_file)
                states_df.to_sql('states', conn, if_exists='replace', index=False)
                self.logger.info(f"导入{len(states_df)}条states记录")
            else:
                self.logger.warning(f"states.csv文件不存在: {states_file}")
            
            # 导入cities表
            cities_file = os.path.join(csv_dir, 'cities.csv')
            if os.path.exists(cities_file):
                cities_df = pd.read_csv(cities_file)
                cities_df.to_sql('cities', conn, if_exists='replace', index=False)
                self.logger.info(f"导入{len(cities_df)}条cities记录")
            else:
                self.logger.warning(f"cities.csv文件不存在: {cities_file}")
            
            # 导入state_names表
            state_names_file = os.path.join(csv_dir, 'state_names.csv')
            if os.path.exists(state_names_file):
                state_names_df = pd.read_csv(state_names_file)
                state_names_df.to_sql('state_names', conn, if_exists='replace', index=False)
                self.logger.info(f"导入{len(state_names_df)}条state_names记录")
            else:
                self.logger.warning(f"state_names.csv文件不存在: {state_names_file}")
            
            # 导入city_names表
            city_names_file = os.path.join(csv_dir, 'city_names.csv')
            if os.path.exists(city_names_file):
                city_names_df = pd.read_csv(city_names_file)
                city_names_df.to_sql('city_names', conn, if_exists='replace', index=False)
                self.logger.info(f"导入{len(city_names_df)}条city_names记录")
            else:
                self.logger.warning(f"city_names.csv文件不存在: {city_names_file}")
            
            conn.close()
            
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
    
    def insert_csc_mapping(self, csc_mapping_data: pd.DataFrame) -> bool:
        """插入CSC映射数据到csc_mapping表
        
        Args:
            csc_mapping_data: CSC映射数据列表，每个元素包含csc_id, wikidata_id, geonameid
            
        Returns:
            bool: 插入是否成功
        """
        try:
            if not csc_mapping_data.empty:
                self.logger.warning("CSC映射数据为空，跳过插入")
                return True
                
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 准备插入语句
            insert_sql = """
                INSERT OR REPLACE INTO csc_mapping (csc_id, wikidata_id, geonameid, created_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """
            
            # 批量插入数据
            insert_data = []
            for mapping in csc_mapping_data:
                csc_id = mapping.get('csc_id')
                wikidata_id = mapping.get('wikidata_id')
                geonameid = mapping.get('geonameid')
                
                # 验证必要字段
                if not csc_id:
                    self.logger.warning(f"跳过无效记录：缺少csc_id - {mapping}")
                    continue
                    
                insert_data.append((csc_id, wikidata_id, geonameid))
            
            if insert_data:
                cursor.executemany(insert_sql, insert_data)
                conn.commit()
                self.logger.info(f"成功插入{len(insert_data)}条CSC映射记录")
            else:
                self.logger.warning("没有有效的CSC映射数据可插入")
            
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"插入CSC映射数据时出错: {e}", exc_info=True)
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
            
            conn.close()
            return stats
            
        except Exception as e:
            self.logger.error(f"获取数据库统计信息时出错: {e}")
            return {'error': str(e)}
    
    def import_csc_city_names(self, city_names: List[Dict[str, Any]]) -> bool:
        """
        导入CSC城市名称到city_names表
        
        Args:
            city_names: city_names记录列表
            
        Returns:
            bool: 导入是否成功
        """
        try:
            if not city_names:
                self.logger.warning("没有CSC城市名称数据需要导入")
                return True
            
            conn = self.get_connection()
            
            # 转换为DataFrame
            df = pd.DataFrame(city_names)
            
            # 导入到city_names表（追加模式）
            df.to_sql('city_names', conn, if_exists='append', index=False)
            
            conn.close()
            
            self.logger.info(f"成功导入{len(city_names)}条CSC城市名称记录")
            return True
            
        except Exception as e:
            self.logger.error(f"导入CSC城市名称时出错: {e}")
            return False
    
    def import_csc_state_names(self, state_names: List[Dict[str, Any]]) -> bool:
        """
        导入CSC州/省名称到state_names表
        
        Args:
            state_names: state_names记录列表
            
        Returns:
            bool: 导入是否成功
        """
        try:
            if not state_names:
                self.logger.warning("没有CSC州/省名称数据需要导入")
                return True
            
            conn = self.get_connection()
            
            # 转换为DataFrame
            df = pd.DataFrame(state_names)
            
            # 导入到state_names表（追加模式）
            df.to_sql('state_names', conn, if_exists='append', index=False)
            
            conn.close()
            
            self.logger.info(f"成功导入{len(state_names)}条CSC州/省名称记录")
            return True
            
        except Exception as e:
            self.logger.error(f"导入CSC州/省名称时出错: {e}")
            return False
    
    def validate_csc_names_import(self) -> Dict[str, Any]:
        """
        验证CSC名称导入结果
        
        Returns:
            Dict: 验证结果报告
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            validation_report = {
                'city_names_total': 0,
                'state_names_total': 0,
                'csc_city_names': 0,
                'csc_state_names': 0,
                'countries_with_csc_cities': 0,
                'countries_with_csc_states': 0,
                'data_quality': {}
            }
            
            # 统计city_names表总记录数
            cursor.execute("SELECT COUNT(*) FROM city_names")
            validation_report['city_names_total'] = cursor.fetchone()[0]
            
            # 统计state_names表总记录数
            cursor.execute("SELECT COUNT(*) FROM state_names")
            validation_report['state_names_total'] = cursor.fetchone()[0]
            
            # 统计来自CSC的city_names记录（通过geonameid关联csc_cities表）
            cursor.execute("""
                SELECT COUNT(*) FROM city_names cn
                WHERE cn.geonameid IN (SELECT matched_geonameid FROM csc_cities WHERE matched_geonameid IS NOT NULL)
            """)
            validation_report['csc_city_names'] = cursor.fetchone()[0]
            
            # 统计来自CSC的state_names记录（通过geonameid关联csc_cities的state_id）
            cursor.execute("""
                SELECT COUNT(*) FROM state_names sn
                WHERE sn.geonameid IN (SELECT DISTINCT state_id FROM csc_cities WHERE state_id IS NOT NULL)
            """)
            validation_report['csc_state_names'] = cursor.fetchone()[0]
            
            # 统计有CSC城市的国家数
            cursor.execute("""
                SELECT COUNT(DISTINCT cn.country_code) FROM city_names cn
                WHERE cn.geonameid IN (SELECT matched_geonameid FROM csc_cities WHERE matched_geonameid IS NOT NULL)
            """)
            validation_report['countries_with_csc_cities'] = cursor.fetchone()[0]
            
            # 统计有CSC州/省的国家数
            cursor.execute("""
                SELECT COUNT(DISTINCT sn.country_code) FROM state_names sn
                WHERE sn.geonameid IN (SELECT DISTINCT state_id FROM csc_cities WHERE state_id IS NOT NULL)
            """)
            validation_report['countries_with_csc_states'] = cursor.fetchone()[0]
            
            # 数据质量检查
            cursor.execute("SELECT COUNT(*) FROM city_names WHERE name IS NULL OR name = ''")
            empty_city_names = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM state_names WHERE name IS NULL OR name = ''")
            empty_state_names = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM city_names WHERE country_code IS NULL OR country_code = ''")
            missing_city_country_codes = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM state_names WHERE country_code IS NULL OR country_code = ''")
            missing_state_country_codes = cursor.fetchone()[0]
            
            validation_report['data_quality'] = {
                'empty_city_names': empty_city_names,
                'empty_state_names': empty_state_names,
                'missing_city_country_codes': missing_city_country_codes,
                'missing_state_country_codes': missing_state_country_codes
            }
            
            conn.close()
            return validation_report
            
        except Exception as e:
            self.logger.error(f"验证CSC名称导入时出错: {e}")
            return {'error': str(e)}
    
    def get_csc_names_stats(self) -> Dict[str, Any]:
        """
        获取CSC名称统计信息
        
        Returns:
            Dict: CSC名称统计信息
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            stats = {
                'csc_city_names_by_country': [],
                'csc_state_names_by_country': [],
                'top_csc_cities': [],
                'top_csc_states': [],
                'mapping_coverage': {}
            }
            
            # CSC城市名称按国家统计
            cursor.execute("""
                SELECT cn.country_code, COUNT(*) as count
                FROM city_names cn
                WHERE cn.geonameid IN (SELECT matched_geonameid FROM csc_cities WHERE matched_geonameid IS NOT NULL)
                GROUP BY cn.country_code
                ORDER BY count DESC
                LIMIT 20
            """)
            stats['csc_city_names_by_country'] = [
                {'country_code': row[0], 'count': row[1]} for row in cursor.fetchall()
            ]
            
            # CSC州/省名称按国家统计
            cursor.execute("""
                SELECT sn.country_code, COUNT(*) as count
                FROM state_names sn
                WHERE sn.geonameid IN (SELECT DISTINCT state_id FROM csc_cities WHERE state_id IS NOT NULL)
                GROUP BY sn.country_code
                ORDER BY count DESC
                LIMIT 20
            """)
            stats['csc_state_names_by_country'] = [
                {'country_code': row[0], 'count': row[1]} for row in cursor.fetchall()
            ]
            
            # 前20个CSC城市（按名称变体数量）
            cursor.execute("""
                SELECT cn.name, COUNT(*) as variant_count
                FROM city_names cn
                WHERE cn.geonameid IN (SELECT matched_geonameid FROM csc_cities WHERE matched_geonameid IS NOT NULL)
                GROUP BY cn.geonameid
                ORDER BY variant_count DESC
                LIMIT 20
            """)
            stats['top_csc_cities'] = [
                {'name': row[0], 'variant_count': row[1]} for row in cursor.fetchall()
            ]
            
            # 前20个CSC州/省（按名称变体数量）
            cursor.execute("""
                SELECT sn.name, COUNT(*) as variant_count
                FROM state_names sn
                WHERE sn.geonameid IN (SELECT DISTINCT state_id FROM csc_cities WHERE state_id IS NOT NULL)
                GROUP BY sn.geonameid
                ORDER BY variant_count DESC
                LIMIT 20
            """)
            stats['top_csc_states'] = [
                {'name': row[0], 'variant_count': row[1]} for row in cursor.fetchall()
            ]
            
            # 映射覆盖率
            cursor.execute("SELECT COUNT(*) FROM csc_cities")
            total_csc_cities = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM csc_cities WHERE matched_geonameid IS NOT NULL")
            mapped_csc_cities = cursor.fetchone()[0]
            
            if total_csc_cities > 0:
                stats['mapping_coverage'] = {
                    'total_csc_cities': total_csc_cities,
                    'mapped_csc_cities': mapped_csc_cities,
                    'mapping_rate': round(mapped_csc_cities / total_csc_cities, 4)
                }
            
            conn.close()
            return stats
            
        except Exception as e:
            self.logger.error(f"获取CSC名称统计信息时出错: {e}")
            return {'error': str(e)}
    
    def import_csc_data(self, csc_data: pd.DataFrame, mappings: List[Dict[str, Any]] = None) -> bool:
        """导入CSC数据到数据库
        
        Args:
            csc_data: CSC数据DataFrame
            mappings: 映射结果列表（可选）
            
        Returns:
            bool: 导入是否成功
        """
        try:
            conn = self.get_connection()
            
            # 准备数据
            import_data = csc_data.copy()
            
            # 添加映射信息
            if mappings:
                mapping_dict = {m.get('csc_id'): m for m in mappings if m.get('matched_geonameid')}
                
                import_data['matched_geonameid'] = import_data['id'].map(
                    lambda x: mapping_dict.get(x, {}).get('matched_geonameid')
                )
                import_data['match_confidence'] = import_data['id'].map(
                    lambda x: mapping_dict.get(x, {}).get('confidence_score')
                )
                import_data['match_method'] = import_data['id'].map(
                    lambda x: mapping_dict.get(x, {}).get('match_method')
                )
            else:
                import_data['matched_geonameid'] = None
                import_data['match_confidence'] = None
                import_data['match_method'] = None
            
            # 导入到csc_cities表
            import_data.to_sql('csc_cities', conn, if_exists='replace', index=False)
            
            conn.close()
            
            self.logger.info(f"成功导入{len(import_data)}条CSC记录")
            return True
            
        except Exception as e:
            self.logger.error(f"导入CSC数据时出错: {e}")
            return False
    
    def create_csc_indexes(self) -> bool:
        """为CSC表创建索引
        
        Returns:
            bool: 创建是否成功
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 创建CSC表的索引
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_csc_country_code ON csc_cities(country_code)",
                "CREATE INDEX IF NOT EXISTS idx_csc_matched_geonameid ON csc_cities(matched_geonameid)",
                "CREATE INDEX IF NOT EXISTS idx_csc_wikidata ON csc_cities(wikiDataId)",
                "CREATE INDEX IF NOT EXISTS idx_csc_name ON csc_cities(name)",
                "CREATE INDEX IF NOT EXISTS idx_csc_state_code ON csc_cities(state_code)",
                "CREATE INDEX IF NOT EXISTS idx_csc_coordinates ON csc_cities(latitude, longitude)"
            ]
            
            for index_sql in indexes:
                cursor.execute(index_sql)
            
            conn.commit()
            conn.close()
            
            self.logger.info("CSC表索引创建成功")
            return True
            
        except Exception as e:
            self.logger.error(f"创建CSC表索引时出错: {e}")
            return False
    
    def validate_csc_import(self) -> Dict[str, Any]:
        """验证CSC数据导入结果
        
        Returns:
            Dict: 验证结果报告
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            validation_report = {
                'table_exists': False,
                'total_records': 0,
                'records_with_mapping': 0,
                'mapping_rate': 0.0,
                'unique_countries': 0,
                'unique_states': 0,
                'data_quality': {},
                'foreign_key_violations': 0
            }
            
            # 检查表是否存在
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='csc_cities'")
            if cursor.fetchone():
                validation_report['table_exists'] = True
                
                # 统计总记录数
                cursor.execute("SELECT COUNT(*) FROM csc_cities")
                validation_report['total_records'] = cursor.fetchone()[0]
                
                # 统计有映射的记录数
                cursor.execute("SELECT COUNT(*) FROM csc_cities WHERE matched_geonameid IS NOT NULL")
                validation_report['records_with_mapping'] = cursor.fetchone()[0]
                
                # 计算映射率
                if validation_report['total_records'] > 0:
                    validation_report['mapping_rate'] = round(
                        validation_report['records_with_mapping'] / validation_report['total_records'], 4
                    )
                
                # 统计唯一国家数
                cursor.execute("SELECT COUNT(DISTINCT country_code) FROM csc_cities")
                validation_report['unique_countries'] = cursor.fetchone()[0]
                
                # 统计唯一州/省数
                cursor.execute("SELECT COUNT(DISTINCT state_code) FROM csc_cities WHERE state_code IS NOT NULL")
                validation_report['unique_states'] = cursor.fetchone()[0]
                
                # 数据质量检查
                cursor.execute("SELECT COUNT(*) FROM csc_cities WHERE name IS NULL OR name = ''")
                empty_names = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM csc_cities WHERE latitude IS NULL OR longitude IS NULL")
                missing_coordinates = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM csc_cities WHERE wikiDataId IS NULL OR wikiDataId = ''")
                missing_wikidata = cursor.fetchone()[0]
                
                validation_report['data_quality'] = {
                    'empty_names': empty_names,
                    'missing_coordinates': missing_coordinates,
                    'missing_wikidata': missing_wikidata
                }
                
                # 检查外键约束违反
                cursor.execute("""
                    SELECT COUNT(*) FROM csc_cities 
                    WHERE matched_geonameid IS NOT NULL 
                    AND matched_geonameid NOT IN (SELECT geonameid FROM cities)
                """)
                validation_report['foreign_key_violations'] = cursor.fetchone()[0]
            
            conn.close()
            return validation_report
            
        except Exception as e:
            self.logger.error(f"验证CSC导入时出错: {e}")
            return {'error': str(e)}
    
    def get_csc_stats(self) -> Dict[str, Any]:
        """获取CSC表统计信息
        
        Returns:
            Dict: CSC表统计信息
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            stats = {
                'total_records': 0,
                'mapped_records': 0,
                'unmapped_records': 0,
                'mapping_methods': {},
                'confidence_distribution': {},
                'top_countries': [],
                'top_states': []
            }
            
            # 基本统计
            cursor.execute("SELECT COUNT(*) FROM csc_cities")
            stats['total_records'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM csc_cities WHERE matched_geonameid IS NOT NULL")
            stats['mapped_records'] = cursor.fetchone()[0]
            
            stats['unmapped_records'] = stats['total_records'] - stats['mapped_records']
            
            # 映射方法分布
            cursor.execute("""
                SELECT match_method, COUNT(*) 
                FROM csc_cities 
                WHERE match_method IS NOT NULL 
                GROUP BY match_method
            """)
            for method, count in cursor.fetchall():
                stats['mapping_methods'][method] = count
            
            # 置信度分布
            cursor.execute("""
                SELECT 
                    CASE 
                        WHEN match_confidence > 0.8 THEN 'high'
                        WHEN match_confidence >= 0.6 THEN 'medium'
                        ELSE 'low'
                    END as confidence_level,
                    COUNT(*)
                FROM csc_cities 
                WHERE match_confidence IS NOT NULL 
                GROUP BY confidence_level
            """)
            for level, count in cursor.fetchall():
                stats['confidence_distribution'][level] = count
            
            # 前10个国家
            cursor.execute("""
                SELECT country_name, COUNT(*) as count
                FROM csc_cities 
                GROUP BY country_name 
                ORDER BY count DESC 
                LIMIT 10
            """)
            stats['top_countries'] = [{'country': row[0], 'count': row[1]} for row in cursor.fetchall()]
            
            # 前10个州/省
            cursor.execute("""
                SELECT state_name, COUNT(*) as count
                FROM csc_cities 
                WHERE state_name IS NOT NULL
                GROUP BY state_name 
                ORDER BY count DESC 
                LIMIT 10
            """)
            stats['top_states'] = [{'state': row[0], 'count': row[1]} for row in cursor.fetchall()]
            
            conn.close()
            return stats
            
        except Exception as e:
            self.logger.error(f"获取CSC统计信息时出错: {e}")
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
            
            conn.close()
            
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
            
            conn.close()
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
            
            conn.close()
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
    import_result = integrator.import_csv_data('test_csv_output')
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
    setup_result = integrator2.setup_database('test_csv_output')
    print(f"完整设置: {setup_result}")


if __name__ == "__main__":
    test_sqlite_integrator()