#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GeoNames重复检查模块

该模块用于检查SQLite数据库中的重复alternate_name条目：
- state_names表中的重复项（country_code + name）
- city_names表中的重复项（country_code + state_geonameid + name）
"""

import sqlite3
import pandas as pd
import os
from typing import Dict, Any
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DuplicateChecker:
    """重复检查器类"""
    
    def __init__(self, db_path='sqlite_output/geonames.db'):
        """
        初始化重复检查器
        
        Args:
            db_path (str): SQLite数据库文件路径
        """
        self.db_path = db_path
        
        # 验证数据库文件是否存在
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"数据库文件不存在: {db_path}")
        
        logger.info(f"初始化重复检查器，数据库路径: {db_path}")
    
    def check_state_name_duplicates(self) -> pd.DataFrame:
        """
        检查state_names表中的重复项
        
        查找条件：country_code + name的重复
        
        Returns:
            pd.DataFrame: 包含重复项信息的DataFrame
        """
        logger.info("开始检查state_names表中的重复项")
        
        query = '''
        SELECT country_code, name
        FROM state_names 
        GROUP BY country_code, name 
        HAVING COUNT(*) > 1
        '''
        
        try:
            conn = sqlite3.connect(self.db_path)
            result = pd.read_sql_query(query, conn)
            conn.close()
            
            logger.info(f"发现 {len(result)} 个state_names重复项")
            return result
            
        except Exception as e:
            logger.error(f"检查state_names重复项时出错: {e}")
            raise
    
    def check_city_name_duplicates(self) -> pd.DataFrame:
        """
        检查city_names表中的重复项
        
        查找条件：country_code + state_geonameid + name的重复
        
        Returns:
            pd.DataFrame: 包含重复项信息的DataFrame
        """
        logger.info("开始检查city_names表中的重复项")
        
        query = '''
        SELECT country_code, state_geonameid, name
        FROM city_names 
        GROUP BY country_code, state_geonameid, name 
        HAVING COUNT(*) > 1
        '''
        
        try:
            conn = sqlite3.connect(self.db_path)
            result = pd.read_sql_query(query, conn)
            conn.close()
            
            logger.info(f"发现 {len(result)} 个city_names重复项")
            return result
            
        except Exception as e:
            logger.error(f"检查city_names重复项时出错: {e}")
            raise
    
    def get_duplicate_statistics(self) -> Dict[str, Any]:
        """
        获取重复项统计信息
        
        Returns:
            Dict[str, Any]: 包含统计信息的字典
        """
        logger.info("生成重复项统计信息")
        
        try:
            state_dups = self.check_state_name_duplicates()
            city_dups = self.check_city_name_duplicates()
            
            # 计算总记录数
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM state_names")
            total_state_names = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM city_names")
            total_city_names = cursor.fetchone()[0]
            
            conn.close()
            
            # 计算重复记录数
            # state_duplicate_records = state_dups['duplicate_count'].sum() if len(state_dups) > 0 else 0
            # city_duplicate_records = city_dups['duplicate_count'].sum() if len(city_dups) > 0 else 0
            state_duplicate_records = len(state_dups)
            city_duplicate_records = len(city_dups)
            
            statistics = {
                'state_names': {
                    'total_records': total_state_names,
                    'duplicate_groups': len(state_dups),
                    'duplicate_records': state_duplicate_records,
                    'duplicate_rate': (state_duplicate_records / total_state_names * 100) if total_state_names > 0 else 0
                },
                'city_names': {
                    'total_records': total_city_names,
                    'duplicate_groups': len(city_dups),
                    'duplicate_records': city_duplicate_records,
                    'duplicate_rate': (city_duplicate_records / total_city_names * 100) if total_city_names > 0 else 0
                }
            }
            
            logger.info("统计信息生成完成")
            return statistics
            
        except Exception as e:
            logger.error(f"生成统计信息时出错: {e}")
            raise
    
    def generate_duplicate_report(self, output_file='duplicate_report.txt') -> None:
        """
        生成详细的重复项报告
        
        Args:
            output_file (str): 输出文件路径
        """
        logger.info(f"开始生成重复项报告: {output_file}")
        
        try:
            # 获取重复项数据
            state_dups = self.check_state_name_duplicates()
            city_dups = self.check_city_name_duplicates()
            statistics = self.get_duplicate_statistics()
            
            # 创建输出目录
            os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else '.', exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write("GeoNames 重复项检查报告\n")
                f.write("=" * 80 + "\n\n")
                
                # 写入统计摘要
                f.write("## 统计摘要\n\n")
                f.write(f"State Names 表:\n")
                f.write(f"  - 总记录数: {statistics['state_names']['total_records']:,}\n")
                f.write(f"  - 重复组数: {statistics['state_names']['duplicate_groups']:,}\n")
                f.write(f"  - 重复记录数: {statistics['state_names']['duplicate_records']:,}\n")
                f.write(f"  - 重复率: {statistics['state_names']['duplicate_rate']:.2f}%\n\n")
                
                f.write(f"City Names 表:\n")
                f.write(f"  - 总记录数: {statistics['city_names']['total_records']:,}\n")
                f.write(f"  - 重复组数: {statistics['city_names']['duplicate_groups']:,}\n")
                f.write(f"  - 重复记录数: {statistics['city_names']['duplicate_records']:,}\n")
                f.write(f"  - 重复率: {statistics['city_names']['duplicate_rate']:.2f}%\n\n")
                
                # 写入state_names重复项详情
                f.write("=" * 80 + "\n")
                f.write(f"## State Names 重复项详情 ({len(state_dups)} 组)\n")
                f.write("=" * 80 + "\n\n")
                
                if len(state_dups) > 0:
                    f.write(state_dups.to_string(index=False))
                    f.write("\n\n")
                else:
                    f.write("未发现重复项\n\n")
                
                # 写入city_names重复项详情
                f.write("=" * 80 + "\n")
                f.write(f"## City Names 重复项详情 ({len(city_dups)} 组)\n")
                f.write("=" * 80 + "\n\n")
                
                if len(city_dups) > 0:
                    f.write(city_dups.to_string(index=False))
                    f.write("\n\n")
                else:
                    f.write("未发现重复项\n\n")
                
                f.write("=" * 80 + "\n")
                f.write("报告生成完成\n")
                f.write("=" * 80 + "\n")
            
            logger.info(f"重复项报告已生成: {output_file}")
            
        except Exception as e:
            logger.error(f"生成重复项报告时出错: {e}")
            raise
    
    def validate_database_structure(self) -> bool:
        """
        验证数据库结构是否正确
        
        Returns:
            bool: 验证是否通过
        """
        logger.info("验证数据库结构")
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 检查必需的表是否存在
            required_tables = ['state_names', 'city_names']
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            existing_tables = [row[0] for row in cursor.fetchall()]
            
            for table in required_tables:
                if table not in existing_tables:
                    logger.error(f"缺少必需的表: {table}")
                    conn.close()
                    return False
            
            # 检查state_names表结构
            cursor.execute("PRAGMA table_info(state_names)")
            state_names_columns = [row[1] for row in cursor.fetchall()]
            required_state_columns = ['country_code', 'name', 'geonameid']
            
            for col in required_state_columns:
                if col not in state_names_columns:
                    logger.error(f"state_names表缺少必需的列: {col}")
                    conn.close()
                    return False
            
            # 检查city_names表结构
            cursor.execute("PRAGMA table_info(city_names)")
            city_names_columns = [row[1] for row in cursor.fetchall()]
            required_city_columns = ['country_code', 'state_geonameid', 'name', 'geonameid']
            
            for col in required_city_columns:
                if col not in city_names_columns:
                    logger.error(f"city_names表缺少必需的列: {col}")
                    conn.close()
                    return False
            
            conn.close()
            logger.info("数据库结构验证通过")
            return True
            
        except Exception as e:
            logger.error(f"验证数据库结构时出错: {e}")
            return False

def test_duplicate_checker():
    """
    测试重复检查器功能
    """
    print("=" * 60)
    print("测试重复检查器")
    print("=" * 60)
    
    try:
        # 使用测试数据库
        test_db_path = 'test_sqlite_output/test_geonames.db'
        
        if not os.path.exists(test_db_path):
            print(f"测试数据库不存在: {test_db_path}")
            print("请先运行sqlite_integrator.py生成测试数据库")
            return
        
        # 创建重复检查器实例
        checker = DuplicateChecker(test_db_path)
        
        # 验证数据库结构
        print("\n1. 验证数据库结构...")
        if checker.validate_database_structure():
            print("✓ 数据库结构验证通过")
        else:
            print("✗ 数据库结构验证失败")
            return
        
        # 检查state_names重复项
        print("\n2. 检查state_names重复项...")
        state_dups = checker.check_state_name_duplicates()
        print(f"发现 {len(state_dups)} 个state_names重复组")
        if len(state_dups) > 0:
            print("前5个重复项:")
            print(state_dups.head().to_string(index=False))
        
        # 检查city_names重复项
        print("\n3. 检查city_names重复项...")
        city_dups = checker.check_city_name_duplicates()
        print(f"发现 {len(city_dups)} 个city_names重复组")
        if len(city_dups) > 0:
            print("前5个重复项:")
            print(city_dups.head().to_string(index=False))
        
        # 生成统计信息
        print("\n4. 生成统计信息...")
        stats = checker.get_duplicate_statistics()
        print(f"State Names: {stats['state_names']['duplicate_groups']} 重复组, {stats['state_names']['duplicate_rate']:.2f}% 重复率")
        print(f"City Names: {stats['city_names']['duplicate_groups']} 重复组, {stats['city_names']['duplicate_rate']:.2f}% 重复率")
        
        # 生成重复项报告
        print("\n5. 生成重复项报告...")
        report_file = 'test_duplicate_report.txt'
        checker.generate_duplicate_report(report_file)
        print(f"✓ 重复项报告已生成: {report_file}")
        
        print("\n=" * 60)
        print("重复检查器测试完成")
        print("=" * 60)
        
    except Exception as e:
        print(f"测试过程中出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_duplicate_checker()