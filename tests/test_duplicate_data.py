#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创建包含重复数据的测试数据库，用于验证重复检查功能
"""

import sqlite3
import os
from duplicate_checker import DuplicateChecker

def create_test_database_with_duplicates():
    """创建包含重复数据的测试数据库"""
    db_path = 'test_duplicate_db.db'
    
    # 删除已存在的数据库
    if os.path.exists(db_path):
        os.remove(db_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 创建state_names表
    cursor.execute('''
        CREATE TABLE state_names (
            country_code TEXT NOT NULL,
            name TEXT NOT NULL,
            geonameid INTEGER NOT NULL
        )
    ''')
    
    # 创建city_names表
    cursor.execute('''
        CREATE TABLE city_names (
            country_code TEXT NOT NULL,
            state_geonameid INTEGER NOT NULL,
            name TEXT NOT NULL,
            geonameid INTEGER NOT NULL
        )
    ''')
    
    # 插入state_names测试数据（包含重复项）
    state_names_data = [
        ('US', 'California', 1001),
        ('US', 'California', 1002),  # 重复：US + California
        ('US', 'Texas', 1003),
        ('US', 'New York', 1004),
        ('US', 'New York', 1005),   # 重复：US + New York
        ('US', 'New York', 1006),   # 重复：US + New York (三重复)
        ('CA', 'Ontario', 2001),
        ('CA', 'Quebec', 2002),
        ('CA', 'Ontario', 2003),    # 重复：CA + Ontario
        ('CN', 'Beijing', 3001),
        ('CN', 'Shanghai', 3002),
    ]
    
    cursor.executemany('INSERT INTO state_names VALUES (?, ?, ?)', state_names_data)
    
    # 插入city_names测试数据（包含重复项）
    city_names_data = [
        ('US', 1001, 'Los Angeles', 4001),
        ('US', 1001, 'San Francisco', 4002),
        ('US', 1001, 'Los Angeles', 4003),  # 重复：US + 1001 + Los Angeles
        ('US', 1003, 'Houston', 4004),
        ('US', 1003, 'Dallas', 4005),
        ('US', 1003, 'Houston', 4006),      # 重复：US + 1003 + Houston
        ('US', 1003, 'Houston', 4007),      # 重复：US + 1003 + Houston (三重复)
        ('US', 1004, 'New York City', 4008),
        ('CA', 2001, 'Toronto', 5001),
        ('CA', 2001, 'Ottawa', 5002),
        ('CA', 2001, 'Toronto', 5003),      # 重复：CA + 2001 + Toronto
        ('CN', 3001, 'Beijing', 6001),
        ('CN', 3002, 'Shanghai', 6002),
    ]
    
    cursor.executemany('INSERT INTO city_names VALUES (?, ?, ?, ?)', city_names_data)
    
    conn.commit()
    conn.close()
    
    print(f"测试数据库已创建: {db_path}")
    print(f"State names 记录数: {len(state_names_data)}")
    print(f"City names 记录数: {len(city_names_data)}")
    
    return db_path

def test_duplicate_detection():
    """测试重复检测功能"""
    print("=" * 60)
    print("测试重复检测功能")
    print("=" * 60)
    
    # 创建包含重复数据的测试数据库
    db_path = create_test_database_with_duplicates()
    
    try:
        # 创建重复检查器
        checker = DuplicateChecker(db_path)
        
        # 检查state_names重复项
        print("\n1. 检查state_names重复项:")
        state_dups = checker.check_state_name_duplicates()
        print(f"发现 {len(state_dups)} 个重复组")
        if len(state_dups) > 0:
            print(state_dups.to_string(index=False))
        
        # 检查city_names重复项
        print("\n2. 检查city_names重复项:")
        city_dups = checker.check_city_name_duplicates()
        print(f"发现 {len(city_dups)} 个重复组")
        if len(city_dups) > 0:
            print(city_dups.to_string(index=False))
        
        # 生成统计信息
        print("\n3. 统计信息:")
        stats = checker.get_duplicate_statistics()
        print(f"State Names: {stats['state_names']['total_records']} 总记录, {stats['state_names']['duplicate_groups']} 重复组, {stats['state_names']['duplicate_rate']:.2f}% 重复率")
        print(f"City Names: {stats['city_names']['total_records']} 总记录, {stats['city_names']['duplicate_groups']} 重复组, {stats['city_names']['duplicate_rate']:.2f}% 重复率")
        
        # 生成详细报告
        print("\n4. 生成详细报告...")
        report_file = 'test_duplicate_detection_report.txt'
        checker.generate_duplicate_report(report_file)
        print(f"✓ 详细报告已生成: {report_file}")
        
        print("\n=" * 60)
        print("重复检测功能测试完成")
        print("=" * 60)
        
    except Exception as e:
        print(f"测试过程中出错: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 清理测试数据库
        if os.path.exists(db_path):
            os.remove(db_path)
            print(f"\n测试数据库已清理: {db_path}")

if __name__ == "__main__":
    test_duplicate_detection()