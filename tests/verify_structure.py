#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据结构验证脚本
验证GeoNames数据文件结构和编码
"""

import os
import pandas as pd

def verify_file_structure(file_path):
    """
    验证文件结构是否符合GeoNames格式
    """
    print(f"验证文件: {file_path}")
    
    # 检查文件是否存在
    if not os.path.exists(file_path):
        print(f"错误: 文件不存在 {file_path}")
        return False
    
    try:
        # 验证UTF-8编码
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            
        # 验证字段数量
        fields = first_line.split('\t')
        print(f"字段数量: {len(fields)}")
        
        if len(fields) != 19:
            print(f"警告: 期望19个字段，实际得到{len(fields)}个字段")
            return False
            
        print("✓ 文件结构验证通过")
        return True
        
    except UnicodeDecodeError as e:
        print(f"编码错误: {e}")
        return False
    except Exception as e:
        print(f"验证失败: {e}")
        return False

def verify_sample_data(file_path):
    """
    验证样本数据中是否包含ADM1和ADM2记录
    """
    try:
        # 使用pandas读取前100行进行验证
        df = pd.read_csv(file_path, sep='\t', nrows=100, header=None, encoding='utf-8')
        
        # GeoNames字段名
        columns = [
            'geonameid', 'name', 'asciiname', 'alternatenames',
            'latitude', 'longitude', 'feature_class', 'feature_code',
            'country_code', 'cc2', 'admin1_code', 'admin2_code',
            'admin3_code', 'admin4_code', 'population', 'elevation',
            'dem', 'timezone', 'modification_date'
        ]
        
        df.columns = columns
        
        # 检查ADM1和ADM2记录
        adm1_count = len(df[df['feature_code'] == 'ADM1'])
        adm2_count = len(df[df['feature_code'] == 'ADM2'])
        
        print(f"样本数据中ADM1记录数: {adm1_count}")
        print(f"样本数据中ADM2记录数: {adm2_count}")
        
        if adm1_count > 0 or adm2_count > 0:
            print("✓ 样本数据包含ADM级别记录")
            return True
        else:
            print("警告: 样本数据中未找到ADM1或ADM2记录")
            return False
            
    except Exception as e:
        print(f"样本数据验证失败: {e}")
        return False

def setup_environment():
    """
    设置项目环境
    """
    print("设置项目环境...")
    
    # 创建输出目录
    directories = ['csv_output', 'sqlite_output']
    
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"✓ 创建目录: {directory}")
        else:
            print(f"✓ 目录已存在: {directory}")
    
    # 验证pandas安装
    try:
        import pandas as pd
        print(f"✓ pandas版本: {pd.__version__}")
    except ImportError:
        print("错误: pandas未安装")
        return False
    
    return True

def main():
    """
    主验证流程
    """
    print("=== GeoNames数据结构验证 ===")
    
    # 设置环境
    if not setup_environment():
        print("环境设置失败")
        return False
    
    # 验证样本文件
    sample_file = 'source_data/geoname/allCountries.txt'
    if verify_file_structure(sample_file):
        verify_sample_data(sample_file)
    
    print("\n=== 验证完成 ===")
    return True

if __name__ == '__main__':
    main()