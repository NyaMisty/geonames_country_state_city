#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV导出模块

生成用户需求的四个CSV文件：
- states.csv: 存储ADM1级别的行政区数据
- cities.csv: 存储ADM2级别的行政区数据
- state_names.csv: 存储state名称映射
- city_names.csv: 存储city名称映射
"""

import os
import pandas as pd
import logging
from typing import Optional

class CSVExporter:
    """CSV导出器"""
    
    def __init__(self, output_dir: str = 'csv_output'):
        """初始化CSV导出器
        
        Args:
            output_dir: 输出目录路径
        """
        self.output_dir = output_dir
        self.logger = logging.getLogger(__name__)
        
        # 确保输出目录存在
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            self.logger.info(f"创建输出目录: {self.output_dir}")
    
    def export_states(self, states_df: pd.DataFrame, filename: str = 'states.csv') -> bool:
        """
        导出states表到CSV文件（包含完整的geonames原始信息）
        
        Args:
            states_df: states数据DataFrame
            filename: 输出文件名
            
        Returns:
            bool: 导出是否成功
        """
        try:
            if states_df.empty:
                self.logger.warning("States数据为空，跳过导出")
                return False
            
            # GeoNames完整字段列表（19个字段）
            all_columns = [
                'geonameid', 'name', 'asciiname', 'alternatenames',
                'latitude', 'longitude', 'feature_class', 'feature_code',
                'country_code', 'cc2', 'admin1_code', 'admin2_code',
                'admin3_code', 'admin4_code', 'population', 'elevation',
                'dem', 'timezone', 'modification_date'
            ]
            
            # 检查可用字段
            available_columns = [col for col in all_columns if col in states_df.columns]
            missing_columns = [col for col in all_columns if col not in states_df.columns]
            
            if missing_columns:
                self.logger.warning(f"States数据缺少字段: {missing_columns}，将导出可用字段")
            
            # 选择可用字段并保持原始顺序
            export_df = states_df[available_columns].copy()
            
            # 导出到CSV
            output_path = os.path.join(self.output_dir, filename)
            export_df.to_csv(output_path, index=False, encoding='utf-8')
            
            self.logger.info(f"成功导出{len(export_df)}条states记录到 {output_path}（包含{len(available_columns)}个字段）")
            return True
            
        except Exception as e:
            self.logger.error(f"导出states数据时出错: {e}")
            return False
    
    def export_cities(self, cities_df: pd.DataFrame, filename: str = 'cities.csv') -> bool:
        """
        导出cities表到CSV文件（包含完整的geonames原始信息）
        
        Args:
            cities_df: cities数据DataFrame
            filename: 输出文件名
            
        Returns:
            bool: 导出是否成功
        """
        try:
            if cities_df.empty:
                self.logger.warning("Cities数据为空，跳过导出")
                return False
            
            # GeoNames完整字段列表（19个字段）
            all_columns = [
                'geonameid', 'name', 'asciiname', 'alternatenames',
                'latitude', 'longitude', 'feature_class', 'feature_code',
                'country_code', 'cc2', 'admin1_code', 'admin2_code',
                'admin3_code', 'admin4_code', 'population', 'elevation',
                'dem', 'timezone', 'modification_date'
            ]
            
            # 检查可用字段
            available_columns = [col for col in all_columns if col in cities_df.columns]
            missing_columns = [col for col in all_columns if col not in cities_df.columns]
            
            if missing_columns:
                self.logger.warning(f"Cities数据缺少字段: {missing_columns}，将导出可用字段")
            
            # 选择可用字段并保持原始顺序
            export_df = cities_df[available_columns].copy()
            
            # 导出到CSV
            output_path = os.path.join(self.output_dir, filename)
            export_df.to_csv(output_path, index=False, encoding='utf-8')
            
            self.logger.info(f"成功导出{len(export_df)}条cities记录到 {output_path}（包含{len(available_columns)}个字段）")
            return True
            
        except Exception as e:
            self.logger.error(f"导出cities数据时出错: {e}")
            return False
    
    def export_state_names(self, state_names_df: pd.DataFrame, filename: str = 'state_names.csv') -> bool:
        """导出state_names表到CSV文件
        
        Args:
            state_names_df: state_names数据DataFrame
            filename: 输出文件名
            
        Returns:
            bool: 导出是否成功
        """
        try:
            if state_names_df.empty:
                self.logger.warning("State_names数据为空，跳过导出")
                return False
            
            # 确保字段顺序正确
            expected_columns = ['country_code', 'name', 'geonameid']
            missing_columns = [col for col in expected_columns if col not in state_names_df.columns]
            if missing_columns:
                self.logger.error(f"State_names数据缺少必要字段: {missing_columns}")
                return False
            
            # 选择并排序字段
            export_df = state_names_df[expected_columns].copy()
            
            # 导出到CSV
            output_path = os.path.join(self.output_dir, filename)
            export_df.to_csv(output_path, index=False, encoding='utf-8')
            
            self.logger.info(f"成功导出{len(export_df)}条state_names记录到 {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"导出state_names数据时出错: {e}")
            return False
    
    def export_city_names(self, city_names_df: pd.DataFrame, filename: str = 'city_names.csv') -> bool:
        """导出city_names表到CSV文件
        
        Args:
            city_names_df: city_names数据DataFrame
            filename: 输出文件名
            
        Returns:
            bool: 导出是否成功
        """
        try:
            if city_names_df.empty:
                self.logger.warning("City_names数据为空，跳过导出")
                return False
            
            # 确保字段顺序正确
            expected_columns = ['country_code', 'state_geonameid', 'name', 'geonameid']
            missing_columns = [col for col in expected_columns if col not in city_names_df.columns]
            if missing_columns:
                self.logger.error(f"City_names数据缺少必要字段: {missing_columns}")
                return False
            
            # 选择并排序字段
            export_df = city_names_df[expected_columns].copy()
            
            # 导出到CSV
            output_path = os.path.join(self.output_dir, filename)
            export_df.to_csv(output_path, index=False, encoding='utf-8')
            
            self.logger.info(f"成功导出{len(export_df)}条city_names记录到 {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"导出city_names数据时出错: {e}")
            return False
    
    def export_all(self, states_df: pd.DataFrame, cities_df: pd.DataFrame, 
                   state_names_df: pd.DataFrame, city_names_df: pd.DataFrame) -> bool:
        """导出所有表到CSV文件
        
        Args:
            states_df: states数据DataFrame
            cities_df: cities数据DataFrame
            state_names_df: state_names数据DataFrame
            city_names_df: city_names数据DataFrame
            
        Returns:
            bool: 所有导出是否成功
        """
        try:
            self.logger.info("开始导出所有CSV文件...")
            
            results = []
            results.append(self.export_states(states_df))
            results.append(self.export_cities(cities_df))
            results.append(self.export_state_names(state_names_df))
            results.append(self.export_city_names(city_names_df))
            
            success_count = sum(results)
            total_count = len(results)
            
            if success_count == total_count:
                self.logger.info(f"所有{total_count}个CSV文件导出成功")
                return True
            else:
                self.logger.warning(f"CSV导出完成，成功{success_count}/{total_count}个文件")
                return False
                
        except Exception as e:
            self.logger.error(f"批量导出CSV文件时出错: {e}")
            return False
    
    def get_export_summary(self) -> dict:
        """获取导出文件的摘要信息
        
        Returns:
            dict: 导出文件摘要
        """
        summary = {
            'output_directory': self.output_dir,
            'files': []
        }
        
        expected_files = ['states.csv', 'cities.csv', 'state_names.csv', 'city_names.csv']
        
        for filename in expected_files:
            filepath = os.path.join(self.output_dir, filename)
            if os.path.exists(filepath):
                file_size = os.path.getsize(filepath)
                # 简单计算行数（减去表头）
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        line_count = sum(1 for _ in f) - 1  # 减去表头
                except:
                    line_count = 'unknown'
                
                summary['files'].append({
                    'filename': filename,
                    'exists': True,
                    'size_bytes': file_size,
                    'record_count': line_count
                })
            else:
                summary['files'].append({
                    'filename': filename,
                    'exists': False,
                    'size_bytes': 0,
                    'record_count': 0
                })
        
        return summary
    
    def validate_exports(self) -> bool:
        """验证导出的CSV文件
        
        Returns:
            bool: 验证是否通过
        """
        try:
            expected_files = ['states.csv', 'cities.csv', 'state_names.csv', 'city_names.csv']
            
            for filename in expected_files:
                filepath = os.path.join(self.output_dir, filename)
                
                if not os.path.exists(filepath):
                    self.logger.error(f"缺少文件: {filename}")
                    return False
                
                # 验证文件可以正常读取
                try:
                    df = pd.read_csv(filepath, encoding='utf-8')
                    if df.empty:
                        self.logger.warning(f"文件为空: {filename}")
                    else:
                        self.logger.info(f"验证通过: {filename} ({len(df)}条记录)")
                except Exception as e:
                    self.logger.error(f"文件读取失败: {filename} - {e}")
                    return False
            
            self.logger.info("所有CSV文件验证通过")
            return True
            
        except Exception as e:
            self.logger.error(f"验证CSV文件时出错: {e}")
            return False


def test_csv_exporter():
    """测试CSV导出模块"""
    print("=== CSV导出模块测试 ===")
    
    # 配置日志
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    # 创建测试数据
    states_data = pd.DataFrame([
        {'geonameid': 1001, 'name': 'California', 'asciiname': 'California', 
         'country_code': 'US', 'admin1_code': 'CA'},
        {'geonameid': 1002, 'name': 'Texas', 'asciiname': 'Texas', 
         'country_code': 'US', 'admin1_code': 'TX'}
    ])
    
    cities_data = pd.DataFrame([
        {'geonameid': 2001, 'name': 'Los Angeles County', 'asciiname': 'Los Angeles County', 
         'country_code': 'US', 'admin1_code': 'CA', 'admin2_code': '037'},
        {'geonameid': 2002, 'name': 'Harris County', 'asciiname': 'Harris County', 
         'country_code': 'US', 'admin1_code': 'TX', 'admin2_code': '201'}
    ])
    
    state_names_data = pd.DataFrame([
        {'country_code': 'US', 'name': 'California', 'geonameid': 1001},
        {'country_code': 'US', 'name': 'CA', 'geonameid': 1001},
        {'country_code': 'US', 'name': 'Texas', 'geonameid': 1002},
        {'country_code': 'US', 'name': 'TX', 'geonameid': 1002}
    ])
    
    city_names_data = pd.DataFrame([
        {'country_code': 'US', 'state_geonameid': 1001, 'name': 'Los Angeles County', 'geonameid': 2001},
        {'country_code': 'US', 'state_geonameid': 1001, 'name': 'LA County', 'geonameid': 2001},
        {'country_code': 'US', 'state_geonameid': 1002, 'name': 'Harris County', 'geonameid': 2002},
        {'country_code': 'US', 'state_geonameid': 1002, 'name': 'Harris Co.', 'geonameid': 2002}
    ])
    
    # 创建CSV导出器
    exporter = CSVExporter('test_csv_output')
    
    # 测试单独导出
    print("\n1. 测试单独导出:")
    print(f"States导出: {exporter.export_states(states_data)}")
    print(f"Cities导出: {exporter.export_cities(cities_data)}")
    print(f"State_names导出: {exporter.export_state_names(state_names_data)}")
    print(f"City_names导出: {exporter.export_city_names(city_names_data)}")
    
    # 测试批量导出
    print("\n2. 测试批量导出:")
    all_success = exporter.export_all(states_data, cities_data, state_names_data, city_names_data)
    print(f"批量导出结果: {all_success}")
    
    # 测试验证
    print("\n3. 测试文件验证:")
    validation_result = exporter.validate_exports()
    print(f"验证结果: {validation_result}")
    
    # 获取摘要
    print("\n4. 导出摘要:")
    summary = exporter.get_export_summary()
    print(f"输出目录: {summary['output_directory']}")
    for file_info in summary['files']:
        print(f"  {file_info['filename']}: 存在={file_info['exists']}, "
              f"大小={file_info['size_bytes']}字节, 记录数={file_info['record_count']}")


if __name__ == "__main__":
    test_csv_exporter()