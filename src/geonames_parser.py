#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GeoNames数据解析器模块
支持大文件分块读取和行政区数据筛选
"""

import pandas as pd
from typing import Iterator, Dict, List, Optional, Tuple
import logging
import os

class GeonamesParser:
    """
    GeoNames数据解析器
    支持分块读取大文件，筛选ADM1和ADM2级别的行政区数据
    """
    
    # GeoNames标准字段名（19个字段）
    FIELD_NAMES = [
        'geonameid', 'name', 'asciiname', 'alternatenames',
        'latitude', 'longitude', 'feature_class', 'feature_code',
        'country_code', 'cc2', 'admin1_code', 'admin2_code',
        'admin3_code', 'admin4_code', 'population', 'elevation',
        'dem', 'timezone', 'modification_date'
    ]
    
    # 目标行政区级别
    TARGET_FEATURE_CODES = ['ADM1', 'ADM2']
    
    def __init__(self, file_path: str, chunk_size: int = 10000):
        """
        初始化解析器
        
        Args:
            file_path: GeoNames数据文件路径
            chunk_size: 分块读取大小，默认10000行
        """
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.logger = logging.getLogger(__name__)
        
        # 验证文件路径
        import os
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"GeoNames数据文件不存在: {file_path}")
    
    def parse_chunks(self) -> Iterator[pd.DataFrame]:
        """
        分块解析GeoNames数据文件
        
        Yields:
            pd.DataFrame: 筛选后的行政区数据块
        """
        try:
            self.logger.info(f"开始分块读取文件: {self.file_path}")
            
            # 使用pandas分块读取
            chunk_reader = pd.read_csv(
                self.file_path,
                sep='\t',
                names=self.FIELD_NAMES,
                chunksize=self.chunk_size,
                encoding='utf-8',
                dtype={
                    'geonameid': 'int64',
                    'name': 'string',
                    'asciiname': 'string',
                    'alternatenames': 'string',
                    'latitude': 'float64',
                    'longitude': 'float64',
                    'feature_class': 'string',
                    'feature_code': 'string',
                    'country_code': 'string',
                    'cc2': 'string',
                    'admin1_code': 'string',
                    'admin2_code': 'string',
                    'admin3_code': 'string',
                    'admin4_code': 'string',
                    'population': 'Int64',  # 可空整数
                    'elevation': 'Int64',
                    'dem': 'Int64',
                    'timezone': 'string',
                    'modification_date': 'string'
                },
                na_values=['', 'NULL'],
                keep_default_na=False
            )
            
            chunk_count = 0
            total_records = 0
            filtered_records = 0
            
            for chunk in chunk_reader:
                chunk_count += 1
                total_records += len(chunk)
                
                # 筛选行政区记录
                filtered_chunk = self.filter_admin_records(chunk)
                filtered_records += len(filtered_chunk)
                
                if len(filtered_chunk) > 0:
                    self.logger.debug(f"块 {chunk_count}: 原始 {len(chunk)} 行，筛选后 {len(filtered_chunk)} 行")
                    yield filtered_chunk
                
            self.logger.info(f"解析完成: 总计 {chunk_count} 块，{total_records} 行，筛选出 {filtered_records} 行行政区数据")
            
        except Exception as e:
            self.logger.error(f"解析文件时发生错误: {e}")
            raise
    
    def filter_admin_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        筛选行政区记录（ADM1和ADM2）
        
        Args:
            df: 原始数据块
            
        Returns:
            pd.DataFrame: 筛选后的行政区数据
        """
        # 筛选ADM1和ADM2记录
        mask = df['feature_code'].isin(self.TARGET_FEATURE_CODES)
        filtered_df = df[mask].copy()
        
        # 数据清理
        filtered_df = self._clean_data(filtered_df)
        
        return filtered_df
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清理数据
        
        Args:
            df: 待清理的数据
            
        Returns:
            pd.DataFrame: 清理后的数据
        """
        # 去除name和asciiname中的前后空白
        df['name'] = df['name'].str.strip()
        df['asciiname'] = df['asciiname'].str.strip()
        
        # 处理alternatenames字段的空值
        df['alternatenames'] = df['alternatenames'].fillna('')
        
        # 确保country_code为大写
        df['country_code'] = df['country_code'].str.upper()
        
        # 处理admin codes的空值
        df['admin1_code'] = df['admin1_code'].fillna('')
        df['admin2_code'] = df['admin2_code'].fillna('')
        
        return df
    
    def get_field_mapping(self) -> Dict[str, str]:
        """
        获取字段映射关系
        
        Returns:
            dict: 字段名到描述的映射
        """
        return {
            'geonameid': '地理名称ID',
            'name': '地名',
            'asciiname': 'ASCII地名',
            'alternatenames': '别名',
            'latitude': '纬度',
            'longitude': '经度',
            'feature_class': '特征类别',
            'feature_code': '特征代码',
            'country_code': '国家代码',
            'cc2': '备用国家代码',
            'admin1_code': '一级行政区代码',
            'admin2_code': '二级行政区代码',
            'admin3_code': '三级行政区代码',
            'admin4_code': '四级行政区代码',
            'population': '人口',
            'elevation': '海拔',
            'dem': '数字高程模型',
            'timezone': '时区',
            'modification_date': '修改日期'
        }
    
    def get_statistics(self) -> Dict[str, int]:
        """
        获取解析统计信息
        
        Returns:
            dict: 统计信息
        """
        stats = {
            'total_chunks': 0,
            'total_records': 0,
            'adm1_records': 0,
            'adm2_records': 0,
            'countries': set()
        }
        
        for chunk in self.parse_chunks():
            stats['total_chunks'] += 1
            stats['total_records'] += len(chunk)
            
            # 统计ADM1和ADM2记录
            adm1_count = len(chunk[chunk['feature_code'] == 'ADM1'])
            adm2_count = len(chunk[chunk['feature_code'] == 'ADM2'])
            
            stats['adm1_records'] += adm1_count
            stats['adm2_records'] += adm2_count
            
            # 收集国家代码
            countries = set(chunk['country_code'].dropna().unique())
            stats['countries'].update(countries)
        
        # 转换set为list以便JSON序列化
        stats['countries'] = sorted(list(stats['countries']))
        stats['country_count'] = len(stats['countries'])
        
        return stats
    
    def query_by_geonameid(self, geonameid: str) -> Optional[Dict]:
        """
        根据geonameid查询记录
        
        Args:
            geonameid: 地理名称ID
            
        Returns:
            Optional[Dict]: 查询到的记录，如果未找到则返回None
        """
        try:
            self.logger.debug(f"查询geonameid: {geonameid}")
            
            # 分块搜索
            chunk_reader = pd.read_csv(
                self.file_path,
                sep='\t',
                names=self.FIELD_NAMES,
                chunksize=self.chunk_size,
                encoding='utf-8',
                dtype={'geonameid': 'int64'},
                na_values=['', 'NULL'],
                keep_default_na=False
            )
            
            target_geonameid = int(geonameid)
            
            for chunk in chunk_reader:
                # 查找匹配的记录
                matches = chunk[chunk['geonameid'] == target_geonameid]
                
                if not matches.empty:
                    record = matches.iloc[0].to_dict()
                    self.logger.debug(f"找到记录: {record['name']} ({record['feature_code']})")
                    return record
            
            self.logger.warning(f"未找到geonameid: {geonameid}")
            return None
            
        except Exception as e:
            self.logger.error(f"查询geonameid {geonameid} 时发生错误: {e}")
            return None
    
    def batch_query_geonameids(self, geonameid_list: List[str]) -> Dict[str, Dict]:
        """
        批量查询geonameid对应的记录
        
        Args:
            geonameid_list: geonameid列表
            
        Returns:
            Dict[str, Dict]: {geonameid: record} 映射字典
        """
        try:
            self.logger.info(f"开始批量查询 {len(geonameid_list)} 个geonameid")
            
            result_mapping = {}
            target_ids = set(int(gid) for gid in geonameid_list)
            found_ids = set()
            
            # 分块搜索
            chunk_reader = pd.read_csv(
                self.file_path,
                sep='\t',
                names=self.FIELD_NAMES,
                chunksize=self.chunk_size,
                encoding='utf-8',
                dtype={'geonameid': 'int64'},
                na_values=['', 'NULL'],
                keep_default_na=False
            )
            
            chunk_count = 0
            for chunk in chunk_reader:
                chunk_count += 1
                
                # 查找匹配的记录
                matches = chunk[chunk['geonameid'].isin(target_ids)]
                
                for _, record in matches.iterrows():
                    geonameid_str = str(record['geonameid'])
                    result_mapping[geonameid_str] = record.to_dict()
                    found_ids.add(record['geonameid'])
                
                # 如果已找到所有目标ID，提前退出
                if len(found_ids) == len(target_ids):
                    self.logger.debug(f"在第 {chunk_count} 块找到所有目标记录")
                    break
            
            # 记录查询结果
            found_count = len(result_mapping)
            missing_count = len(geonameid_list) - found_count
            
            self.logger.info(f"批量查询完成: 找到 {found_count}/{len(geonameid_list)} 个记录")
            
            if missing_count > 0:
                missing_ids = [gid for gid in geonameid_list if gid not in result_mapping]
                self.logger.warning(f"未找到 {missing_count} 个geonameid: {missing_ids[:10]}{'...' if len(missing_ids) > 10 else ''}")
            
            return result_mapping
            
        except Exception as e:
            self.logger.error(f"批量查询geonameid时发生错误: {e}")
            return {}
    
    def extract_admin_codes(self, record: Dict) -> Tuple[str, str]:
        """
        从记录中提取adminCode1和adminCode2
        
        Args:
            record: geonames记录字典
            
        Returns:
            Tuple[str, str]: (admin1_code, admin2_code)
        """
        try:
            admin1_code = str(record.get('admin1_code', '')).strip()
            admin2_code = str(record.get('admin2_code', '')).strip()
            
            # 处理NaN值
            if admin1_code in ['nan', 'None', 'NULL']:
                admin1_code = ''
            if admin2_code in ['nan', 'None', 'NULL']:
                admin2_code = ''
            
            return admin1_code, admin2_code
            
        except Exception as e:
            self.logger.error(f"提取admin codes时发生错误: {e}")
            return '', ''
    
    def build_geonameid_index(self, output_file: str = None) -> Dict[str, Dict]:
        """
        构建geonameid索引以提高查询性能
        
        Args:
            output_file: 可选的索引文件输出路径
            
        Returns:
            Dict[str, Dict]: geonameid到记录的映射
        """
        try:
            self.logger.info("开始构建geonameid索引")
            
            index_mapping = {}
            
            for chunk in self.parse_chunks():
                for _, record in chunk.iterrows():
                    geonameid_str = str(record['geonameid'])
                    index_mapping[geonameid_str] = record.to_dict()
            
            self.logger.info(f"索引构建完成: {len(index_mapping)} 个记录")
            
            # 可选：保存索引到文件
            if output_file:
                import json
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(index_mapping, f, ensure_ascii=False, indent=2)
                self.logger.info(f"索引已保存到: {output_file}")
            
            return index_mapping
            
        except Exception as e:
            self.logger.error(f"构建索引时发生错误: {e}")
            return {}
    
    def get_admin_code_summary(self, geonameid_list: List[str]) -> Dict[str, any]:
        """
        获取指定geonameid列表的admin code摘要信息
        
        Args:
            geonameid_list: geonameid列表
            
        Returns:
            Dict: 摘要信息
        """
        try:
            records = self.batch_query_geonameids(geonameid_list)
            
            summary = {
                'total_queried': len(geonameid_list),
                'found_records': len(records),
                'admin1_codes': set(),
                'admin2_codes': set(),
                'countries': set(),
                'feature_codes': set()
            }
            
            for record in records.values():
                admin1, admin2 = self.extract_admin_codes(record)
                
                if admin1:
                    summary['admin1_codes'].add(admin1)
                if admin2:
                    summary['admin2_codes'].add(admin2)
                
                if record.get('country_code'):
                    summary['countries'].add(record['country_code'])
                
                if record.get('feature_code'):
                    summary['feature_codes'].add(record['feature_code'])
            
            # 转换set为sorted list
            for key in ['admin1_codes', 'admin2_codes', 'countries', 'feature_codes']:
                summary[key] = sorted(list(summary[key]))
            
            return summary
            
        except Exception as e:
            self.logger.error(f"生成admin code摘要时发生错误: {e}")
            return {}

def main():
    """
    测试函数
    """
    import logging
    logging.basicConfig(level=logging.INFO)
    
    sample_file = 'source_data/geoname/allCountries.txt'
    
    try:
        parser = GeonamesParser(sample_file, chunk_size=100)
        
        print("=== GeoNames解析器测试 ===")
        print(f"文件: {sample_file}")
        print(f"字段映射: {len(parser.get_field_mapping())} 个字段")
        
        # 获取统计信息
        stats = parser.get_statistics()
        print(f"\n统计信息:")
        print(f"- 总记录数: {stats['total_records']}")
        print(f"- ADM1记录: {stats['adm1_records']}")
        print(f"- ADM2记录: {stats['adm2_records']}")
        print(f"- 国家数量: {stats['country_count']}")
        print(f"- 国家列表: {stats['countries']}")
        
    except Exception as e:
        print(f"测试失败: {e}")

if __name__ == '__main__':
    main()