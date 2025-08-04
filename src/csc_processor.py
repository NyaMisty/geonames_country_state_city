#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSC数据处理器模块
处理source_data/csc/cities.txt文件，提取wikiDataId，实现数据验证和清理
"""

import os
import pandas as pd
from typing import List, Dict, Optional, Tuple, Any
import logging
import re
from alias_processor import AliasProcessor

class CSCProcessor:
    """
    CSC数据处理器
    处理CSC cities.txt文件，提供数据加载、验证、清理和wikiDataId提取功能
    """
    
    # CSC标准字段名（11个字段）
    FIELD_NAMES = [
        'id', 'name', 'state_id', 'state_code', 'state_name',
        'country_id', 'country_code', 'country_name', 
        'latitude', 'longitude', 'wikiDataId'
    ]
    
    # 必填字段
    REQUIRED_FIELDS = ['id', 'name', 'country_code', 'wikiDataId']
    
    def __init__(self, file_path: str = 'source_data/csc/cities.txt'):
        """
        初始化CSC处理器
        
        Args:
            file_path: CSC数据文件路径
        """
        self.file_path = file_path
        self.logger = logging.getLogger(__name__)
        self.data = None
        self.validation_errors = []
        self.alias_processor = AliasProcessor()
        
        # 验证文件路径
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSC数据文件不存在: {file_path}")
    
    def load_csc_data(self, file_path: Optional[str] = None) -> pd.DataFrame:
        """
        加载CSC数据文件
        
        Args:
            file_path: 可选的文件路径，如果不提供则使用初始化时的路径
            
        Returns:
            pd.DataFrame: 加载的CSC数据
            
        Raises:
            FileNotFoundError: 文件不存在
            ValueError: 数据格式错误
        """
        target_file = file_path or self.file_path
        
        try:
            self.logger.info(f"开始加载CSC数据文件: {target_file}")
            
            # 读取CSV文件
            df = pd.read_csv(
                target_file,
                encoding='utf-8',
                dtype={
                    'id': 'int64',
                    'name': 'string',
                    'state_id': 'Int64',  # 可空整数
                    'state_code': 'string',
                    'state_name': 'string',
                    'country_id': 'Int64',
                    'country_code': 'string',
                    'country_name': 'string',
                    'latitude': 'float64',
                    'longitude': 'float64',
                    'wikiDataId': 'string'
                },
                na_values=['NULL', 'null'],
                keep_default_na=False
            )
            
            # 验证字段名
            expected_columns = set(self.FIELD_NAMES)
            actual_columns = set(df.columns)
            
            if expected_columns != actual_columns:
                missing = expected_columns - actual_columns
                extra = actual_columns - expected_columns
                error_msg = f"字段不匹配 - 缺失: {missing}, 多余: {extra}"
                raise ValueError(error_msg)
            
            self.logger.info(f"成功加载CSC数据: {len(df)} 条记录")
            return df
            
        except Exception as e:
            self.logger.error(f"加载CSC数据失败: {str(e)}")
            raise
    
    def validate_csc_data(self, df: pd.DataFrame) -> bool:
        """
        验证CSC数据完整性
        
        Args:
            df: CSC数据DataFrame
            
        Returns:
            bool: 验证是否通过
            
        Raises:
            ValueError: 数据验证失败
        """
        try:
            self.logger.info("开始验证CSC数据完整性")
            
            # 检查DataFrame是否为空
            if df.empty:
                raise ValueError("CSC数据为空")
            
            # 检查必填字段
            for field in self.REQUIRED_FIELDS:
                if field not in df.columns:
                    raise ValueError(f"缺少必填字段: {field}")
                
                # 检查必填字段的空值
                null_count = df[field].isna().sum()
                if null_count > 0:
                    self.logger.warning(f"字段 {field} 存在 {null_count} 个空值")
            
            # 检查ID唯一性
            duplicate_ids = df[df['id'].duplicated()]
            if not duplicate_ids.empty:
                self.logger.warning(f"发现 {len(duplicate_ids)} 个重复ID")
            
            # 检查wikiDataId格式
            invalid_wiki_ids = self._validate_wikidata_ids(df)
            if invalid_wiki_ids:
                self.logger.warning(f"发现 {len(invalid_wiki_ids)} 个无效的wikiDataId: {invalid_wiki_ids[:5]}...")
            
            # 检查坐标范围
            invalid_coords = self._validate_coordinates(df)
            if invalid_coords > 0:
                self.logger.warning(f"发现 {invalid_coords} 个无效坐标")
            
            self.logger.info("CSC数据验证完成")
            return True
            
        except Exception as e:
            self.logger.error(f"CSC数据验证失败: {str(e)}")
            raise
    
    def extract_wikidata_ids(self, df: pd.DataFrame) -> List[str]:
        """
        提取有效的wikiDataId列表
        
        Args:
            df: CSC数据DataFrame
            
        Returns:
            List[str]: 有效的wikiDataId列表
        """
        try:
            self.logger.info("开始提取wikiDataId")
            
            # 过滤空值和无效格式
            valid_ids = []
            
            for idx, wiki_id in df['wikiDataId'].items():
                if pd.notna(wiki_id) and self._is_valid_wikidata_id(str(wiki_id)):
                    valid_ids.append(str(wiki_id))
            
            # 去重并排序
            unique_ids = sorted(list(set(valid_ids)))
            
            self.logger.info(f"提取到 {len(unique_ids)} 个有效的wikiDataId")
            return unique_ids
            
        except Exception as e:
            self.logger.error(f"提取wikiDataId失败: {str(e)}")
            raise
    
    def clean_csc_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清理CSC地名数据
        
        Args:
            df: 原始CSC数据DataFrame
            
        Returns:
            pd.DataFrame: 清理后的数据
        """
        try:
            self.logger.info("开始清理CSC地名数据")
            
            # 创建副本避免修改原数据
            cleaned_df = df.copy()
            
            # 清理地名字段
            name_fields = ['name', 'state_name', 'country_name']
            
            for field in name_fields:
                if field in cleaned_df.columns:
                    # 去除首尾空格
                    cleaned_df[field] = cleaned_df[field].str.strip()
                    
                    # 标准化引号
                    cleaned_df[field] = cleaned_df[field].str.replace('"', '', regex=False)
                    
                    # 处理特殊字符
                    cleaned_df[field] = cleaned_df[field].apply(self._clean_name_text)
            
            # 标准化国家代码
            if 'country_code' in cleaned_df.columns:
                cleaned_df['country_code'] = cleaned_df['country_code'].str.upper().str.strip()
            
            # 标准化州代码
            if 'state_code' in cleaned_df.columns:
                cleaned_df['state_code'] = cleaned_df['state_code'].str.upper().str.strip()
            
            self.logger.info("CSC地名数据清理完成")
            return cleaned_df
            
        except Exception as e:
            self.logger.error(f"清理CSC地名数据失败: {str(e)}")
            raise
    
    def get_data_summary(self, df: pd.DataFrame) -> Dict[str, any]:
        """
        获取CSC数据摘要信息
        
        Args:
            df: CSC数据DataFrame
            
        Returns:
            Dict: 数据摘要信息
        """
        try:
            summary = {
                'total_records': len(df),
                'countries': df['country_code'].nunique() if 'country_code' in df.columns else 0,
                'states': df['state_id'].nunique() if 'state_id' in df.columns else 0,
                'valid_wikidata_ids': len(self.extract_wikidata_ids(df)),
                'missing_wikidata_ids': df['wikiDataId'].isna().sum(),
                'coordinate_coverage': {
                    'latitude_missing': df['latitude'].isna().sum(),
                    'longitude_missing': df['longitude'].isna().sum()
                }
            }
            
            return summary
            
        except Exception as e:
            self.logger.error(f"生成数据摘要失败: {str(e)}")
            raise
    
    def _validate_wikidata_ids(self, df: pd.DataFrame) -> List[int]:
        """
        验证wikiDataId格式
        
        Args:
            df: CSC数据DataFrame
            
        Returns:
            List[int]: 无效wikiDataId的行索引
        """
        invalid_indices = []
        
        for idx, wiki_id in df['wikiDataId'].items():
            if pd.notna(wiki_id) and not self._is_valid_wikidata_id(str(wiki_id)):
                invalid_indices.append(idx)
        
        return invalid_indices
    
    def _validate_coordinates(self, df: pd.DataFrame) -> int:
        """
        验证坐标范围
        
        Args:
            df: CSC数据DataFrame
            
        Returns:
            int: 无效坐标的数量
        """
        invalid_count = 0
        
        # 检查纬度范围 [-90, 90]
        if 'latitude' in df.columns:
            invalid_lat = (df['latitude'] < -90) | (df['latitude'] > 90)
            invalid_count += invalid_lat.sum()
        
        # 检查经度范围 [-180, 180]
        if 'longitude' in df.columns:
            invalid_lon = (df['longitude'] < -180) | (df['longitude'] > 180)
            invalid_count += invalid_lon.sum()
        
        return invalid_count
    
    def _is_valid_wikidata_id(self, wiki_id: str) -> bool:
        """
        检查wikiDataId格式是否有效
        
        Args:
            wiki_id: wikiDataId字符串
            
        Returns:
            bool: 是否有效
        """
        # Wikidata ID格式: Q + 数字
        pattern = r'^Q\d+$'
        return bool(re.match(pattern, wiki_id.strip()))
    
    def _clean_name_text(self, text: any) -> str:
        """
        清理文本内容
        
        Args:
            text: 原始文本
            
        Returns:
            str: 清理后的文本
        """
        if pd.isna(text):
            return text
        
        text = str(text)
        
        # 去除多余空格
        text = re.sub(r'\s+', ' ', text)
        
        # 去除首尾空格
        text = text.strip()
        
        return text if text else None

    def _normalize_csc_name(self, name: str) -> List[str]:
        """
        CSC名称标准化处理，使用AliasProcessor进行统一的别名处理
        
        Args:
            name: 原始名称
            
        Returns:
            List[str]: 标准化后的名称列表
        """
        if not name:
            return []
        
        # 使用AliasProcessor处理别名
        processed_aliases = self.alias_processor.process_alternatenames([name])
        
        # 移除原始名称，只返回处理后的变体
        normalized_names = [alias for alias in processed_aliases if alias != name]
        
        return normalized_names
    
    def get_csc_processing_stats(self, city_names: List[Dict], state_names: List[Dict]) -> Dict[str, Any]:
        """
        获取CSC名称处理统计信息
        
        Args:
            city_names: city_names记录列表
            state_names: state_names记录列表
            
        Returns:
            Dict: 统计信息
        """
        stats = {
            'city_names_count': len(city_names),
            'state_names_count': len(state_names),
            'unique_countries': set(),
            'unique_states': set(),
            'cities_by_country': {},
            'states_by_country': {}
        }
        
        # 统计city_names
        for record in city_names:
            country = record.get('country_code', '')
            stats['unique_countries'].add(country)
            
            if country not in stats['cities_by_country']:
                stats['cities_by_country'][country] = 0
            stats['cities_by_country'][country] += 1
        
        # 统计state_names
        for record in state_names:
            country = record.get('country_code', '')
            stats['unique_countries'].add(country)
            stats['unique_states'].add(record.get('name', ''))
            
            if country not in stats['states_by_country']:
                stats['states_by_country'][country] = 0
            stats['states_by_country'][country] += 1
        
        # 转换集合为计数
        stats['unique_countries_count'] = len(stats['unique_countries'])
        stats['unique_states_count'] = len(stats['unique_states'])
        
        # 移除集合（不可序列化）
        del stats['unique_countries']
        del stats['unique_states']
        
        return stats
    
    def process_csc_integration1(self, enable_cache: bool = True) -> pd.DataFrame:
        """
        统一的CSC数据处理集成接口
        
        Args:
            enable_cache: 是否启用Wikidata查询缓存，默认True
            
        Returns:
            Dict[str, Any]: geomapping匹配结果
            Dict[str, Any]: wikidata列表
            
        Raises:
            Exception: 处理过程中的任何错误
        """
        try:
            self.logger.info("开始CSC数据集成处理流程")
            
            # 1. 加载CSC数据
            self.logger.info("步骤1: 加载CSC数据")
            csc_data = self.load_csc_data()
            
            # 2. 验证数据完整性
            self.logger.info("步骤2: 验证数据完整性")
            self.validate_csc_data(csc_data)
            
            # # 3. 清理地名数据
            # self.logger.info("步骤3: 清理地名数据")
            # cleaned_data = self.clean_csc_names(csc_data)
            cleaned_data = csc_data
            
            # 4. 提取wikidata IDs
            self.logger.info("步骤4: 提取wikidata IDs")
            wikidata_ids = self.extract_wikidata_ids(cleaned_data)
            
            if not wikidata_ids:
                self.logger.warning("未找到有效的wikidata IDs")
                return self._create_empty_result()
            
            # 5. 批量查询geonameid（使用缓存）
            self.logger.info(f"步骤5: 批量查询geonameid (缓存: {'启用' if enable_cache else '禁用'})")
            from wikidata_query import batch_query_geonameid
            geonameid_mapping = batch_query_geonameid(wikidata_ids, enable_cache=enable_cache)
            self.cleaned_data = cleaned_data
            return geonameid_mapping, wikidata_ids
        except Exception as e:
            self.logger.error(f"CSC数据集成处理失败: {str(e)}")
            raise
    def process_csc_integration2(self, geonameid_mapping, wikidata_ids) -> Dict[str, Any]:
        """
        统一的CSC数据处理集成接口2
        
        Returns:
            Dict[str, Any]: 包含处理结果的字典，格式为:
            {
                "csc_states_df": DataFrame,     # CSC州数据
                "csc_mapping_df": DataFrame,    # CSC映射表(csc_id, wikidata_id, geonameid)
                "csc_aliases_dict": dict,       # geonameid到CSC别名列表的映射
                "stats": dict                   # 处理统计信息
            }
            
        Raises:
            Exception: 处理过程中的任何错误
        """
        try:
            self.logger.info("开始CSC数据集成处理流程2")
            
            # 6. 将geonameid映射添加到数据中
            self.logger.info("步骤6: 合并geonameid映射")
            enriched_data = self._merge_geonameid_mapping(self.cleaned_data, geonameid_mapping)
            
            # 7. 生成CSC数据结构
            self.logger.info("步骤7: 生成CSC数据结构")
            csc_mapping_df = self._generate_csc_mapping(enriched_data)
            csc_aliases_dict = self._generate_csc_aliases(enriched_data)
            
            # 8. 收集统计信息
            self.logger.info("步骤8: 收集统计信息")
            stats = self._collect_integration_stats(
                enriched_data, geonameid_mapping, wikidata_ids
            )
            
            # 9. 构建返回结果
            result = {
                "csc_mapping_df": csc_mapping_df,
                "csc_aliases_dict": csc_aliases_dict,
                "stats": stats
            }
            
            self.logger.info("CSC数据集成处理完成")
            return result
            
        except Exception as e:
            self.logger.error(f"CSC数据集成处理失败: {str(e)}", exc_info=True)
            raise
    
    def _create_empty_result(self) -> Dict[str, Any]:
        """
        创建空的处理结果
        
        Returns:
            Dict[str, Any]: 空的结果字典
        """
        return {
            "csc_states_df": pd.DataFrame(),
            "csc_mapping_df": pd.DataFrame(),
            "csc_aliases_dict": {},
            "stats": {
                "total_records": 0,
                "valid_wikidata_ids": 0,
                "geonameid_matches": 0,
                "states_generated": 0,
                "cities_generated": 0
            }
        }
    
    def _merge_geonameid_mapping(self, csc_data: pd.DataFrame, geonameid_mapping: Dict[str, str]) -> pd.DataFrame:
        """
        将geonameid映射合并到CSC数据中
        
        Args:
            csc_data: CSC数据DataFrame
            geonameid_mapping: wikiDataId到geonameid的映射
            
        Returns:
            pd.DataFrame: 包含geonameid的enriched数据
        """
        enriched_data = csc_data.copy()
        
        # 添加matched_geonameid列
        enriched_data['matched_geonameid'] = enriched_data['wikiDataId'].map(geonameid_mapping)
        
        # 记录匹配统计
        matched_count = enriched_data['matched_geonameid'].notna().sum()
        total_count = len(enriched_data)
        
        self.logger.info(f"geonameid匹配: {matched_count}/{total_count} ({matched_count/total_count*100:.1f}%)")
        
        return enriched_data
    
    def _generate_csc_mapping(self, enriched_data: pd.DataFrame) -> pd.DataFrame:
        """
        生成CSC映射表，包含cscId、wikidataId、geonameid字段
        
        Args:
            enriched_data: 包含geonameid的enriched数据
            
        Returns:
            pd.DataFrame: CSC映射表
        """
        # 只包含有geonameid匹配的记录
        mapping_data = enriched_data[enriched_data['matched_geonameid'].notna()].copy()
        
        # 生成映射表
        # csc_mapping_df = mapping_data[['id', 'wikiDataId', 'matched_geonameid']].rename(columns={
        csc_mapping_df = mapping_data.rename(columns={
            # 'id': 'csc_id',
            # 'wikiDataId': 'wikidata_id',
            'matched_geonameid': 'geonameid'
        }) #.drop_duplicates()
        
        self.logger.info(f"生成CSC映射表: {len(csc_mapping_df)} 条记录")
        
        return csc_mapping_df
    
    def _generate_csc_aliases(self, enriched_data: pd.DataFrame) -> Dict[str, List[str]]:
        """
        生成CSC别名字典，用于geo_hierarchy模块处理
        使用AliasProcessor进行统一的别名处理
        
        Args:
            enriched_data: 包含geonameid的enriched数据
            
        Returns:
            Dict[str, List[str]]: geonameid到CSC别名列表的映射
        """
        aliases_dict = {}
        
        # 处理有geonameid匹配的记录
        matched_data = enriched_data[enriched_data['matched_geonameid'].notna()]
        
        for _, row in matched_data.iterrows():
            geonameid = str(row['matched_geonameid'])
            csc_name = self._clean_name_text(row['name'])
            
            if geonameid and csc_name:
                if geonameid not in aliases_dict:
                    aliases_dict[geonameid] = []
                
                # 使用AliasProcessor处理CSC名称，获取所有别名变体
                processed_aliases = self.alias_processor.process_alternatenames([csc_name])
                
                # 添加所有处理后的别名（包括原始名称和变体）
                for alias in processed_aliases:
                    if alias and alias not in aliases_dict[geonameid]:
                        aliases_dict[geonameid].append(alias)
        
        self.logger.info(f"生成CSC别名字典: {len(aliases_dict)} 个geonameid")
        
        return aliases_dict
    
    def _collect_integration_stats(self, enriched_data: pd.DataFrame,
                                 geonameid_mapping: Dict[str, str], wikidata_ids: List[str]) -> Dict[str, Any]:
        """
        收集集成处理的统计信息
        
        Args:
            enriched_data: 包含geonameid的enriched数据
            geonameid_mapping: geonameid映射字典
            wikidata_ids: wikidata ID列表
            
        Returns:
            Dict[str, Any]: 统计信息字典
        """
        # 内存优化：使用生成器表达式计算匹配数
        geonameid_matches = sum(1 for v in geonameid_mapping.values() if v)
        
        # 计算数据质量指标
        total_wikidata_ids = len(wikidata_ids)
        match_rate = (geonameid_matches / total_wikidata_ids * 100) if total_wikidata_ids > 0 else 0
        
        # 计算覆盖率统计
        countries_covered = enriched_data['country_code'].nunique() if not enriched_data.empty else 0
        
        # 内存使用统计
        memory_stats = {
            "enriched_data_memory_mb": enriched_data.memory_usage(deep=True).sum() / 1024 / 1024 if not enriched_data.empty else 0
        }
        
        # 数据分布统计
        distribution_stats = {}
        if not enriched_data.empty:
            distribution_stats["records_by_country"] = enriched_data['country_code'].value_counts().to_dict()
            distribution_stats["coordinate_coverage"] = {
                "records_with_coordinates": enriched_data[['latitude', 'longitude']].notna().all(axis=1).sum(),
                "records_missing_coordinates": enriched_data[['latitude', 'longitude']].isna().any(axis=1).sum()
            }
            distribution_stats["geonameid_coverage"] = {
                "records_with_geonameid": enriched_data['matched_geonameid'].notna().sum(),
                "records_missing_geonameid": enriched_data['matched_geonameid'].isna().sum()
            }
        
        # 计算生成的城市和州数量
        cities_generated = enriched_data[enriched_data['matched_geonameid'].notna()].shape[0] if not enriched_data.empty else 0
        states_generated = enriched_data[
            (enriched_data['state_id'].notna()) & 
            (enriched_data['state_name'].notna())
        ][['state_id', 'state_name', 'country_code']].drop_duplicates().shape[0] if not enriched_data.empty else 0
        
        stats = {
            "total_records": len(enriched_data),
            "valid_wikidata_ids": total_wikidata_ids,
            "geonameid_matches": geonameid_matches,
            "states_generated": states_generated,
            "cities_generated": cities_generated,
            "geonameid_match_rate": round(match_rate, 2),
            "countries_covered": countries_covered,
            "memory_usage": memory_stats,
            "data_distribution": distribution_stats,
            "processing_summary": {
                "cache_enabled": True,  # 这个值会在调用时设置
                "data_quality": "excellent" if match_rate > 80 else "good" if match_rate > 50 else "fair" if match_rate > 20 else "poor",
                "completion_status": "success",
                "efficiency_score": min(100, round((geonameid_matches / max(1, total_wikidata_ids)) * 100 + (countries_covered * 2), 1))
            }
        }
        
        return stats