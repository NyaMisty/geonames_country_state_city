#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV地理匹配处理器模块
实现CSV数据与GeoNames数据库的地理匹配功能
"""

import os
import logging
import pandas as pd
from typing import Dict, List, Tuple, Any, Optional

from sqlalchemy.orm import state
from sqlite_integrator import SQLiteIntegrator

logger = logging.getLogger(__name__)

class CSVGeoMatcher:
    """
    CSV地理匹配处理器
    实现CSV数据与GeoNames数据库的分层地理匹配
    """
    
    def __init__(self, 
                 db_path: str,
                 input_csv: str,
                 output_csv: str,
                 column_mapping: Dict[str, str],
                 output_dir: str = 'output/csv_geo_matching'):
        """
        初始化CSV地理匹配处理器
        
        Args:
            db_path (str): GeoNames数据库路径
            input_csv (str): 输入CSV文件路径
            output_csv (str): 输出CSV文件路径
            column_mapping (Dict[str, str]): 列名映射配置
                格式: {'country_code': 'country_col', 'state_name': 'state_col', 'city_name': 'city_col'}
            output_dir (str): 输出目录
        """
        self.db_path = db_path
        self.input_csv = input_csv
        self.output_csv = output_csv
        self.column_mapping = column_mapping
        self.output_dir = output_dir
        
        # 初始化数据库连接
        self.sqlite_integrator = None
        
        # 处理统计信息
        self.stats = {
            'total_records': 0,
            'successful_matches': 0,
            'state_match_failures': 0,
            'city_match_failures': 0,
            'data_validation_errors': 0,
            'processing_time': 0
        }
        
        # 详细统计信息
        self.detailed_stats = {
            'state_match_details': {
                'exact_matches': 0,
                'multiple_matches': 0,
                'no_matches': 0,
                'failed_countries': set(),
                'failed_states': set()
            },
            'city_match_details': {
                'exact_matches': 0,
                'multiple_matches': 0,
                'no_matches': 0,
                'failed_cities': set()
            },
            'data_quality_issues': {
                'empty_country_codes': 0,
                'empty_state_names': 0,
                'empty_city_names': 0,
                'invalid_data_types': 0,
                'special_characters': 0
            },
            'performance_metrics': {
                'avg_processing_time_per_record': 0,
                'database_query_count': 0,
                'memory_usage_mb': 0
            }
        }
        
        # 数据存储
        self.df = None
        self.match_results = []
        self.failed_records = []
        
        # 验证初始化参数
        self._validate_initialization()
        
        logger.info(f"CSV地理匹配处理器初始化完成")
        logger.info(f"输入文件: {self.input_csv}")
        logger.info(f"输出文件: {self.output_csv}")
        logger.info(f"数据库路径: {self.db_path}")
        logger.info(f"列名映射: {self.column_mapping}")
    
    def _validate_initialization(self) -> None:
        """
        验证初始化参数
        """
        # 检查数据库文件是否存在
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"数据库文件不存在: {self.db_path}")
        
        # 检查输入CSV文件是否存在
        if not os.path.exists(self.input_csv):
            raise FileNotFoundError(f"输入CSV文件不存在: {self.input_csv}")
        
        # 验证列名映射配置
        required_keys = ['country_code', 'state_name', 'city_name']
        for key in required_keys:
            if key not in self.column_mapping:
                raise ValueError(f"列名映射缺少必需的键: {key}")
        
        # 创建输出目录
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 初始化数据库连接
        try:
            self.sqlite_integrator = SQLiteIntegrator(self.db_path)
            logger.info("数据库连接初始化成功")
        except Exception as e:
            raise RuntimeError(f"数据库连接初始化失败:", exc_info=True)
    
    def load_csv_data(self) -> pd.DataFrame:
        """
        读取CSV文件并应用列名映射
        
        Returns:
            pd.DataFrame: 处理后的DataFrame
        """
        try:
            logger.info(f"开始读取CSV文件: {self.input_csv}")
            
            # 读取CSV文件
            self.df = pd.read_csv(self.input_csv)
            logger.info(f"成功读取CSV文件，共 {len(self.df)} 行数据")
            
            # 验证必需列是否存在
            missing_columns = []
            for key, column_name in self.column_mapping.items():
                if column_name not in self.df.columns:
                    missing_columns.append(column_name)
            
            if missing_columns:
                raise ValueError(f"CSV文件缺少必需的列: {missing_columns}")
            
            # 应用列名映射，创建标准化列名 (改为在访问时映射)
            # self.df = self.df.rename(columns={v:k for k,v in self.column_mapping.items()})

            # 添加geonameid列（如果不存在）
            if 'geonameid' not in self.df.columns:
                self.df['geonameid'] = None
            
            # 数据清理和验证
            self._clean_and_validate_data()
            
            self.stats['total_records'] = len(self.df)
            logger.info(f"CSV数据加载完成，有效记录数: {self.stats['total_records']}")
            
            return self.df
            
        except Exception as e:
            logger.error(f"CSV数据加载失败:", exc_info=True)
            raise
    
    def _clean_and_validate_data(self) -> None:
        """
        清理和验证数据
        """
        initial_count = len(self.df)
        
        # 移除空值记录
        required_columns = [v for k, v in self.column_mapping.items() if k in ['country_code', 'state_name', 'city_name']]
        self.df = self.df.dropna(subset=required_columns)
        
        # 数据类型转换和清理
        for col in required_columns:
            self.df[col] = self.df[col].astype(str).str.strip()
        
        # 移除空字符串记录
        for col in required_columns:
            self.df = self.df[self.df[col] != '']
        
        # 统计数据验证错误
        self.stats['data_validation_errors'] = initial_count - len(self.df)
        
        if self.stats['data_validation_errors'] > 0:
            logger.warning(f"数据验证过程中移除了 {self.stats['data_validation_errors']} 条无效记录")
    
    def validate_columns(self) -> bool:
        """
        验证映射后的列名是否正确
        
        Returns:
            bool: 验证是否通过
        """
        try:
            if self.df is None:
                logger.error("数据未加载，请先调用load_csv_data()")
                return False
            
            # 检查必需列是否存在
            required_columns = [v for k, v in self.column_mapping.items() if k in ['country_code', 'state_name', 'city_name']]
            missing_columns = [col for col in required_columns if col not in self.df.columns]
            
            if missing_columns:
                logger.error(f"缺少必需的列: {missing_columns}")
                return False
            
            # 检查数据完整性
            for col in required_columns:
                null_count = self.df[col].isnull().sum()
                if null_count > 0:
                    logger.warning(f"列 '{col}' 包含 {null_count} 个空值")
            
            logger.info("列验证通过")
            return True
            
        except Exception as e:
            logger.error(f"列验证失败:", exc_info=True)
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        获取处理统计信息
        
        Returns:
            Dict[str, Any]: 统计信息字典
        """
        return self.stats.copy()
    
    def reset_statistics(self) -> None:
        """
        重置统计信息
        """
        self.stats = {
            'total_records': 0,
            'successful_matches': 0,
            'state_match_failures': 0,
            'city_match_failures': 0,
            'data_validation_errors': 0,
            'processing_time': 0
        }
        logger.info("统计信息已重置")
    
    def match_state(self, country_code: str, state_name: str) -> Optional[int]:
        """
        通过country_code和state_name匹配state geonameid
        
        Args:
            country_code (str): 国家代码
            state_name (str): 州/省名称
            
        Returns:
            Optional[int]: 匹配到的state geonameid，如果没有匹配则返回None
        """
        try:
            # 数据质量检查
            if not country_code or not country_code.strip():
                self.detailed_stats['data_quality_issues']['empty_country_codes'] += 1
                return None
            
            if not state_name or not state_name.strip():
                self.detailed_stats['data_quality_issues']['empty_state_names'] += 1
                return None
            
            conn = self.sqlite_integrator.get_connection()
            cursor = conn.cursor()
            
            # 统计数据库查询次数
            self.detailed_stats['performance_metrics']['database_query_count'] += 1
            
            # 使用大小写不敏感的查询
            query = """
                SELECT sn.geonameid, s.population, sn.admin1_code 
                FROM state_names sn 
                JOIN states s ON sn.geonameid = s.geonameid 
                WHERE sn.country_code = ? COLLATE NOCASE 
                AND sn.name = ? COLLATE NOCASE
                ORDER BY s.population DESC
                LIMIT 1
            """
            
            cursor.execute(query, (country_code, state_name))
            results = cursor.fetchall()
            
            if results:
                # 如果有多个匹配结果，选择人口最多的
                best_match = results[0]
                state_geonameid = best_match[0]
                state_admin1_code = best_match[2]
                
                if len(results) == 1:
                    self.detailed_stats['state_match_details']['exact_matches'] += 1
                else:
                    self.detailed_stats['state_match_details']['multiple_matches'] += 1
                    logger.debug(f"州名匹配找到多个结果，选择人口最多的: {country_code}/{state_name} -> {state_geonameid}")
                
                return state_geonameid, state_admin1_code
            else:
                self.detailed_stats['state_match_details']['no_matches'] += 1
                self.detailed_stats['state_match_details']['failed_countries'].add(country_code)
                self.detailed_stats['state_match_details']['failed_states'].add(f"{country_code}:{state_name}")
                logger.debug(f"州名匹配失败: {country_code}/{state_name}")
                return None
                
        except Exception as e:
            logger.error(f"州名匹配过程中出错:", exc_info=True)
            return None
    
    def match_city(self, country_code: str, state_geonameid: int, state_admin1_code: str, city_name: str) -> Optional[int]:
        """
        通过state_geonameid和city_name匹配city geonameid
        
        Args:
            state_geonameid (int): 州的geonameid
            city_name (str): 城市名称
            
        Returns:
            Optional[int]: 匹配到的city geonameid，如果没有匹配则返回None
        """
        try:
            # 数据质量检查
            if not city_name or not city_name.strip():
                self.detailed_stats['data_quality_issues']['empty_city_names'] += 1
                return None
            
            conn = self.sqlite_integrator.get_connection()
            cursor = conn.cursor()
            
            # 统计数据库查询次数
            self.detailed_stats['performance_metrics']['database_query_count'] += 1
            
            # 使用大小写不敏感的查询，按人口降序排序
            results = []
            if False:
                query = """
                    SELECT cn.geonameid, c.population 
                    FROM city_names cn 
                    JOIN cities c ON cn.geonameid = c.geonameid 
                    WHERE cn.country_code = ? COLLATE NOCASE
                    AND cn.state_geonameid = ? 
                    AND cn.name = ? COLLATE NOCASE
                    ORDER BY c.population DESC
                    LIMIT 1
                """
                cursor.execute(query, (country_code, state_geonameid, city_name))
                results = cursor.fetchall()
            elif state_geonameid is not None or state_admin1_code is not None:
                query = """
                    SELECT cn.geonameid, c.population 
                    FROM city_names cn 
                    JOIN cities c ON cn.geonameid = c.geonameid 
                    WHERE cn.country_code = ? COLLATE NOCASE
                    AND cn.admin1_code = ? 
                    AND cn.name = ? COLLATE NOCASE
                    ORDER BY c.population DESC
                    LIMIT 1
                """
                cursor.execute(query, (country_code, state_admin1_code, city_name))
                results = cursor.fetchall()
            if not results:
                query = """
                    SELECT cn.geonameid, c.population 
                    FROM city_names cn 
                    JOIN cities c ON cn.geonameid = c.geonameid 
                    WHERE cn.country_code = ? COLLATE NOCASE
                    AND cn.name = ? COLLATE NOCASE
                    ORDER BY c.population DESC
                    LIMIT 2
                """
                cursor.execute(query, (country_code, city_name))
                results = cursor.fetchall()

            
            if results:
                # 选择人口最多的城市
                best_match = results[0]
                city_geonameid = best_match[0]
                
                if len(results) == 1:
                    self.detailed_stats['city_match_details']['exact_matches'] += 1
                else:
                    self.detailed_stats['city_match_details']['multiple_matches'] += 1
                    logger.debug(f"城市名匹配找到多个结果，选择人口最多的: {state_geonameid}/{city_name} -> {city_geonameid}")
                
                return city_geonameid
            else:
                self.detailed_stats['city_match_details']['no_matches'] += 1
                self.detailed_stats['city_match_details']['failed_cities'].add(f"{state_geonameid}:{city_name}")
                logger.debug(f"城市名匹配失败: {state_geonameid}/{city_name}")
                return None
                
        except Exception as e:
            logger.error(f"城市名匹配过程中出错:", exc_info=True)
            return None
    
    def match_geography_single(self, country_code: str, state_name: str, city_name: str) -> Tuple[Optional[int], Dict[str, Any]]:
        """
        对单条记录执行两步地理匹配
        
        Args:
            country_code (str): 国家代码
            state_name (str): 州/省名称
            city_name (str): 城市名称
            
        Returns:
            Tuple[Optional[int], Dict[str, Any]]: (匹配到的geonameid, 匹配详情)
        """
        match_details = {
            'country_code': country_code,
            'state_name': state_name,
            'city_name': city_name,
            'state_geonameid': None,
            'city_geonameid': None,
            'match_status': 'failed',
            'failure_reason': None
        }
        
        try:
            # 第一步：匹配州
            ret = self.match_state(country_code, state_name)
            
            # if ret is None:
            #     match_details['failure_reason'] = 'state_not_found'
            #     self.stats['state_match_failures'] += 1
            #     logger.info(f"无法找到匹配的州: {country_code}/{state_name}")
            #     # 添加到失败记录
            #     self.failed_records.append({
            #         'country_code': country_code,
            #         'state_name': state_name,
            #         'city_name': city_name,
            #         'failure_type': 'state_not_found',
            #         'failure_reason': f'无法找到匹配的州: {country_code}/{state_name}',
            #         'suggestion': '检查国家代码和州名拼写，或确认该州在数据库中存在'
            #     })
            #     return None, match_details
            
            if ret is not None:
                state_geonameid, state_admin1_code = ret
                match_details['state_geonameid'] = state_geonameid
                match_details['state_admin1_code'] = state_admin1_code
            else:
                state_geonameid, state_admin1_code = None, None

            
            # 第二步：匹配城市
            city_geonameid = self.match_city(country_code, state_geonameid, state_admin1_code, city_name)
            
            if city_geonameid is None:
                match_details['failure_reason'] = 'city_not_found'
                self.stats['city_match_failures'] += 1
                logger.info(f"无法找到匹配的城市: {country_code}/{state_name}({state_admin1_code})/{city_name}")
                # 添加到失败记录
                self.failed_records.append({
                    'country_code': country_code,
                    'state_name': state_name,
                    'city_name': city_name,
                    'state_geonameid': state_geonameid,
                    'failure_type': 'city_not_found',
                    'failure_reason': f'无法找到匹配的城市: {city_name} (在州 {state_geonameid})',
                    'suggestion': '检查城市名拼写，或确认该城市在指定州内存在'
                })
                return None, match_details
            
            match_details['city_geonameid'] = city_geonameid
            match_details['match_status'] = 'success'
            self.stats['successful_matches'] += 1
            
            return city_geonameid, match_details
            
        except Exception as e:
            logger.error(f"地理匹配过程中出错:", exc_info=True)
            match_details['failure_reason'] = f'error: {str(e)}'
            return None, match_details
    
    def match_geography_batch(self, df: pd.DataFrame = None) -> pd.DataFrame:
        """
        批量处理整个DataFrame的地理匹配
        
        Args:
            df (pd.DataFrame, optional): 要处理的DataFrame，如果为None则使用self.df
            
        Returns:
            pd.DataFrame: 包含匹配结果的DataFrame
        """
        import time
        
        if df is None:
            df = self.df
        
        if df is None:
            raise ValueError("没有可处理的数据，请先调用load_csv_data()")
        
        logger.info(f"开始批量地理匹配，共 {len(df)} 条记录")
        start_time = time.time()
        
        # 重置统计信息
        self.stats['successful_matches'] = 0
        self.stats['state_match_failures'] = 0
        self.stats['city_match_failures'] = 0
        
        # 存储匹配详情
        self.match_results = []
        
        succ = 0
        def worker(rr):
            index, row = rr
            country_code = str(row[self.column_mapping['country_code']]).strip()
            state_name = str(row[self.column_mapping['state_name']]).strip()
            city_name = str(row[self.column_mapping['city_name']]).strip()
            
            # 执行匹配
            geonameid, match_details = self.match_geography_single(country_code, state_name, city_name)
            
            # 更新DataFrame
            df.at[index, 'geonameid'] = geonameid
            if geonameid:
                nonlocal succ
                succ += 1
            
            # 保存匹配详情
            match_details['row_index'] = index
            self.match_results.append(match_details)
            
        # 批量处理
        # for rr in df.iterrows():
        #     worker(rr)
        #     # 进度日志
        #     if (index + 1) % 1000 == 0:
        #         logger.info(f"已处理 {index + 1}/{len(df)} 条记录, 成功 {succ} 条")
        from multiprocessing.pool import ThreadPool
        pool = ThreadPool(5)
        for index, _ in enumerate(pool.imap_unordered(worker, df.iterrows())):
            # 进度日志
            if (index + 1) % 1000 == 0:
                logger.info(f"已处理 {index + 1}/{len(df)} 条记录, 成功 {succ} 条")
        
        # 更新处理时间
        self.stats['processing_time'] = time.time() - start_time
        
        # 输出统计信息
        success_rate = (self.stats['successful_matches'] / len(df)) * 100 if len(df) > 0 else 0
        logger.info(f"批量匹配完成，成功率: {success_rate:.2f}%")
        logger.info(f"成功匹配: {self.stats['successful_matches']}")
        logger.info(f"州匹配失败: {self.stats['state_match_failures']}")
        logger.info(f"城市匹配失败: {self.stats['city_match_failures']}")
        logger.info(f"处理时间: {self.stats['processing_time']:.2f}秒")
        
        return df
    
    def generate_detailed_match_report(self) -> Dict[str, Any]:
        """
        生成详细的匹配统计报告
        
        Returns:
            Dict[str, Any]: 详细的匹配报告
        """
        total_records = self.stats['total_records']
        if total_records == 0:
            return {'error': '没有处理任何记录'}
        
        # 计算成功率
        success_rate = (self.stats['successful_matches'] / total_records) * 100
        state_failure_rate = (self.stats['state_match_failures'] / total_records) * 100
        city_failure_rate = (self.stats['city_match_failures'] / total_records) * 100
        
        # 生成报告
        report = {
            'summary': {
                'total_records': total_records,
                'successful_matches': self.stats['successful_matches'],
                'success_rate': round(success_rate, 2),
                'state_failures': self.stats['state_match_failures'],
                'state_failure_rate': round(state_failure_rate, 2),
                'city_failures': self.stats['city_match_failures'],
                'city_failure_rate': round(city_failure_rate, 2),
                'processing_time': self.stats['processing_time']
            },
            'state_match_analysis': {
                'exact_matches': self.detailed_stats['state_match_details']['exact_matches'],
                'multiple_matches': self.detailed_stats['state_match_details']['multiple_matches'],
                'no_matches': self.detailed_stats['state_match_details']['no_matches'],
                'failed_countries_count': len(self.detailed_stats['state_match_details']['failed_countries']),
                'failed_countries': list(self.detailed_stats['state_match_details']['failed_countries']),
                'failed_states_count': len(self.detailed_stats['state_match_details']['failed_states']),
                'top_failed_states': list(self.detailed_stats['state_match_details']['failed_states'])[:10]
            },
            'city_match_analysis': {
                'exact_matches': self.detailed_stats['city_match_details']['exact_matches'],
                'multiple_matches': self.detailed_stats['city_match_details']['multiple_matches'],
                'no_matches': self.detailed_stats['city_match_details']['no_matches'],
                'failed_cities_count': len(self.detailed_stats['city_match_details']['failed_cities']),
                'top_failed_cities': list(self.detailed_stats['city_match_details']['failed_cities'])[:10]
            },
            'data_quality_analysis': {
                'empty_country_codes': self.detailed_stats['data_quality_issues']['empty_country_codes'],
                'empty_state_names': self.detailed_stats['data_quality_issues']['empty_state_names'],
                'empty_city_names': self.detailed_stats['data_quality_issues']['empty_city_names'],
                'invalid_data_types': self.detailed_stats['data_quality_issues']['invalid_data_types'],
                'special_characters': self.detailed_stats['data_quality_issues']['special_characters']
            },
            'performance_metrics': {
                'avg_processing_time_per_record': round(self.stats['processing_time'] / total_records, 4) if total_records > 0 else 0,
                'database_query_count': self.detailed_stats['performance_metrics']['database_query_count'],
                'queries_per_record': round(self.detailed_stats['performance_metrics']['database_query_count'] / total_records, 2) if total_records > 0 else 0
            },
            'recommendations': self._generate_recommendations()
        }
        
        return report
    
    def _generate_recommendations(self) -> List[str]:
        """
        基于统计数据生成改进建议
        
        Returns:
            List[str]: 改进建议列表
        """
        recommendations = []
        
        # 基于成功率的建议
        success_rate = (self.stats['successful_matches'] / self.stats['total_records']) * 100 if self.stats['total_records'] > 0 else 0
        
        if success_rate < 50:
            recommendations.append("匹配成功率较低，建议检查数据质量和列名映射配置")
        elif success_rate < 80:
            recommendations.append("匹配成功率中等，可以通过数据清理提高匹配效果")
        
        # 基于州匹配失败的建议
        if self.stats['state_match_failures'] > 0:
            failed_countries = len(self.detailed_stats['state_match_details']['failed_countries'])
            if failed_countries > 5:
                recommendations.append(f"有{failed_countries}个国家的州匹配失败，建议检查国家代码的准确性")
            
            if self.detailed_stats['state_match_details']['no_matches'] > self.stats['total_records'] * 0.2:
                recommendations.append("超过20%的州无法匹配，建议检查州名的拼写和格式")
        
        # 基于城市匹配失败的建议
        if self.stats['city_match_failures'] > 0:
            if self.detailed_stats['city_match_details']['no_matches'] > self.stats['total_records'] * 0.3:
                recommendations.append("超过30%的城市无法匹配，建议检查城市名的拼写和格式")
        
        # 基于数据质量的建议
        data_quality = self.detailed_stats['data_quality_issues']
        if data_quality['empty_country_codes'] > 0:
            recommendations.append(f"发现{data_quality['empty_country_codes']}个空的国家代码，建议清理数据")
        
        if data_quality['empty_state_names'] > 0:
            recommendations.append(f"发现{data_quality['empty_state_names']}个空的州名，建议清理数据")
        
        if data_quality['empty_city_names'] > 0:
            recommendations.append(f"发现{data_quality['empty_city_names']}个空的城市名，建议清理数据")
        
        # 基于性能的建议
        queries_per_record = self.detailed_stats['performance_metrics']['database_query_count'] / self.stats['total_records'] if self.stats['total_records'] > 0 else 0
        if queries_per_record > 3:
            recommendations.append("数据库查询次数较多，建议优化查询逻辑或使用缓存")
        
        if not recommendations:
            recommendations.append("数据质量良好，匹配效果理想")
        
        return recommendations
    
    def export_failed_records(self, output_file: str = None) -> str:
        """
        导出匹配失败的记录到CSV文件
        
        Args:
            output_file (str, optional): 输出文件路径
            
        Returns:
            str: 输出文件的完整路径
        """
        if not self.failed_records:
            logger.warning("没有失败记录可导出")
            return None
        
        if output_file is None:
            # 生成默认文件名
            base_name = os.path.splitext(os.path.basename(self.output_csv))[0]
            output_file = os.path.join(self.output_dir, f"{base_name}_failed_records.csv")
        
        # 确保输出目录存在
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        
        # 转换为DataFrame并保存
        failed_df = pd.DataFrame(self.failed_records)
        failed_df.to_csv(output_file, index=False, encoding='utf-8')
        
        logger.info(f"失败记录已导出到: {output_file}")
        logger.info(f"共导出 {len(self.failed_records)} 条失败记录")
        
        return output_file
    
    def save_results(self, df: pd.DataFrame = None, output_file: str = None) -> str:
        """
        保存匹配结果到CSV文件，保持原始数据结构完整性
        
        Args:
            df (pd.DataFrame, optional): 要保存的DataFrame，如果为None则使用self.df
            output_file (str, optional): 输出文件路径，如果为None则使用self.output_csv
            
        Returns:
            str: 输出文件的完整路径
        """
        if df is None:
            df = self.df
        
        if df is None:
            raise ValueError("没有可保存的数据")
        
        if output_file is None:
            output_file = self.output_csv
        
        # 验证输出数据的完整性
        export_summary = self._validate_export_data(df)
        
        # 确保输出目录存在
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
            logger.info(f"创建输出目录: {output_dir}")
        
        # 准备导出数据
        export_df = self._prepare_export_data(df)
        
        # 保存到CSV，处理特殊字符和编码问题
        try:
            export_df.to_csv(output_file, index=False, encoding='utf-8', 
                            quoting=1, escapechar='\\')  # 使用引号包围所有字段，处理特殊字符
            logger.info(f"匹配结果已保存到: {output_file}")
            
            # 生成并保存导出摘要
            summary_file = self._save_export_summary(export_summary, output_file)
            logger.info(f"导出摘要已保存到: {summary_file}")
            
        except Exception as e:
            logger.error(f"保存CSV文件失败:", exc_info=True)
            raise
        
        return output_file
    
    def _validate_export_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        验证导出数据的完整性
        
        Args:
            df (pd.DataFrame): 要验证的DataFrame
            
        Returns:
            Dict[str, Any]: 验证结果和统计信息
        """
        from datetime import datetime
        
        # 基本统计
        total_records = len(df)
        geonameid_coverage = 0
        
        if 'geonameid' in df.columns:
            # 计算geonameid覆盖率
            valid_geonameid = df['geonameid'].notna().sum()
            geonameid_coverage = (valid_geonameid / total_records) * 100 if total_records > 0 else 0
        
        # 数据质量指标
        null_counts = df.isnull().sum().to_dict()
        duplicate_count = df.duplicated().sum()
        
        # 列信息
        column_info = {
            'total_columns': len(df.columns),
            'column_names': list(df.columns),
            'original_columns': [col for col in df.columns if col != 'geonameid'],
            'added_columns': ['geonameid'] if 'geonameid' in df.columns else []
        }
        
        summary = {
            'export_timestamp': datetime.now().isoformat(),
            'total_records': total_records,
            'geonameid_coverage': round(geonameid_coverage, 2),
            'data_quality': {
                'null_counts': null_counts,
                'duplicate_records': duplicate_count,
                'data_types': df.dtypes.astype(str).to_dict()
            },
            'column_info': column_info,
            'file_size_estimate': df.memory_usage(deep=True).sum()  # 字节
        }
        
        return summary
    
    def _prepare_export_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        准备导出数据，保持原始列的顺序，在末尾添加geonameid列
        
        Args:
            df (pd.DataFrame): 原始DataFrame
            
        Returns:
            pd.DataFrame: 准备好的导出DataFrame
        """
        export_df = df.copy()
        # export_df = export_df.rename(columns={k:v for k,v in self.column_mapping.items()})
        
        # 确保geonameid列在最后
        if 'geonameid' in export_df.columns:
            # 重新排列列顺序：原始列 + geonameid
            original_columns = [col for col in export_df.columns if col != 'geonameid']
            new_column_order = original_columns + ['geonameid']
            export_df = export_df[new_column_order]
        
        # 处理特殊字符和数据类型
        for col in export_df.columns:
            if export_df[col].dtype == 'object':
                # 处理字符串列中的特殊字符
                export_df[col] = export_df[col].astype(str).replace({
                    '\n': ' ',  # 替换换行符
                    '\r': ' ',  # 替换回车符
                    '\t': ' ',  # 替换制表符
                    '"': '""'   # 转义双引号
                })
        
        return export_df
    
    def _save_export_summary(self, summary: Dict[str, Any], output_file: str) -> str:
        """
        保存导出摘要信息
        
        Args:
            summary (Dict[str, Any]): 导出摘要数据
            output_file (str): 主输出文件路径
            
        Returns:
            str: 摘要文件路径
        """
        import json
        import numpy as np
        
        # 生成摘要文件路径
        base_name = os.path.splitext(os.path.basename(output_file))[0]
        summary_file = os.path.join(os.path.dirname(output_file), f"{base_name}_export_summary.json")
        
        # 添加文件信息
        summary['output_file'] = output_file
        summary['summary_file'] = summary_file
        
        # 转换numpy类型为Python原生类型
        def convert_numpy_types(obj):
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, dict):
                return {k: convert_numpy_types(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_numpy_types(item) for item in obj]
            return obj
        
        # 转换摘要数据
        summary_converted = convert_numpy_types(summary)
        
        # 保存摘要
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary_converted, f, ensure_ascii=False, indent=2)
        
        return summary_file
    
    def create_output_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        创建输出摘要信息（公共接口）
        
        Args:
            df (pd.DataFrame): 输出的DataFrame
            
        Returns:
            Dict[str, Any]: 输出摘要
        """
        return self._validate_export_data(df)
    
    def export_results(self, df_with_geonameid: pd.DataFrame, output_path: str) -> bool:
        """
        导出包含geonameid的结果数据（公共接口）
        
        Args:
            df_with_geonameid (pd.DataFrame): 包含geonameid的DataFrame
            output_path (str): 输出文件路径
            
        Returns:
            bool: 导出是否成功
        """
        try:
            self.save_results(df_with_geonameid, output_path)
            return True
        except Exception as e:
            logger.error(f"导出结果失败:", exc_info=True)
            return False
    
    def save_match_report(self, output_file: str = None, format_type: str = 'txt') -> str:
        """
        保存详细的匹配统计报告到文件
        
        Args:
            output_file (str, optional): 输出文件路径
            format_type (str): 报告格式 ('txt' 或 'json')
            
        Returns:
            str: 报告文件的完整路径
        """
        # 生成详细报告
        detailed_report = self.generate_detailed_match_report()
        
        if 'error' in detailed_report:
            logger.warning(detailed_report['error'])
            return None
        
        if output_file is None:
            # 生成默认文件名
            base_name = os.path.splitext(os.path.basename(self.output_csv))[0]
            extension = 'json' if format_type == 'json' else 'txt'
            output_file = os.path.join(self.output_dir, f"{base_name}_detailed_match_report.{extension}")
        
        # 确保输出目录存在
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        
        if format_type == 'json':
            # 保存为JSON格式
            import json
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(detailed_report, f, ensure_ascii=False, indent=2)
        else:
            # 保存为文本格式
            report_content = self._format_report_as_text(detailed_report)
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(report_content)
        
        logger.info(f"详细匹配报告已保存到: {output_file}")
        return output_file
    
    def _format_report_as_text(self, report: Dict[str, Any]) -> str:
        """
        将报告格式化为文本格式
        
        Args:
            report (Dict[str, Any]): 详细报告数据
            
        Returns:
            str: 格式化的文本报告
        """
        from datetime import datetime
        
        content = f"""地理匹配详细统计报告
{'='*60}

总体统计:
{'-'*30}
- 总记录数: {report['summary']['total_records']:,}
- 成功匹配: {report['summary']['successful_matches']:,}
- 成功率: {report['summary']['success_rate']}%
- 州匹配失败: {report['summary']['state_failures']:,} ({report['summary']['state_failure_rate']}%)
- 城市匹配失败: {report['summary']['city_failures']:,} ({report['summary']['city_failure_rate']}%)
- 处理时间: {report['summary']['processing_time']:.2f}秒

州匹配分析:
{'-'*30}
- 精确匹配: {report['state_match_analysis']['exact_matches']:,}
- 多重匹配: {report['state_match_analysis']['multiple_matches']:,}
- 无匹配: {report['state_match_analysis']['no_matches']:,}
- 失败国家数: {report['state_match_analysis']['failed_countries_count']}
- 失败州数: {report['state_match_analysis']['failed_states_count']}
"""
        
        if report['state_match_analysis']['failed_countries']:
            content += f"\n失败的国家代码: {', '.join(report['state_match_analysis']['failed_countries'][:10])}"
            if len(report['state_match_analysis']['failed_countries']) > 10:
                content += f" (显示前10个，共{len(report['state_match_analysis']['failed_countries'])}个)"
        
        content += f"""\n\n城市匹配分析:
{'-'*30}
- 精确匹配: {report['city_match_analysis']['exact_matches']:,}
- 多重匹配: {report['city_match_analysis']['multiple_matches']:,}
- 无匹配: {report['city_match_analysis']['no_matches']:,}
- 失败城市数: {report['city_match_analysis']['failed_cities_count']}
"""
        
        if report['city_match_analysis']['top_failed_cities']:
            content += f"\n常见失败城市: {', '.join(report['city_match_analysis']['top_failed_cities'])}"
        
        content += f"""\n\n数据质量分析:
{'-'*30}
- 空国家代码: {report['data_quality_analysis']['empty_country_codes']:,}
- 空州名: {report['data_quality_analysis']['empty_state_names']:,}
- 空城市名: {report['data_quality_analysis']['empty_city_names']:,}
- 无效数据类型: {report['data_quality_analysis']['invalid_data_types']:,}
- 特殊字符问题: {report['data_quality_analysis']['special_characters']:,}

性能指标:
{'-'*30}
- 平均每条记录处理时间: {report['performance_metrics']['avg_processing_time_per_record']:.4f}秒
- 数据库查询总数: {report['performance_metrics']['database_query_count']:,}
- 每条记录平均查询数: {report['performance_metrics']['queries_per_record']:.2f}

改进建议:
{'-'*30}
"""
        
        for i, recommendation in enumerate(report['recommendations'], 1):
            content += f"{i}. {recommendation}\n"
        
        content += f"\n\n报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        return content
    
    def process_csv_file(self) -> Dict[str, Any]:
        """
        完整的CSV地理匹配处理流程
        
        Returns:
            Dict[str, Any]: 处理结果摘要
        """
        logger.info("开始CSV地理匹配处理流程")
        
        try:
            # 1. 加载CSV数据
            self.load_csv_data()
            
            # 2. 验证列
            self.validate_columns()
            
            # 3. 执行地理匹配
            result_df = self.match_geography_batch()
            
            # 4. 保存结果
            output_file = self.save_results(result_df)
            
            # 5. 保存详细匹配报告（文本和JSON格式）
            report_file_txt = self.save_match_report(format_type='txt')
            report_file_json = self.save_match_report(format_type='json')
            
            # 6. 导出失败记录（如果有）
            failed_records_file = None
            if self.failed_records:
                failed_records_file = self.export_failed_records()
            
            # 7. 生成详细的处理摘要
            detailed_report = self.generate_detailed_match_report()
            
            summary = {
                'input_file': self.input_csv,
                'output_file': output_file,
                'report_files': {
                    'text_report': report_file_txt,
                    'json_report': report_file_json,
                    'failed_records': failed_records_file
                },
                'statistics': {
                    'total_records': len(result_df),
                    'successful_matches': self.stats['successful_matches'],
                    'state_match_failures': self.stats['state_match_failures'],
                    'city_match_failures': self.stats['city_match_failures'],
                    'data_validation_errors': self.stats['data_validation_errors'],
                    'success_rate': detailed_report['summary']['success_rate'],
                    'processing_time': self.stats['processing_time']
                },
                'data_quality': detailed_report['data_quality_analysis'],
                'performance_metrics': detailed_report['performance_metrics'],
                'recommendations': detailed_report['recommendations']
            }
            
            logger.info("CSV地理匹配处理完成")
            logger.info(f"成功率: {detailed_report['summary']['success_rate']}%")
            logger.info(f"处理时间: {self.stats['processing_time']:.2f}秒")
            
            if self.failed_records:
                logger.info(f"失败记录数: {len(self.failed_records)}")
                logger.info(f"失败记录已导出到: {failed_records_file}")
            
            return summary
            
        except Exception as e:
            logger.error(f"CSV地理匹配处理失败:", exc_info=True)
            raise