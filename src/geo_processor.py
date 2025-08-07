import time
import logging
import pandas as pd
import os
import sys

from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional

# 导入自定义模块
from geonames_parser import GeonamesParser
from alias_processor import AliasProcessor
from geo_hierarchy import GeoHierarchy
from csv_exporter import CSVExporter
from sqlite_integrator import SQLiteIntegrator
from duplicate_checker import DuplicateChecker
from csc_processor import CSCProcessor

logger = logging.getLogger(__name__)

class GeonamesProcessor:
    """GeoNames数据处理器主类"""
    
    def __init__(self, 
                 data_file='source_data/geoname/allCountries.txt',
                 csv_output_dir='output/csv_output',
                 sqlite_output_dir='output/sqlite_output',
                 chunk_size=10000,
                 enable_csc=False,
                 csc_file_path='source_data/csc/cities.txt'):
        """
        初始化GeoNames处理器
        
        Args:
            data_file (str): GeoNames数据文件路径
            csv_output_dir (str): CSV输出目录
            sqlite_output_dir (str): SQLite输出目录
            chunk_size (int): 数据分块大小
            enable_csc (bool): 是否启用CSC数据处理
            csc_file_path (str): CSC数据文件路径
        """
        self.data_file = data_file
        self.csv_output_dir = csv_output_dir
        self.sqlite_output_dir = sqlite_output_dir
        self.chunk_size = chunk_size
        self.enable_csc = enable_csc
        self.csc_file_path = csc_file_path
        
        # 初始化处理模块
        self.parser = None
        self.alias_processor = AliasProcessor()
        self.geo_hierarchy = GeoHierarchy()
        self.csv_exporter = CSVExporter(csv_output_dir)
        self.sqlite_integrator = SQLiteIntegrator(f'{sqlite_output_dir}/geonames.db')
        self.duplicate_checker = None  # 延迟初始化，等数据库创建后再初始化
        self.csc_processor = CSCProcessor() if enable_csc else None
        
        # 处理统计信息
        self.stats = {
            'start_time': None,
            'end_time': None,
            'total_records_processed': 0,
            'adm1_records': 0,
            'adm2_records': 0,
            'states_generated': 0,
            'cities_generated': 0,
            'state_names_generated': 0,
            'city_names_generated': 0,
            'processing_time': 0,
            # CSC处理统计
            'csc_records_loaded': 0,
            'csc_wikidata_queries': 0,
            'csc_geonameid_found': 0,
            'csc_admin_codes_found': 0,
            # csc_cities_matched字段已删除
            'csc_records_imported': 0,

            'csc_city_names_generated': 0,
            'csc_state_names_generated': 0
        }
        
        logger.info(f"初始化GeoNames处理器")
        logger.info(f"数据文件: {data_file}")
        logger.info(f"CSV输出目录: {csv_output_dir}")
        logger.info(f"SQLite输出目录: {sqlite_output_dir}")
    

    

    

    
    def validate_environment(self) -> bool:
        """
        验证运行环境和依赖文件
        
        Returns:
            bool: 验证是否通过
        """
        logger.info("验证运行环境...")
        
        # 检查数据文件
        if not os.path.exists(self.data_file):
            logger.error(f"数据文件不存在: {self.data_file}")
            return False
        
        # 创建输出目录和缓存目录
        try:
            os.makedirs(self.csv_output_dir, exist_ok=True)
            os.makedirs(self.sqlite_output_dir, exist_ok=True)
            # 创建缓存目录
            cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'cache')
            os.makedirs(cache_dir, exist_ok=True)
            logger.info("输出目录和缓存目录创建成功")
        except Exception as e:
            logger.error(f"创建目录失败: {e}")
            return False
        
        # 检查必需的模块
        required_modules = [
            'geonames_parser', 'alias_processor', 'geo_hierarchy',
            'csv_exporter', 'sqlite_integrator', 'duplicate_checker'
        ]
        
        for module in required_modules:
            try:
                __import__(module)
            except ImportError as e:
                logger.error(f"缺少必需的模块: {module} - {e}")
                return False
        
        logger.info("环境验证通过")
        return True
    
    def correct_geonameid_mapping(self, geonameid_mapping: Dict[str, str], original_records: Dict[int, Dict],
                                 states_df: pd.DataFrame, cities_df: pd.DataFrame) -> Dict[int, Dict]:
        """
        修正geonameid_mapping的键值，将其强制转换为ADM1和ADM2区域的geonameid
        
        Args:
            geonameid_mapping: 原始的geonameid映射字典 {wikidata_id: geonameid}
            states_df: 州级数据DataFrame
            cities_df: 城市级数据DataFrame
            
        Returns:
            Dict[str, str]: 修正后的geonameid映射字典 {wikidata_id: geonameid}
        """
        logger.info("开始修正geonameid_mapping的键值...")
        
        corrected_mapping = {}
        correction_stats = {
            'total_entries': len(geonameid_mapping),
            'corrected_entries': 0,
            'failed_corrections': 0,
            'already_correct': 0
        }
        
        # 处理每个geonameid
        for wikidata_id, original_geonameid in geonameid_mapping.items():
            try:
                # 获取原始记录
                original_record = original_records.get(int(original_geonameid))
                if original_record is None:
                    # logger.warning(f"无法找到geonameid {original_geonameid} 的原始记录")
                    corrected_mapping[wikidata_id] = original_geonameid
                    correction_stats['failed_corrections'] += 1
                    continue
                
                # 提取行政区代码
                country_code = original_record.get('country_code', '')
                admin1_code = original_record.get('admin1_code', '')
                admin2_code = original_record.get('admin2_code', '')
                
                if not country_code:
                    logger.warning(f"geonameid {original_geonameid} 缺少country_code")
                    corrected_mapping[wikidata_id] = original_geonameid
                    correction_stats['failed_corrections'] += 1
                    continue
                    
                ori_matches = pd.concat([
                    cities_df[(
                        # 或这个geonameid已经是合法的城市geonameid，直接返回
                        (cities_df['geonameid'] == int(original_geonameid))
                    )], 
                    states_df[(
                        # 或这个geonameid已经是合法的城市geonameid，直接返回
                        (states_df['geonameid'] == int(original_geonameid))
                    )], 

                ])
                if not ori_matches.empty:
                    correct_geonameid_str = original_geonameid
                else:
                    correct_geonameid_str = None
                    # # 查找正确的ADM1/ADM2 geonameid
                    # correct_geonameid_str = self._find_correct_admin_geonameid(
                    #     int(original_geonameid), country_code, admin1_code, admin2_code, states_df, cities_df
                    # )
                
                csc_df = self.csc_processor.cleaned_data
                for _, row in csc_df[csc_df['wikiDataId'] == wikidata_id].iterrows():
                    if abs(row['latitude'] - original_record['latitude']) > 10 or abs(row['longitude'] - original_record['longitude']) > 10:
                        logger.warning(f"geonameid {original_geonameid} 与原始经纬度偏差过大 {country_code, admin1_code, admin2_code}")
                        correct_geonameid_str = None
                        break

                if correct_geonameid_str:
                    try:
                        correct_geonameid = int(correct_geonameid_str)
                        if correct_geonameid != original_geonameid:
                            # 使用修正后的geonameid作为键
                            corrected_mapping[wikidata_id] = correct_geonameid
                            correction_stats['corrected_entries'] += 1
                            logger.debug(f"修正geonameid: {original_geonameid} -> {correct_geonameid}")
                        else:
                            # geonameid已经正确
                            corrected_mapping[wikidata_id] = original_geonameid
                            correction_stats['already_correct'] += 1
                    except ValueError:
                        logger.error(f"无法将geonameid字符串转换为整数: {correct_geonameid_str}")
                        # corrected_mapping[wikidata_id] = original_geonameid
                        correction_stats['failed_corrections'] += 1
                else:
                    # 无法找到正确的geonameid，保持原样
                    # corrected_mapping[wikidata_id] = original_geonameid
                    correction_stats['failed_corrections'] += 1
                    logger.warning(f"无法为geonameid {original_geonameid} 找到正确的ADM1/ADM2对应项 {country_code, admin1_code, admin2_code}")
                    
            except Exception as e:
                logger.error(f"处理geonameid {original_geonameid} 时出错:", exc_info=True)
                # corrected_mapping[wikidata_id] = original_geonameid
                correction_stats['failed_corrections'] += 1
        
        # 输出统计信息
        logger.info("geonameid_mapping修正完成")
        logger.info(f"总条目数: {correction_stats['total_entries']}")
        logger.info(f"成功修正: {correction_stats['corrected_entries']}")
        logger.info(f"已经正确: {correction_stats['already_correct']}")
        logger.info(f"修正失败: {correction_stats['failed_corrections']}")
        
        return corrected_mapping

    def _find_correct_admin_geonameid(self, geonameid: str, country_code: str, admin1_code: str, 
                                     admin2_code: str, states_df: pd.DataFrame, 
                                     cities_df: pd.DataFrame) -> Optional[str]:
        """
        根据行政区代码查找正确的ADM1或ADM2级别的geonameid
        
        Args:
            country_code: 国家代码
            admin1_code: 一级行政区代码
            admin2_code: 二级行政区代码
            states_df: 州级数据DataFrame
            cities_df: 城市级数据DataFrame
            
        Returns:
            Optional[str]: 正确的geonameid，如果找不到则返回None
        """
        try:
            # 优先查找ADM2级别（城市）
            if not cities_df.empty:
                if not pd.isna(admin2_code):
                    city_matches = cities_df[
                        (
                            # 查找与给定代码匹配的城市
                            (cities_df['country_code'] == country_code) &
                            (cities_df['admin1_code'] == admin1_code) &
                            (cities_df['admin2_code'] == admin2_code)
                        )
                    ]
                    
                    if not city_matches.empty:
                        return str(city_matches.iloc[0]['geonameid'])
            # # 如果没有找到ADM2，查找ADM1级别（州）
            # if not pd.isna(admin1_code) and not states_df.empty:
            #     state_matches = states_df[
            #         (states_df['country_code'] == country_code) &
            #         (states_df['admin1_code'] == admin1_code)
            #     ]
                
            #     if not state_matches.empty:
            #         return str(state_matches.iloc[0]['geonameid'])
            
            return None
            
        except Exception as e:
            logger.error(f"查找正确geonameid时出错: {e}", exc_info=True)
            return None
    
    def process_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        处理GeoNames数据
        
        Returns:
            Tuple: (states_df, cities_df, state_names_df, city_names_df, csc_mapping_df)
        """
        logger.info("开始处理GeoNames数据...")
        start_time = time.time()
        

        
        # 初始化解析器
        self.parser = GeonamesParser(self.data_file, chunk_size=self.chunk_size)
        
        # 存储处理结果
        all_adm1_records = []
        all_adm2_records = []
        aliases_dict = {}
        
        chunk_count = 0
        
        try:
            geonameid_mapping = {}
            wikidata_ids = {}
            if self.enable_csc and self.csc_processor:
                logger.info("开始CSC数据处理集成1...")
                try:
                    # 调用CSC处理集成接口
                    geonameid_mapping, wikidata_ids = self.csc_processor.process_csc_integration1()
                except Exception as e:
                    logger.warning(f"CSC数据处理集成1出错: {e}")

            # geonameid_mapping is {wikidata_id: str(geonameid)} 映射字典
            # 修正geonameid_mapping的键值，将其强制转换为ADM1和ADM2区域的geonameid
            if geonameid_mapping:
                logger.info(f"开始修正geonameid_mapping，原始条目数: {len(geonameid_mapping)}")
                # 注意：此时states_df和cities_df还未构建，需要在后续步骤中进行修正
                geonameid_to_correct = set(int(c) for c in geonameid_mapping.values())
            else:
                geonameid_to_correct = set()

            found_geonameid = {}

            # 分块处理数据
            for rawchunk, chunk in self.parser.parse_chunks(raw=True):
                chunk_count += 1
                logger.info(f"处理第 {chunk_count} 个数据块，包含 {len(chunk)} 条记录")

                geonameid_chunk = rawchunk[rawchunk['geonameid'].isin(geonameid_to_correct)]

                # merge geonameid_chunk to chunk
                chunk = pd.concat([chunk, geonameid_chunk]).drop_duplicates()

                found_geonameid.update({row['geonameid']: row for _, row in geonameid_chunk.iterrows()})

                for _, record in chunk.iterrows():
                    self.stats['total_records_processed'] += 1

                    # 处理别名
                    if pd.notna(record['alternatenames']) and record['alternatenames']:
                        alternatenames_str = record['alternatenames']
                        alternatenames = []
                        if pd.isna(alternatenames_str) or not alternatenames_str.strip():
                            pass
                        else:
                            # 解析逗号分隔的别名
                            alternatenames = [alias.strip() for alias in alternatenames_str.split(',') if alias.strip()]
                        aliases = self.alias_processor.process_alternatenames([record['name'], record['asciiname']] + alternatenames)
                        aliases_dict[record['geonameid']] = aliases
                    
                    def isAdm1():
                        if pd.isna(record['feature_code']):
                            return False
                        if record['feature_code'] in ['ADM1', 'ADM1H']:
                            return True
                        if pd.isna(record['admin1_code']) or record['admin1_code'] == '00': #fuck geoname
                            return True
                        return False
                    # 分类记录
                    if isAdm1():
                        all_adm1_records.append(record)
                        self.stats['adm1_records'] += 1
                    # elif record['feature_code'] == 'ADM2' or record['feature_code'] == 'PPLA2':
                    else:
                        all_adm2_records.append(record)
                        self.stats['adm2_records'] += 1
                
                # 定期输出进度
                if chunk_count % 10 == 0:
                    logger.info(f"已处理 {chunk_count} 个数据块，总记录数: {self.stats['total_records_processed']}")
            
            logger.info(f"数据解析完成，总记录数: {self.stats['total_records_processed']}")
            logger.info(f"ADM1记录数: {self.stats['adm1_records']}")
            logger.info(f"ADM2记录数: {self.stats['adm2_records']}")
            
            # 构建DataFrame
            adm1_df = pd.DataFrame(all_adm1_records) if all_adm1_records else pd.DataFrame()
            adm2_df = pd.DataFrame(all_adm2_records) if all_adm2_records else pd.DataFrame()
            
            logger.info("开始构建地理层级关系...")
            
            # 构建地理层级关系
            if not adm1_df.empty:
                states_df = self.geo_hierarchy.build_state_records(adm1_df)
                self.stats['states_generated'] = len(states_df)
            else:
                states_df = pd.DataFrame()
            
            if not adm2_df.empty:
                cities_df = self.geo_hierarchy.build_city_records(adm2_df)
                self.stats['cities_generated'] = len(cities_df)
            else:
                cities_df = pd.DataFrame()
            
            logger.info("基础地理层级关系构建完成")
            logger.info(f"生成states记录: {self.stats['states_generated']}")
            logger.info(f"生成cities记录: {self.stats['cities_generated']}")
            
            # CSC数据处理集成
            csc_aliases_dict = {}
            csc_mapping_df = pd.DataFrame()
            if False and self.enable_csc and self.csc_processor:
                # 修正geonameid_mapping的键值
                logger.info("开始修正geonameid_mapping的键值...")
                logger.info(f"寻找到 {len(found_geonameid)} 个geonameid")
                try:
                    corrected_geonameid_mapping = self.correct_geonameid_mapping(
                        geonameid_mapping, found_geonameid, states_df, cities_df
                    )
                    # 更新原始映射
                    geonameid_mapping = corrected_geonameid_mapping
                    logger.info(f"geonameid_mapping修正完成，最终条目数: {len(geonameid_mapping)}")
                except Exception as e:
                    logger.error(f"修正geonameid_mapping时出错: {e}")
                    logger.warning("将继续使用原始geonameid_mapping")

                logger.info("开始CSC数据处理集成2...")
                try:
                    # 调用CSC处理集成接口
                    csc_result = self.csc_processor.process_csc_integration2(geonameid_mapping, wikidata_ids)

                    # 获取CSC别名字典和映射数据
                    csc_aliases_dict = csc_result.get('csc_aliases_dict', {})
                    csc_mapping_df = csc_result.get('csc_mapping_df', pd.DataFrame())
                    
                    # 更新统计信息
                    csc_stats = csc_result['stats']
                    self.stats.update({
                        'csc_records_loaded': csc_stats.get('total_records', 0),
                        'csc_wikidata_queries': csc_stats.get('valid_wikidata_ids', 0),
                        'csc_geonameid_found': csc_stats.get('geonameid_matches', 0),
                        'csc_admin_codes_found': csc_stats.get('states_generated', 0),

                        'csc_records_imported': csc_stats.get('cities_generated', 0) + csc_stats.get('states_generated', 0),
                        'csc_city_names_generated': csc_stats.get('cities_generated', 0),
                        'csc_state_names_generated': csc_stats.get('states_generated', 0)
                    })
                    
                    logger.info("CSC数据处理集成完成")
                    logger.info(f"CSC统计: 城市{csc_stats.get('cities_generated', 0)}, 州{csc_stats.get('states_generated', 0)}, 映射{len(csc_mapping_df)}")
                    
                except Exception as e:
                    logger.error(f"CSC数据处理集成失败: {e}", exc_info=True)
                    # CSC处理失败不影响主流程，继续执行
            
            # 生成名称映射（包含CSC别名）
            logger.info("开始生成名称映射...")
            if not states_df.empty:
                state_names_df = self.geo_hierarchy.create_state_names_mapping(states_df, aliases_dict, csc_aliases_dict)
                self.stats['state_names_generated'] = len(state_names_df)
            else:
                state_names_df = pd.DataFrame()
            
            if not cities_df.empty:
                city_names_df = self.geo_hierarchy.create_city_names_mapping(cities_df, states_df, aliases_dict, csc_aliases_dict)
                self.stats['city_names_generated'] = len(city_names_df)
            else:
                city_names_df = pd.DataFrame()
            
            logger.info("名称映射生成完成")
            logger.info(f"生成state_names记录: {self.stats['state_names_generated']}")
            logger.info(f"生成city_names记录: {self.stats['city_names_generated']}")
            
            # 记录处理完成时间
            end_time = time.time()
            processing_time = end_time - start_time
            
            logger.info(f"数据处理完成: 耗时 {processing_time:.2f}s, 处理 {self.stats['total_records_processed']} 条记录")
            
            return states_df, cities_df, state_names_df, city_names_df, csc_mapping_df
            
        except Exception as e:
            logger.error(f"数据处理过程中出错: {e}")
            raise
    
    def export_data(self, states_df: pd.DataFrame, cities_df: pd.DataFrame, 
                   state_names_df: pd.DataFrame, city_names_df: pd.DataFrame,
                   csc_mapping_df: pd.DataFrame = None) -> None:
        """
        导出数据到CSV和SQLite
        
        Args:
            states_df: states数据
            cities_df: cities数据
            state_names_df: state_names数据
            city_names_df: city_names数据
            csc_mapping_df: CSC映射数据（可选）
        """
        logger.info("开始导出数据...")
        start_time = time.time()
        
        try:
            # 导出CSV文件
            logger.info("导出CSV文件...")
            csv_start_time = time.time()
            
            try:
                self.csv_exporter.export_all(states_df, cities_df, state_names_df, city_names_df, csc_mapping_df)
                csv_time = time.time() - csv_start_time
                total_records = len(states_df) + len(cities_df) + len(state_names_df) + len(city_names_df) + len(csc_mapping_df)
                logger.info(f"CSV导出完成: 耗时 {csv_time:.2f}s, 处理 {total_records} 条记录")
            except Exception as e:
                logger.error(f"CSV导出失败: {e}")
                raise
            
            # 获取导出摘要
            export_summary = self.csv_exporter.get_export_summary()
            logger.info("CSV导出完成")
            for file_info in export_summary['files']:
                if file_info['exists']:
                    size_mb = file_info['size_bytes'] / (1024 * 1024)
                    logger.info(f"  {file_info['filename']}: {size_mb:.2f}MB, {file_info['record_count']} 记录")
            
            # 输出数据来源统计
            if False and self.enable_csc:
                logger.info("数据来源统计:")
                logger.info(f"  GeoNames数据: 城市{self.stats['cities_generated']}, 州{self.stats['states_generated']}, 城市名称{self.stats['city_names_generated']}, 州名称{self.stats['state_names_generated']}")
                logger.info(f"  CSC数据: 城市名称{self.stats.get('csc_city_names_generated', 0)}, 州名称{self.stats.get('csc_state_names_generated', 0)}")
                logger.info(f"  合并后总计: 城市{len(cities_df)}, 州{len(states_df)}, 城市名称{len(city_names_df)}, 州名称{len(state_names_df)}")
            
            # 导入SQLite数据库
            logger.info("导入SQLite数据库...")
            sqlite_start_time = time.time()
            
            try:
                self.sqlite_integrator.setup_database(self.csv_output_dir)

                # 获取数据库统计
                db_stats = self.sqlite_integrator.get_database_stats()
                sqlite_time = time.time() - sqlite_start_time
                
                logger.info("SQLite导入完成")
                if 'database_size_bytes' in db_stats:
                    size_mb = db_stats['database_size_bytes'] / (1024 * 1024)
                    logger.info(f"数据库大小: {size_mb:.2f}MB")
                for table, info in db_stats.get('tables', {}).items():
                    logger.info(f"  {table}: {info.get('record_count', 0)} 记录")
                
                # 记录SQLite导入时间
                total_db_records = sum(info.get('record_count', 0) for info in db_stats.get('tables', {}).values())
                logger.info(f"SQLite导入完成: 耗时 {sqlite_time:.2f}s, 处理 {total_db_records} 条记录")
                
            except Exception as e:
                logger.error(f"SQLite导入失败: {e}")
                raise
            
            # 记录总导出时间
            total_export_time = time.time() - start_time
            total_records = len(states_df) + len(cities_df) + len(state_names_df) + len(city_names_df)
            logger.info(f"数据导出总计完成: 耗时 {total_export_time:.2f}s, 处理 {total_records} 条记录")
            
        except Exception as e:
            logger.error(f"数据导出过程中出错: {e}")
            raise
    
    def check_duplicates(self) -> Dict[str, Any]:
        """
        检查重复项
        
        Returns:
            Dict: 重复项统计信息
        """
        logger.info("开始检查重复项...")
        start_time = time.time()
        
        try:
            # 初始化重复检查器（此时数据库已经创建）
            if self.duplicate_checker is None:
                try:
                    self.duplicate_checker = DuplicateChecker(f'{self.sqlite_output_dir}/geonames.db')
                except Exception as e:
                    logger.error(f"初始化重复检查器失败: {e}")
                    raise
            
            # 生成重复项报告
            report_file = f'{self.sqlite_output_dir}/duplicate_report.txt'
            try:
                self.duplicate_checker.generate_duplicate_report(report_file)
                logger.info(f"重复项报告已生成: {report_file}")
            except Exception as e:
                logger.error(f"生成重复项报告失败: {e}")
                raise
            
            # 获取统计信息
            try:
                duplicate_stats = self.duplicate_checker.get_duplicate_statistics()
            except Exception as e:
                logger.error(f"获取重复项统计失败: {e}")
                raise
            
            # 记录检查时间
            check_time = time.time() - start_time
            total_checked = (duplicate_stats.get('state_names', {}).get('total_records', 0) + 
                           duplicate_stats.get('city_names', {}).get('total_records', 0))
            logger.info(f"重复项检查完成: 耗时 {check_time:.2f}s, 检查 {total_checked} 条记录")
            
            logger.info("重复项检查完成")
            logger.info(f"State Names重复组: {duplicate_stats['state_names']['duplicate_groups']}")
            logger.info(f"State Names重复率: {duplicate_stats['state_names']['duplicate_rate']:.2f}%")
            logger.info(f"City Names重复组: {duplicate_stats['city_names']['duplicate_groups']}")
            logger.info(f"City Names重复率: {duplicate_stats['city_names']['duplicate_rate']:.2f}%")
            
            return duplicate_stats
            
        except Exception as e:
            logger.error(f"重复项检查过程中出错: {e}")
            raise
    
    def generate_final_report(self, duplicate_stats: Dict[str, Any]) -> None:
        """
        生成最终处理报告
        
        Args:
            duplicate_stats: 重复项统计信息
        """
        logger.info("生成最终处理报告...")
        
        try:
            report_file = f'{self.sqlite_output_dir}/processing_report.txt'
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write("GeoNames 数据处理完整报告\n")
                f.write("=" * 80 + "\n\n")
                
                # 基本信息
                f.write("## 处理基本信息\n\n")
                f.write(f"数据文件: {self.data_file}\n")
                f.write(f"开始时间: {self.stats['start_time']}\n")
                f.write(f"结束时间: {self.stats['end_time']}\n")
                f.write(f"处理耗时: {self.stats['processing_time']:.2f} 秒\n")
                f.write(f"分块大小: {self.chunk_size:,}\n\n")
                
                # 数据统计
                f.write("## 数据处理统计\n\n")
                f.write(f"总记录数: {self.stats['total_records_processed']:,}\n")
                f.write(f"ADM1记录数: {self.stats['adm1_records']:,}\n")
                f.write(f"ADM2记录数: {self.stats['adm2_records']:,}\n")
                f.write(f"生成states记录: {self.stats['states_generated']:,}\n")
                f.write(f"生成cities记录: {self.stats['cities_generated']:,}\n")
                f.write(f"生成state_names记录: {self.stats['state_names_generated']:,}\n")
                f.write(f"生成city_names记录: {self.stats['city_names_generated']:,}\n\n")
                
                # CSC数据统计
                if self.stats['csc_records_loaded'] > 0:
                    f.write("## CSC数据处理统计\n\n")
                    f.write(f"CSC记录加载数: {self.stats['csc_records_loaded']:,}\n")
                    f.write(f"CSC记录导入数: {self.stats['csc_records_imported']:,}\n")
                    f.write(f"CSC城市名称生成数: {self.stats['csc_city_names_generated']:,}\n")
                    f.write(f"CSC州/省名称生成数: {self.stats['csc_state_names_generated']:,}\n\n")
                
                # 性能统计
                f.write("## 性能统计\n\n")
                f.write(f"处理耗时: {self.stats['processing_time']:.2f} 秒\n")
                if self.stats['processing_time'] > 0:
                    records_per_second = self.stats['total_records_processed'] / self.stats['processing_time']
                    f.write(f"处理速度: {records_per_second:.1f} 记录/秒\n")
                f.write("\n")
                
                if duplicate_stats:
                    # 重复项统计
                    f.write("## 重复项检查结果\n\n")
                    f.write(f"State Names重复组: {duplicate_stats['state_names']['duplicate_groups']:,}\n")
                    f.write(f"State Names重复率: {duplicate_stats['state_names']['duplicate_rate']:.2f}%\n")
                    f.write(f"City Names重复组: {duplicate_stats['city_names']['duplicate_groups']:,}\n")
                    f.write(f"City Names重复率: {duplicate_stats['city_names']['duplicate_rate']:.2f}%\n\n")
                
                # 输出文件
                f.write("## 生成的输出文件\n\n")
                f.write(f"CSV文件目录: {self.csv_output_dir}/\n")
                f.write(f"  - states.csv\n")
                f.write(f"  - cities.csv\n")
                f.write(f"  - state_names.csv\n")
                f.write(f"  - city_names.csv\n\n")
                f.write(f"  - csc_mapping.csv\n\n")
                
                f.write(f"SQLite数据库: {self.sqlite_output_dir}/geonames.db\n")
                f.write(f"重复项报告: {self.sqlite_output_dir}/duplicate_report.txt\n")
                f.write(f"处理日志: geonames_processing.log\n\n")
                
                f.write("=" * 80 + "\n")
                f.write("处理完成\n")
                f.write("=" * 80 + "\n")
            
            logger.info(f"最终报告已生成: {report_file}")
            
        except Exception as e:
            logger.error(f"生成最终报告时出错: {e}")
            raise
    

    
    def generate_csc_report(self) -> None:
        """
        生成CSC处理报告
        """
        logger.info("生成CSC处理报告...")
        
        try:
            report_file = f'{self.sqlite_output_dir}/csc_processing_report.txt'
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write("CSC 数据集成处理报告\n")
                f.write("=" * 80 + "\n\n")
                
                # CSC处理统计
                f.write("## CSC数据处理统计\n\n")
                f.write(f"加载的CSC记录数: {self.stats['csc_records_loaded']:,}\n")
                f.write(f"Wikidata查询数: {self.stats['csc_wikidata_queries']:,}\n")
                f.write(f"获取的geonameid数: {self.stats['csc_geonameid_found']:,}\n")
                f.write(f"获取的adminCode数: {self.stats['csc_admin_codes_found']:,}\n")
                # 匹配的城市数统计已删除
                f.write(f"导入的CSC记录数: {self.stats['csc_records_imported']:,}\n")
                f.write(f"生成的CSC城市名称: {self.stats['csc_city_names_generated']:,}\n")
                f.write(f"生成的CSC州/省名称: {self.stats['csc_state_names_generated']:,}\n\n")
                
                # 处理质量指标
                if self.stats['csc_records_loaded'] > 0:
                    # 安全计算成功率，避免除零错误
                    wikidata_success_rate = (self.stats['csc_geonameid_found'] / self.stats['csc_wikidata_queries'] * 100) if self.stats['csc_wikidata_queries'] > 0 else 0
                    admin_success_rate = (self.stats['csc_admin_codes_found'] / self.stats['csc_geonameid_found'] * 100) if self.stats['csc_geonameid_found'] > 0 else 0
                    # 匹配成功率计算已删除 - 基于csc_cities_matched的计算不再可用
                    
                    f.write("## 处理质量指标\n\n")
                    f.write(f"Wikidata查询成功率: {wikidata_success_rate:.2f}%\n")
                    f.write(f"AdminCode获取成功率: {admin_success_rate:.2f}%\n\n")
                
                # 获取数据库统计
                try:
                    csc_stats = self.sqlite_integrator.get_csc_stats()
                    if 'error' not in csc_stats:
                        f.write("## CSC数据库统计\n\n")
                        f.write(f"CSC表总记录数: {csc_stats.get('total_records', 0):,}\n")
                        f.write(f"匹配成功记录数: {csc_stats.get('matched_records', 0):,}\n")
                        f.write(f"匹配成功率: {csc_stats.get('match_rate', 0):.2%}\n")
                        
                        # 按国家统计
                        country_stats = csc_stats.get('by_country', [])
                        if country_stats:
                            f.write(f"\n按国家统计（前10个）:\n")
                            for i, stat in enumerate(country_stats[:10]):
                                f.write(f"  {i+1}. {stat['country_code']}: {stat['count']:,} 记录\n")
                        
                        f.write("\n")
                except Exception as e:
                    f.write(f"获取CSC统计信息时出错: {e}\n\n")
                
                f.write("=" * 80 + "\n")
                f.write("CSC处理完成\n")
                f.write("=" * 80 + "\n")
            
            logger.info(f"CSC处理报告已生成: {report_file}")
            
        except Exception as e:
            logger.error(f"生成CSC报告时出错: {e}", exc_info=True)

    def run_full_pipeline(self) -> bool:
        """
        运行完整的数据处理流程
        
        Returns:
            bool: 处理是否成功
        """
        logger.info("=" * 80)
        logger.info("开始GeoNames数据处理流程")
        logger.info("=" * 80)
        
        self.stats['start_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        start_time = time.time()
        
        try:
            # 1. 验证环境
            logger.info("步骤1: 验证运行环境")
            if not self.validate_environment():
                logger.error("环境验证失败，处理终止")
                return False
            logger.info("环境验证完成")
            
            # 2. 处理数据
            logger.info("步骤2: 处理GeoNames数据")
            try:
                states_df, cities_df, state_names_df, city_names_df, csc_mapping_df = self.process_data()
                logger.info("数据处理完成")
            except Exception as e:
                logger.error(f"数据处理失败: {e}")
                raise
            
            # 3. 导出数据
            logger.info("步骤3: 导出数据到CSV和SQLite")
            try:
                self.export_data(states_df, cities_df, state_names_df, city_names_df, csc_mapping_df)
                logger.info("数据导出完成")
            except Exception as e:
                logger.error(f"数据导出失败: {e}")
                raise
            
            # # 4. 检查重复项 (我们目前摆烂了，把所有的大小城市都纳入进来了，所以重复项检查的意义不大)
            # logger.info("步骤4: 检查重复项")
            # try:
            #     duplicate_stats = self.check_duplicates()
            #     logger.info("重复项检查完成")
            # except Exception as e:
            #     logger.error(f"重复项检查失败: {e}")
            #     raise
            duplicate_stats = None
            
            # 5. 生成最终报告
            logger.info("步骤5: 生成最终报告")
            try:
                self.generate_final_report(duplicate_stats)
                logger.info("最终报告生成完成")
            except Exception as e:
                logger.error(f"报告生成失败: {e}")
                raise
            
            # 更新统计信息
            end_time = time.time()
            self.stats['end_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.stats['processing_time'] = end_time - start_time
            
            logger.info("=" * 80)
            logger.info("GeoNames数据处理流程完成")
            logger.info(f"总耗时: {self.stats['processing_time']:.2f} 秒")
            logger.info(f"处理速度: {self.stats['total_records_processed'] / self.stats['processing_time']:.1f} 记录/秒")
            logger.info("=" * 80)
            
            return True
            
        except Exception as e:
            logger.error(f"处理流程中出现错误: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
