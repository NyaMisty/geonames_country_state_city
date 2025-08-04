#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GeoNames地理层级关系模块

处理ADM1(state)和ADM2(city)之间的层级关系验证和管理
构建state、city、state_names、city_names数据表
"""

import pandas as pd
import logging
from typing import Dict, List, Tuple, Optional

class GeoHierarchy:
    """地理层级关系管理器"""
    
    def __init__(self):
        """初始化地理层级关系管理器"""
        self.logger = logging.getLogger(__name__)
        
    def validate_hierarchy(self, df: pd.DataFrame) -> bool:
        """验证地理层级关系的一致性
        
        Args:
            df: 包含ADM1和ADM2数据的DataFrame
            
        Returns:
            bool: 层级关系是否一致
        """
        try:
            # 检查必要字段
            required_fields = ['geonameid', 'country_code', 'feature_code', 'admin1_code']
            missing_fields = [field for field in required_fields if field not in df.columns]
            if missing_fields:
                self.logger.error(f"缺少必要字段: {missing_fields}")
                return False
                
            # 验证ADM1记录的admin1_code不为空
            adm1_df = df[df['feature_code'] == 'ADM1']
            invalid_adm1 = adm1_df[adm1_df['admin1_code'].isna() | (adm1_df['admin1_code'] == '')]
            if not invalid_adm1.empty:
                self.logger.warning(f"发现{len(invalid_adm1)}个ADM1记录缺少admin1_code")
                
            # 验证ADM2记录的层级关系
            adm2_df = df[df['feature_code'] == 'ADM2']
            if not adm2_df.empty:
                # 检查ADM2是否有对应的ADM1
                adm1_keys = set(adm1_df.apply(lambda x: f"{x['country_code']}_{x['admin1_code']}", axis=1))
                adm2_keys = set(adm2_df.apply(lambda x: f"{x['country_code']}_{x['admin1_code']}", axis=1))
                orphaned_adm2 = adm2_keys - adm1_keys
                if orphaned_adm2:
                    self.logger.warning(f"发现{len(orphaned_adm2)}个ADM2记录没有对应的ADM1父级")
                    
            self.logger.info("地理层级关系验证完成")
            return True
            
        except Exception as e:
            self.logger.error(f"验证层级关系时出错: {e}")
            return False
    
    def build_state_records(self, adm1_df: pd.DataFrame) -> pd.DataFrame:
        """
        构建state数据表（保留完整的geonames原始信息）
        
        Args:
            adm1_df: ADM1级别的DataFrame
            
        Returns:
            pd.DataFrame: state表数据
        """
        try:
            # 保留所有原始字段
            states_df = adm1_df.copy()
            
            # 数据清理
            # states_df = states_df.dropna(subset=['geonameid', 'country_code', 'admin1_code'])
            states_df = states_df.dropna(subset=['geonameid', 'country_code'])
            states_df['geonameid'] = states_df['geonameid'].astype(int)
            
            # 去重（基于country_code + admin1_code）
            # states_df = states_df.drop_duplicates(subset=['country_code', 'admin1_code'])
            states_df = states_df.drop_duplicates(subset=['geonameid'])
            
            self.logger.info(f"构建state表完成，共{len(states_df)}条记录，包含{len(states_df.columns)}个字段")
            return states_df
            
        except Exception as e:
            self.logger.error(f"构建state记录时出错: {e}")
            return pd.DataFrame()
    
    def build_city_records(self, adm2_df: pd.DataFrame) -> pd.DataFrame:
        """
        构建city数据表（保留完整的geonames原始信息）
        
        Args:
            adm2_df: ADM2级别的DataFrame
            
        Returns:
            pd.DataFrame: city表数据
        """
        try:
            # 保留所有原始字段
            cities_df = adm2_df.copy()
            
            # 数据清理
            cities_df = cities_df.dropna(subset=['geonameid', 'country_code'])
            cities_df['geonameid'] = cities_df['geonameid'].astype(int)
            
            # 处理缺失的admin_code
            cities_df['admin1_code'] = cities_df['admin1_code'].fillna('')
            cities_df['admin2_code'] = cities_df['admin2_code'].fillna('')
            
            # 勿去重，保留原始结果
            # # 去重（基于geonameid）
            cities_df = cities_df.drop_duplicates(subset=['geonameid'])
            
            self.logger.info(f"构建city表完成，共{len(cities_df)}条记录，包含{len(cities_df.columns)}个字段")
            return cities_df
            
        except Exception as e:
            self.logger.error(f"构建city记录时出错: {e}")
            return pd.DataFrame()
    
    def create_state_names_mapping(self, states_df: pd.DataFrame, aliases_dict: Dict[int, List[str]], csc_aliases_dict: Dict[int, List[str]] = None) -> pd.DataFrame:
        """创建state_names映射表
        
        Args:
            states_df: state表数据
            aliases_dict: 别名字典 {geonameid: [alias1, alias2, ...]}
            csc_aliases_dict: CSC别名字典 {geonameid: [csc_alias1, csc_alias2, ...]}，可选
            
        Returns:
            pd.DataFrame: state_names表数据
        """
        try:
            state_names = []
            
            geoname_count = 0
            csc_count = 0
            for _, state in states_df.iterrows():
                geonameid = int(state['geonameid'])
                country_code = state['country_code']
                
                # 获取该state的所有别名
                aliases = aliases_dict.get(geonameid, [])
                
                # 获取CSC别名（如果提供）
                csc_aliases = []
                if csc_aliases_dict:
                    csc_aliases = csc_aliases_dict.get(geonameid, [])
                
                # 合并所有别名
                all_aliases = list(aliases) + list(csc_aliases)
                geoname_count += len(list(aliases))
                csc_count += len(list(csc_aliases))
                
                # 如果没有别名，至少添加主名称
                if not all_aliases:
                    all_aliases = [state['name']]
                    if pd.notna(state['asciiname']) and state['asciiname'] != state['name']:
                        all_aliases.append(state['asciiname'])
                
                # 为每个别名创建映射记录
                for alias in all_aliases:
                    if alias and alias.strip():  # 确保别名不为空
                        state_names.append({
                            'country_code': country_code,
                            'name': alias.strip(),
                            'geonameid': geonameid
                        })
            
            state_names_df = pd.DataFrame(state_names)
            
            # 勿去重，保留原始结果
            # # 去重（基于country_code + name）
            # if not state_names_df.empty:
            #     state_names_df = state_names_df.drop_duplicates(subset=['country_code', 'name'])
            
            self.logger.info(f"构建state_names表完成，共{len(state_names_df)}条(%d + %d)记录", geoname_count, csc_count)
            return state_names_df
            
        except Exception as e:
            self.logger.error(f"创建state_names映射时出错: {e}")
            return pd.DataFrame()
    
    def create_city_names_mapping(self, cities_df: pd.DataFrame, states_df: pd.DataFrame, aliases_dict: Dict[int, List[str]], csc_aliases_dict: Dict[int, List[str]] = None) -> pd.DataFrame:
        """创建city_names映射表
        
        Args:
            cities_df: city表数据
            states_df: state表数据
            aliases_dict: 别名字典 {geonameid: [alias1, alias2, ...]}
            csc_aliases_dict: CSC别名字典 {geonameid: [csc_alias1, csc_alias2, ...]}，可选
            
        Returns:
            pd.DataFrame: city_names表数据
        """
        try:
            # 创建state查找字典 (country_code + admin1_code -> geonameid)
            state_lookup = {}
            for _, state in states_df.iterrows():
                key = f"{state['country_code']}_{state['admin1_code']}"
                state_lookup[key] = int(state['geonameid'])
            
            city_names = []
            
            for _, city in cities_df.iterrows():
                geonameid = int(city['geonameid'])
                country_code = city['country_code']
                admin1_code = city['admin1_code']
                
                # 查找对应的state geonameid
                state_key = f"{country_code}_{admin1_code}"
                state_geonameid = state_lookup.get(state_key)
                
                if state_geonameid is None:
                    # 如果找不到对应的state，跳过或使用0作为默认值
                    self.logger.warning(f"City {geonameid} 找不到对应的state (country: {country_code}, admin1: {admin1_code})")
                    state_geonameid = 0
                
                # 获取该city的所有别名
                aliases = aliases_dict.get(geonameid, [])
                
                # 获取CSC别名（如果提供）
                csc_aliases = []
                if csc_aliases_dict:
                    csc_aliases = csc_aliases_dict.get(geonameid, [])
                
                # 合并所有别名
                all_aliases = list(aliases) + list(csc_aliases)
                
                # 如果没有别名，至少添加主名称
                if not all_aliases:
                    all_aliases = [city['name']]
                    if pd.notna(city['asciiname']) and city['asciiname'] != city['name']:
                        all_aliases.append(city['asciiname'])
                
                # 为每个别名创建映射记录
                for alias in all_aliases:
                    if alias and alias.strip():  # 确保别名不为空
                        city_names.append({
                            'country_code': country_code,
                            'state_geonameid': state_geonameid,
                            'name': alias.strip(),
                            'geonameid': geonameid
                        })
            
            city_names_df = pd.DataFrame(city_names)
            
            # 不要去重，保留原始结果
            # # 去重（基于country_code + state_geonameid + name）
            # if not city_names_df.empty:
            #     city_names_df = city_names_df.drop_duplicates(subset=['country_code', 'state_geonameid', 'name'])
            
            self.logger.info(f"构建city_names表完成，共{len(city_names_df)}条记录")
            return city_names_df
            
        except Exception as e:
            self.logger.error(f"创建city_names映射时出错: {e}")
            return pd.DataFrame()
    
    def verify_admin_codes(self, df: pd.DataFrame) -> bool:
        """验证admin代码的完整性
        
        Args:
            df: 包含admin代码的DataFrame
            
        Returns:
            bool: admin代码是否完整
        """
        try:
            issues = []
            
            # 检查ADM1的admin1_code
            adm1_df = df[df['feature_code'] == 'ADM1']
            missing_admin1 = adm1_df[adm1_df['admin1_code'].isna() | (adm1_df['admin1_code'] == '')]
            if not missing_admin1.empty:
                issues.append(f"ADM1缺少admin1_code: {len(missing_admin1)}条")
            
            # 检查ADM2的admin1_code和admin2_code
            adm2_df = df[df['feature_code'] == 'ADM2']
            missing_admin1_in_adm2 = adm2_df[adm2_df['admin1_code'].isna() | (adm2_df['admin1_code'] == '')]
            if not missing_admin1_in_adm2.empty:
                issues.append(f"ADM2缺少admin1_code: {len(missing_admin1_in_adm2)}条")
            
            missing_admin2 = adm2_df[adm2_df['admin2_code'].isna() | (adm2_df['admin2_code'] == '')]
            if not missing_admin2.empty:
                issues.append(f"ADM2缺少admin2_code: {len(missing_admin2)}条")
            
            if issues:
                self.logger.warning(f"Admin代码验证发现问题: {'; '.join(issues)}")
                return False
            else:
                self.logger.info("Admin代码验证通过")
                return True
                
        except Exception as e:
            self.logger.error(f"验证admin代码时出错: {e}")
            return False
    
    def get_statistics(self, states_df: pd.DataFrame, cities_df: pd.DataFrame, 
                      state_names_df: pd.DataFrame, city_names_df: pd.DataFrame) -> Dict:
        """获取数据统计信息
        
        Args:
            states_df: state表数据
            cities_df: city表数据
            state_names_df: state_names表数据
            city_names_df: city_names表数据
            
        Returns:
            Dict: 统计信息
        """
        stats = {
            'states_count': len(states_df),
            'cities_count': len(cities_df),
            'state_names_count': len(state_names_df),
            'city_names_count': len(city_names_df),
            'countries_with_states': states_df['country_code'].nunique() if not states_df.empty else 0,
            'countries_with_cities': cities_df['country_code'].nunique() if not cities_df.empty else 0
        }
        
        return stats


def test_geo_hierarchy():
    """测试地理层级关系模块"""
    print("=== 地理层级关系模块测试 ===")
    
    # 配置日志
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    # 创建测试数据
    test_data = pd.DataFrame([
        {'geonameid': 1001, 'name': 'California', 'asciiname': 'California', 
         'country_code': 'US', 'feature_code': 'ADM1', 'admin1_code': 'CA', 'admin2_code': ''},
        {'geonameid': 1002, 'name': 'Texas', 'asciiname': 'Texas', 
         'country_code': 'US', 'feature_code': 'ADM1', 'admin1_code': 'TX', 'admin2_code': ''},
        {'geonameid': 2001, 'name': 'Los Angeles County', 'asciiname': 'Los Angeles County', 
         'country_code': 'US', 'feature_code': 'ADM2', 'admin1_code': 'CA', 'admin2_code': '037'},
        {'geonameid': 2002, 'name': 'Harris County', 'asciiname': 'Harris County', 
         'country_code': 'US', 'feature_code': 'ADM2', 'admin1_code': 'TX', 'admin2_code': '201'}
    ])
    
    # 测试别名数据
    test_aliases = {
        1001: ['California', 'CA', 'Golden State'],
        1002: ['Texas', 'TX', 'Lone Star State'],
        2001: ['Los Angeles County', 'LA County'],
        2002: ['Harris County', 'Harris Co.']
    }
    
    # 创建GeoHierarchy实例
    geo_hierarchy = GeoHierarchy()
    
    # 测试层级验证
    print("\n1. 测试层级验证:")
    is_valid = geo_hierarchy.validate_hierarchy(test_data)
    print(f"层级关系验证结果: {is_valid}")
    
    # 测试构建state表
    print("\n2. 测试构建state表:")
    adm1_data = test_data[test_data['feature_code'] == 'ADM1']
    states_df = geo_hierarchy.build_state_records(adm1_data)
    print(f"State表记录数: {len(states_df)}")
    print(states_df.to_string(index=False))
    
    # 测试构建city表
    print("\n3. 测试构建city表:")
    adm2_data = test_data[test_data['feature_code'] == 'ADM2']
    cities_df = geo_hierarchy.build_city_records(adm2_data)
    print(f"City表记录数: {len(cities_df)}")
    print(cities_df.to_string(index=False))
    
    # 测试state_names映射
    print("\n4. 测试state_names映射:")
    state_names_df = geo_hierarchy.create_state_names_mapping(states_df, test_aliases)
    print(f"State_names表记录数: {len(state_names_df)}")
    print(state_names_df.to_string(index=False))
    
    # 测试city_names映射
    print("\n5. 测试city_names映射:")
    city_names_df = geo_hierarchy.create_city_names_mapping(cities_df, states_df, test_aliases)
    print(f"City_names表记录数: {len(city_names_df)}")
    print(city_names_df.to_string(index=False))
    
    # 测试admin代码验证
    print("\n6. 测试admin代码验证:")
    admin_valid = geo_hierarchy.verify_admin_codes(test_data)
    print(f"Admin代码验证结果: {admin_valid}")
    
    # 获取统计信息
    print("\n7. 统计信息:")
    stats = geo_hierarchy.get_statistics(states_df, cities_df, state_names_df, city_names_df)
    for key, value in stats.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    test_geo_hierarchy()