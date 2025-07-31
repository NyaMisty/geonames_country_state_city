#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
别名处理器模块
处理GeoNames数据中的地区别名，支持多种清理规则
"""

import re
import pandas as pd
from typing import List, Set, Dict, Tuple, Any
import logging

class AliasProcessor:
    """
    别名处理器
    处理GeoNames数据中的alternatenames字段，应用各种清理规则
    """
    
    # 中日韩后缀模式
    CJK_SUFFIXES = {
        # 中文后缀
        'zh': ['市', '县', '区', '省', '州', '府', '镇', '乡', '村'],
        # 日文后缀
        'ja': ['市', '県', '区', '町', '村', '府', '都'],
        # 韩文后缀
        'ko': ['시', '군', '구', '도', '읍', '면', '동']
    }
    
    # 英文后缀模式（罗马化）
    ROMANIZED_SUFFIXES = [
        'shi', 'ken', 'ku', 'machi', 'mura', 'fu', 'to',  # 日文罗马化
        'si', 'gun', 'gu', 'do', 'eup', 'myeon', 'dong'   # 韩文罗马化
    ]
    
    def __init__(self):
        """
        初始化别名处理器
        """
        self.logger = logging.getLogger(__name__)
        
        # 编译正则表达式以提高性能
        self._compile_patterns()
    
    def _compile_patterns(self):
        """
        编译常用的正则表达式模式
        """
        # County移除模式
        self.county_pattern = re.compile(r'\b[Cc]ounty\b', re.IGNORECASE)
        
        # ASCII符号清理模式
        self.ascii_symbols_pattern = re.compile(r'[\s\-,\.\(\)\[\]\{\}\'\";:!\?]+', re.ASCII)
        
        # 中日韩后缀模式
        all_cjk_suffixes = []
        for suffixes in self.CJK_SUFFIXES.values():
            all_cjk_suffixes.extend(suffixes)
        
        # 创建CJK后缀正则（匹配末尾的后缀）
        cjk_pattern = '|'.join(re.escape(suffix) for suffix in all_cjk_suffixes)
        self.cjk_suffix_pattern = re.compile(f'({cjk_pattern})$')
        
        # 罗马化后缀模式（匹配末尾的后缀，忽略大小写）
        romanized_pattern = '|'.join(re.escape(suffix) for suffix in self.ROMANIZED_SUFFIXES)
        self.romanized_suffix_pattern = re.compile(f'\\b({romanized_pattern})$', re.IGNORECASE)
    
    def process_alternatenames(self, raw_aliases: List[str]) -> List[str]:
        """
        处理alternatenames字段
        
        Args:
            raw_aliases: 原始别名列表
            
        Returns:
            List[str]: 处理后的别名列表（包含原始别名和清理版本）
        """
        
        processed_aliases = set()
        
        for alias in raw_aliases:
            # 添加原始别名
            processed_aliases.add(alias)
            
            # 生成清理版本
            cleaned_versions = self.clean_alias(alias)
            processed_aliases.update(cleaned_versions)
        
        # 移除空字符串并返回排序后的列表
        result = [alias for alias in processed_aliases if alias.strip()]
        return sorted(result)
    
    def clean_alias(self, alias: str) -> List[str]:
        """
        清理单个别名，生成多个清理版本
        
        Args:
            alias: 原始别名
            
        Returns:
            List[str]: 清理后的别名版本列表
        """
        cleaned_versions = []
        
        # 1. 删除County后缀
        no_county = self.remove_county_suffix(alias)
        if no_county != alias and no_county.strip():
            cleaned_versions.append(no_county)
        
        # 2. ASCII符号清理（仅对ASCII文本）
        if self.is_ascii(alias):
            no_symbols = self.clean_ascii_symbols(alias)
            if no_symbols != alias and no_symbols.strip():
                cleaned_versions.append(no_symbols)
                
            # 对去除County后的版本也进行符号清理
            if no_county != alias:
                no_county_no_symbols = self.clean_ascii_symbols(no_county)
                if no_county_no_symbols != no_county and no_county_no_symbols.strip():
                    cleaned_versions.append(no_county_no_symbols)
        
        # 3. 中日韩后缀处理
        no_cjk_suffix = self.remove_cjk_suffixes(alias)
        if no_cjk_suffix != alias and no_cjk_suffix.strip():
            cleaned_versions.append(no_cjk_suffix)
        
        # 4. 罗马化后缀处理
        no_romanized_suffix = self.remove_romanized_suffixes(alias)
        if no_romanized_suffix != alias and no_romanized_suffix.strip():
            cleaned_versions.append(no_romanized_suffix)
        
        return cleaned_versions
    
    def remove_county_suffix(self, name: str) -> str:
        """
        删除名称中的'County'关键词
        
        Args:
            name: 原始名称
            
        Returns:
            str: 删除County后的名称
        """
        # 使用正则表达式删除County（忽略大小写）
        cleaned = self.county_pattern.sub('', name)
        
        # 清理多余的空白
        cleaned = re.sub(r'\s+', '', cleaned).strip()
        
        return cleaned
    
    def clean_ascii_symbols(self, name: str) -> str:
        """
        清理ASCII名称中的空白字符和符号
        
        Args:
            name: ASCII名称
            
        Returns:
            str: 清理后的名称
        """
        if not self.is_ascii(name):
            return name
        
        # 删除符号和多余空白，保留字母和数字
        cleaned = re.sub(r'[^a-zA-Z0-9\s]', '', name)
        
        # 清理多余的空白
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        return cleaned
    
    def remove_cjk_suffixes(self, name: str) -> str:
        """
        删除中日韩文后缀
        
        Args:
            name: 原始名称
            
        Returns:
            str: 删除后缀后的名称
        """
        # 使用预编译的正则表达式
        cleaned = self.cjk_suffix_pattern.sub('', name)
        
        return cleaned.strip()
    
    def remove_romanized_suffixes(self, name: str) -> str:
        """
        删除罗马化后缀（如Shi, Ken等）
        
        Args:
            name: 原始名称
            
        Returns:
            str: 删除后缀后的名称
        """
        # 使用预编译的正则表达式
        cleaned = self.romanized_suffix_pattern.sub('', name)
        
        return cleaned.strip()
    
    def is_ascii(self, text: str) -> bool:
        """
        检查文本是否为纯ASCII
        
        Args:
            text: 待检查的文本
            
        Returns:
            bool: 是否为ASCII文本
        """
        try:
            text.encode('ascii')
            return True
        except UnicodeEncodeError:
            return False
    
    def get_statistics(self, aliases_list: List[List[str]]) -> dict:
        """
        获取别名处理统计信息
        
        Args:
            aliases_list: 别名列表的列表
            
        Returns:
            dict: 统计信息
        """
        stats = {
            'total_records': len(aliases_list),
            'total_original_aliases': 0,
            'total_processed_aliases': 0,
            'avg_aliases_per_record': 0,
            'county_removals': 0,
            'ascii_cleanings': 0,
            'cjk_suffix_removals': 0,
            'romanized_suffix_removals': 0
        }
        
        for aliases in aliases_list:
            if aliases:
                # 估算原始别名数量（假设处理后的数量是原始的1.5倍）
                estimated_original = len(aliases) // 2 if len(aliases) > 1 else len(aliases)
                stats['total_original_aliases'] += estimated_original
                stats['total_processed_aliases'] += len(aliases)
        
        if stats['total_records'] > 0:
            stats['avg_aliases_per_record'] = stats['total_processed_aliases'] / stats['total_records']
        
        return stats

def main():
    """
    测试函数
    """
    import logging
    logging.basicConfig(level=logging.INFO)
    
    processor = AliasProcessor()
    
    # 测试用例
    test_cases = [
        "New York County,NYC,Big Apple",
        "Tokyo Shi,東京市,Tokyo",
        "Los Angeles County,LA,L.A.",
        "Beijing Shi,北京市,Peking",
        "Seoul Si,서울시,Seoul",
        "San Francisco,SF,San-Francisco",
        "Osaka Fu,大阪府,Osaka"
    ]
    
    print("=== 别名处理器测试 ===")
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n测试 {i}: {test_case}")
        processed = processor.process_alternatenames(test_case)
        print(f"处理结果: {processed}")
        print(f"别名数量: {len(processed)}")
    
    # 测试单个清理功能
    print("\n=== 单项清理测试 ===")
    
    test_names = [
        "Los Angeles County",
        "Tokyo Shi",
        "New-York",
        "北京市",
        "Seoul Si"
    ]
    
    for name in test_names:
        print(f"\n原始: {name}")
        print(f"去County: {processor.remove_county_suffix(name)}")
        print(f"ASCII清理: {processor.clean_ascii_symbols(name)}")
        print(f"去CJK后缀: {processor.remove_cjk_suffixes(name)}")
        print(f"去罗马化后缀: {processor.remove_romanized_suffixes(name)}")
        print(f"是否ASCII: {processor.is_ascii(name)}")

if __name__ == '__main__':
    main()