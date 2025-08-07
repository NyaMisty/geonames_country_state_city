#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Wikidata SPARQL查询模块
提供Wikidata查询功能，支持批量geonameid查询
"""

import requests
import time
import logging
import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from tqdm import tqdm

class WikidataCache:
    """
    Wikidata查询结果缓存管理器
    使用JSON文件存储缓存，支持过期检查和批量操作
    """
    
    def __init__(self, cache_file_path: str = None):
        """
        初始化缓存管理器
        
        Args:
            cache_file_path: 缓存文件路径，默认为项目根目录下的cache/wikidata_cache.json
        """
        if cache_file_path is None:
            # 获取项目根目录
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(current_dir)
            cache_dir = os.path.join(project_root, 'cache')
            cache_file_path = os.path.join(cache_dir, 'wikidata_cache.json')
        
        self.cache_file_path = cache_file_path
        self.cache_data = {}
        self.cache_expiry_days = 30  # 缓存30天过期
        
        # 统计信息
        self.stats = {
            'cache_hits': 0,
            'cache_misses': 0,
            'cache_expired': 0,
            'total_requests': 0
        }
        
        self.load_cache()
    
    def load_cache(self) -> None:
        """
        从JSON文件加载现有缓存
        """
        try:
            if os.path.exists(self.cache_file_path):
                with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                    self.cache_data = json.load(f)
                logging.getLogger(__name__).debug(f"加载缓存文件: {len(self.cache_data)} 条记录")
            else:
                self.cache_data = {}
                logging.getLogger(__name__).debug("缓存文件不存在，创建新缓存")
        except Exception as e:
            logging.getLogger(__name__).warning(f"加载缓存文件失败: {e}，使用空缓存")
            self.cache_data = {}
    
    def save_cache(self) -> None:
        """
        保存缓存到JSON文件
        """
        try:
            # 确保缓存目录存在
            os.makedirs(os.path.dirname(self.cache_file_path), exist_ok=True)
            
            with open(self.cache_file_path, 'w', encoding='utf-8') as f:
                json.dump(self.cache_data, f, ensure_ascii=False, indent=2)
            logging.getLogger(__name__).debug(f"缓存已保存: {len(self.cache_data)} 条记录")
        except Exception as e:
            logging.getLogger(__name__).error(f"保存缓存文件失败: {e}")
    
    def is_cache_valid(self, timestamp_str: str) -> bool:
        """
        检查缓存是否过期
        
        Args:
            timestamp_str: 时间戳字符串
            
        Returns:
            bool: 缓存是否有效
        """
        try:
            cache_time = datetime.fromisoformat(timestamp_str)
            expiry_time = cache_time + timedelta(days=self.cache_expiry_days)
            return datetime.now() < expiry_time
        except Exception:
            return False
    
    def get_cached_result(self, wikidata_id: str) -> Optional[str]:
        """
        获取缓存的geonameid
        
        Args:
            wikidata_id: Wikidata ID (格式: Q123456)
            
        Returns:
            Optional[str]: 缓存的geonameid，如果未找到或过期则返回None
        """
        self.stats['total_requests'] += 1
        
        if wikidata_id not in self.cache_data:
            self.stats['cache_misses'] += 1
            return None
        
        cache_entry = self.cache_data[wikidata_id]
        if not isinstance(cache_entry, dict):
            self.stats['cache_misses'] += 1
            return None
        
        timestamp = cache_entry.get('timestamp')
        if timestamp and not self.is_cache_valid(timestamp):
            # 缓存过期，删除该条目
            del self.cache_data[wikidata_id]
            self.stats['cache_expired'] += 1
            return None
        
        self.stats['cache_hits'] += 1
        return cache_entry.get('geonameid')
    
    def cache_result(self, wikidata_id: str, geonameid: str) -> None:
        """
        缓存查询结果
        
        Args:
            wikidata_id: Wikidata ID
            geonameid: 对应的geonameid
        """
        self.cache_data[wikidata_id] = {
            'geonameid': geonameid,
            'timestamp': datetime.now().isoformat()
        }
    
    def get_uncached_ids(self, wikidata_ids: List[str]) -> List[str]:
        """
        获取未缓存的wikidata_ids列表
        
        Args:
            wikidata_ids: 要检查的wikidata_ids列表
            
        Returns:
            List[str]: 未缓存的wikidata_ids
        """
        uncached_ids = []
        for wikidata_id in wikidata_ids:
            if self.get_cached_result(wikidata_id) is None:
                uncached_ids.append(wikidata_id)
        return uncached_ids
    
    def get_cached_results(self, wikidata_ids: List[str]) -> Dict[str, str]:
        """
        批量获取缓存结果
        
        Args:
            wikidata_ids: Wikidata ID列表
            
        Returns:
            Dict[str, str]: 缓存的结果映射 {wikidata_id: geonameid}
        """
        results = {}
        for wikidata_id in wikidata_ids:
            cached_result = self.get_cached_result(wikidata_id)
            if cached_result is not None:
                results[wikidata_id] = cached_result
        return results
    
    def get_cache_stats(self) -> Dict[str, any]:
        """
        获取缓存统计信息
        
        Returns:
            Dict[str, any]: 包含缓存命中率、总请求数等统计信息
        """
        total_requests = self.stats['total_requests']
        if total_requests == 0:
            hit_rate = 0.0
        else:
            hit_rate = (self.stats['cache_hits'] / total_requests) * 100
        
        return {
            'total_requests': total_requests,
            'cache_hits': self.stats['cache_hits'],
            'cache_misses': self.stats['cache_misses'],
            'cache_expired': self.stats['cache_expired'],
            'hit_rate_percent': round(hit_rate, 2),
            'cache_size': len(self.cache_data)
        }
    
    def reset_stats(self):
        """
        重置统计信息
        """
        self.stats = {
            'cache_hits': 0,
            'cache_misses': 0,
            'cache_expired': 0,
            'total_requests': 0
        }

def doRawSparql(query):
    for _ in range(10):
        try:
            headers = {
                'accept': 'application/sparql-results+json',
                'api-user-agent': 'query-service-ui (https://query.wikidata.org)',
                'referer': 'https://query.wikidata.org/',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest',
                # 'cookie': 'WMF-Last-Access-Global=31-Jul-2025; GeoIP=US:::37.75:-97.82:v4; WMF-Uniq=dJsanhv4wIhxNGZruSfAHwJBAAEBAFvdgdRxbJ7MGiZkqg-DXndOOP5W_ecfNMqk',
            }

            params = {
                'query': query,
            }

            response = requests.post('https://query.wikidata.org/sparql', data=params, headers=headers)
            if response.status_code != 200:
                raise Exception(f"Query failed with status code {response.status_code}: {response.content}")
            return response.json()
        except Exception:
            time.sleep(10)

def doSparql(query):
    ret = doRawSparql(query)
    for val in ret['results']['bindings']:
        yield {k: v['value'] for k,v in val.items()}

def batch_query_geonameid(wikidata_ids: List[str], batch_size: int = 500, enable_cache: bool = True) -> Dict[str, str]:
    """
    批量查询wikiDataId对应的geonameid，支持本地缓存
    
    Args:
        wikidata_ids: wikiDataId列表 (格式: Q123456)
        batch_size: 每批查询的数量，默认500
        enable_cache: 是否启用缓存，默认True
        
    Returns:
        Dict[str, str]: {wikiDataId: geonameid} 映射字典
        
    Raises:
        Exception: 查询失败时抛出异常
    """
    logger = logging.getLogger(__name__)
    logger.info(f"开始批量查询 {len(wikidata_ids)} 个wikiDataId的geonameid (缓存: {'启用' if enable_cache else '禁用'})")
    
    # 初始化缓存
    cache = WikidataCache() if enable_cache else None
    result_mapping = {}
    failed_ids = []
    
    # 如果启用缓存，先从缓存中获取已有结果
    if cache:
        cached_results = cache.get_cached_results(wikidata_ids)
        result_mapping.update(cached_results)
        
        # 获取未缓存的ID列表
        uncached_ids = cache.get_uncached_ids(wikidata_ids)
        logger.info(f"缓存命中: {len(cached_results)}/{len(wikidata_ids)}, 需要查询: {len(uncached_ids)}")
        
        # 如果全部命中缓存，直接返回
        if not uncached_ids:
            logger.info("所有结果均来自缓存，无需网络查询")
            return result_mapping
        
        # 只查询未缓存的ID
        query_ids = uncached_ids
    else:
        query_ids = wikidata_ids
    
    # 分批处理未缓存的ID
    for start in tqdm(range(0, len(query_ids), batch_size), desc="查询Wikidata"):
        end = min(len(query_ids), start + batch_size)
        batch_ids = query_ids[start:end]
        
        try:
            # 构建SPARQL查询
            values_clause = '\n'.join(f'wd:{qid}' for qid in batch_ids)
            query = f'''
            SELECT ?item ?geonameid ?redirected WHERE {{
            VALUES ?item {{
            {values_clause}
            }}
            OPTIONAL {{ ?item wdt:P1566 ?geonameid. }}
            OPTIONAL {{ ?item owl:sameAs ?redirected. }}
            }}'''
            
            # 执行查询
            batch_results = list(doSparql(query))
            
            redirectedIds = {} # newQid -> oldQid
            # 处理结果
            for result in batch_results:
                item_uri = result.get('item', '')
                geonameid = result.get('geonameid', '')
                redirected = result.get('redirected', '')
                
                # 从URI中提取QID
                assert item_uri.startswith('http://www.wikidata.org/entity/')
                qid = item_uri.replace('http://www.wikidata.org/entity/', '')
                if geonameid:
                    result_mapping[qid] = geonameid
                    # 缓存查询结果
                    if cache:
                        cache.cache_result(qid, geonameid)
                elif redirected:
                    assert redirected.startswith('http://www.wikidata.org/entity/')
                    newQid = redirected.replace('http://www.wikidata.org/entity/', '')
                    redirectedIds[newQid] = qid
                else:
                    continue
            
            if redirectedIds:
                logger.info(f"查询 {len(redirectedIds)} 个重定向ID")
                redirectedResults = batch_query_geonameid(redirectedIds.keys(), batch_size, enable_cache)
                for newQid, geonameid in redirectedResults.items():
                    oldQid = redirectedIds[newQid]
                    result_mapping[oldQid] = geonameid
                    result_mapping[newQid] = geonameid
                    # 缓存查询结果
                    if cache:
                        cache.cache_result(oldQid, geonameid)
                        cache.cache_result(newQid, geonameid)

            # 记录未找到geonameid的ID
            found_ids = set(result_mapping.keys())
            batch_failed = [qid for qid in batch_ids if qid not in found_ids]
            failed_ids.extend(batch_failed)
            
            logger.debug(f"批次 {start//batch_size + 1}: 成功查询 {len(batch_results)} 个，失败 {len(batch_failed)} 个")
            
        except Exception as e:
            logger.error(f"批次查询失败 (批次 {start//batch_size + 1}): {str(e)}")
            failed_ids.extend(batch_ids)
            continue
    
    # 保存缓存并输出统计信息
    if cache:
        try:
            cache.save_cache()
            cache_stats = cache.get_cache_stats()
            logger.info(f"缓存统计: 命中率 {cache_stats['hit_rate_percent']}%, "
                       f"总请求 {cache_stats['total_requests']}, "
                       f"命中 {cache_stats['cache_hits']}, "
                       f"未命中 {cache_stats['cache_misses']}, "
                       f"过期 {cache_stats['cache_expired']}, "
                       f"缓存大小 {cache_stats['cache_size']}")
        except Exception as e:
            logger.error(f"保存缓存失败: {str(e)}")
    
    # 记录查询统计
    success_count = len(result_mapping)
    failed_count = len(failed_ids)
    total_count = len(wikidata_ids)
    success_rate = (success_count / total_count * 100) if total_count > 0 else 0
    
    # 计算缓存统计
    if cache:
        cached_count = len(wikidata_ids) - len(query_ids)
        network_queries = len(query_ids)
        logger.info(f"批量查询完成: 总计 {total_count}, 缓存命中 {cached_count}, "
                   f"网络查询 {network_queries}, 成功 {success_count} ({success_rate:.1f}%), "
                   f"失败 {failed_count}")
    else:
        logger.info(f"批量查询完成: 成功 {success_count}/{total_count} ({success_rate:.1f}%), 失败 {failed_count}")
    
    if failed_ids:
        logger.warning(f"以下wikiDataId未找到对应的geonameid: {failed_ids[:10]}{'...' if len(failed_ids) > 10 else ''}")
    
    return result_mapping

def query_single_geonameid(wikidata_id: str) -> Optional[str]:
    """
    查询单个wikiDataId对应的geonameid
    
    Args:
        wikidata_id: wikiDataId (格式: Q123456)
        
    Returns:
        Optional[str]: geonameid，如果未找到则返回None
    """
    try:
        result = batch_query_geonameid([wikidata_id], batch_size=1)
        return result.get(wikidata_id)
    except Exception:
        return None

if False:
    print(list(doSparql('''
    SELECT ?item ?itemLabel ?geonameid WHERE {
    VALUES ?item {

    wd:Q868700
    wd:Q20260778

    }
    ?item wdt:P1566 ?geonameid.
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en,zh". }
    }''')))

if __name__ == '__main__':
    import pandas as pd
    df = pd.read_csv('wikidataQid.csv')
    qids = list(df.to_dict()['wikiDataId'].values())

    df = None
    from tqdm import tqdm
    for start in tqdm(range(0, len(qids), 500)):
        end = min(len(qids), start+500)
        ret = doSparql('''
        SELECT ?item ?itemLabel ?geonameid WHERE {
        VALUES ?item {
        %s
        }
        ?item wdt:P1566 ?geonameid.
        SERVICE wikibase:label { bd:serviceParam wikibase:language "en,zh". }
        }''' % '\n'.join(f'wd:{qid}' for qid in qids[start:end]))
        newDf = pd.DataFrame(ret)
        if df is None:
            df = newDf
        else:
            df = pd.concat([df, newDf])

    df.to_csv('wikidataQid_geonameid.csv', index=False)
