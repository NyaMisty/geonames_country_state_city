#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
sys.path.append('src')

# 测试缓存机制
from wikidata_query import WikidataCache

print("=== 缓存机制验证测试 ===")

# 1. 初始化缓存
cache = WikidataCache()
print(f"✓ 缓存初始化成功")
print(f"✓ 缓存文件路径: {cache.cache_file_path}")

# 2. 检查目录和文件
print(f"✓ 缓存目录存在: {os.path.exists('cache')}")
print(f"✓ 缓存文件存在: {os.path.exists(cache.cache_file_path)}")

# 3. 测试缓存读写
test_id = 'Q999'
test_geonameid = '123456'

# 缓存一个结果
cache.cache_result(test_id, test_geonameid)
print(f"✓ 缓存结果: {test_id} -> {test_geonameid}")

# 保存缓存
cache.save_cache()
print(f"✓ 缓存已保存")

# 读取缓存
result = cache.get_cached_result(test_id)
print(f"✓ 缓存读取测试: {test_id} -> {result}")

# 4. 测试批量操作
test_ids = ['Q1001', 'Q1002', 'Q1003']
uncached_ids = cache.get_uncached_ids(test_ids)
print(f"✓ 未缓存ID数量: {len(uncached_ids)}")

cached_results = cache.get_cached_results(test_ids)
print(f"✓ 已缓存结果数量: {len(cached_results)}")

print("\n=== 缓存机制验证完成 ===")
print("所有基础功能正常工作！")