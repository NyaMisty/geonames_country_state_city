#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GeoNames数据处理主程序

完整的数据处理流程：
1. 解析GeoNames数据文件
2. 处理地区别名
3. 构建地理层级关系
4. 导出CSV文件
5. 导入SQLite数据库
6. 检查重复项
7. 生成处理报告
"""

import os
import sys
import logging

# 配置日志
import os
os.makedirs("output", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('output/geonames_processing.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

from geo_processor import GeonamesProcessor

def main():
    """
    主函数
    """
    import argparse
    
    print("GeoNames数据处理系统")
    print("=" * 50)
    
    # 设置命令行参数解析
    parser = argparse.ArgumentParser(description='GeoNames数据处理系统')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # gen 子命令 - 生成数据库
    gen_parser = subparsers.add_parser('gen', help='生成GeoNames数据库')
    gen_parser.add_argument('data_file', nargs='?', default='source_data/geoname/allCountries.txt',
                           help='GeoNames数据文件路径')
    gen_parser.add_argument('--full', action='store_true', default=True, help='完整流程（GeoNames + CSC）')
    gen_parser.add_argument('--csc', action='store_true', help='仅处理CSC数据集成')
    gen_parser.add_argument('--enable-csc', action='store_true', help='启用CSC数据处理')
    gen_parser.add_argument('--csc-file', default='source_data/csc/cities.txt',
                           help='CSC数据文件路径（默认: source_data/csc/cities.txt）')
    
    # convert 子命令 - CSV转换
    convert_parser = subparsers.add_parser('convert', help='CSV地理匹配转换')
    convert_parser.add_argument('--input-csv', required=True, help='输入CSV文件路径')
    convert_parser.add_argument('--output-csv', required=True, help='输出CSV文件路径（可选，默认在output目录）')
    convert_parser.add_argument('--country-col', default='country_code', help='国家代码列名（默认: country_code）')
    convert_parser.add_argument('--state-col', default='state_name', help='州/省名称列名（默认: state_name）')
    convert_parser.add_argument('--city-col', default='city_name', help='城市名称列名（默认: city_name）')
    convert_parser.add_argument('--db-path', default='output/sqlite_output/geonames.db', help='GeoNames数据库路径（默认: output/sqlite_output/geonames.db）')
    convert_parser.add_argument('--output-dir', default='output/csv_match_output', help='CSV匹配输出目录（默认: output/csv_match_output）')
    
    args = parser.parse_args()
    
    # 检查是否提供了命令
    if not args.command:
        parser.print_help()
        print("\n使用示例:")
        print("  python main.py gen --full                    # 生成完整数据库")
        print("  python main.py convert --input-csv data.csv  # 转换CSV文件")
        sys.exit(1)
    
    # 根据子命令执行相应功能
    if args.command == 'convert':
        # CSV转换功能
        # 验证CSV匹配参数
        if not os.path.exists(args.input_csv):
            print(f"错误: 输入CSV文件不存在: {args.input_csv}")
            sys.exit(1)
        
        if not os.path.exists(args.db_path):
            print(f"错误: GeoNames数据库文件不存在: {args.db_path}")
            print("请先运行完整的GeoNames数据处理流程生成数据库")
            sys.exit(1)
        
        # 执行CSV地理匹配
        print("\n=== CSV地理匹配功能 ===")
        print(f"输入文件: {args.input_csv}")
        print(f"数据库路径: {args.db_path}")
        print(f"输出目录: {args.output_dir}")
        print(f"列名映射: 国家={args.country_col}, 州={args.state_col}, 城市={args.city_col}")
        
        try:
            from csv_geo_matcher import CSVGeoMatcher
            
            # 构建列名映射
            column_mapping = {
                'country_code': args.country_col,
                'state_name': args.state_col,
                'city_name': args.city_col
            }
            
            # 创建CSV地理匹配器
            matcher = CSVGeoMatcher(
                db_path=args.db_path,
                input_csv=args.input_csv,
                output_csv=args.output_csv,
                column_mapping=column_mapping,
                output_dir=args.output_dir
            )
            
            # 执行匹配处理
            print("\n开始CSV地理匹配处理...")
            result = matcher.process_csv_file()
            
            # 处理结果
            print("\nCSV地理匹配完成！")
            print(f"处理记录数: {result['statistics']['total_records']}")
            print(f"成功匹配: {result['statistics']['successful_matches']}")
            print(f"匹配成功率: {result['statistics']['success_rate']:.2f}%")
            print(f"处理时间: {result['statistics']['processing_time']:.2f}秒")
            print(f"\n输出文件:")
            print(f"  - 匹配结果: {result['output_file']}")
            print(f"  - 详细报告(文本): {result['report_files']['text_report']}")
            print(f"  - 详细报告(JSON): {result['report_files']['json_report']}")
            if result['report_files']['failed_records']:
                print(f"  - 失败记录: {result['report_files']['failed_records']}")
            sys.exit(0)
                
        except ImportError:
            print("错误: 无法导入csv_geo_matcher模块")
            print("请确保csv_geo_matcher.py文件存在于src目录中")
            sys.exit(1)
        except Exception as e:
            print(f"CSV地理匹配过程中发生错误: {e}")
            logger.error(f"CSV地理匹配失败: {e}")
            sys.exit(1)
    
    elif args.command == 'gen':
        # 数据库生成功能
        # 确定处理模式
        mode = 'geonames'  # 默认模式
        enable_csc = False
        csc_file_path = args.csc_file
        
        if args.csc:
            mode = 'csc'
            enable_csc = True
        elif args.full:
            mode = 'full'
            enable_csc = True
        elif args.enable_csc:
            enable_csc = True
        
        data_file = args.data_file
        
        # 显示模式信息
        if mode == 'csc':
            print("模式: CSC数据集成")
        elif mode == 'full':
            print("模式: 完整流程（GeoNames + CSC）")
        else:
            print("模式: GeoNames数据处理")
        
        if enable_csc:
            print(f"CSC处理: 启用")
            print(f"CSC数据文件: {csc_file_path}")
        else:
            print("CSC处理: 禁用")
        
        # 配置验证
        if enable_csc and not os.path.exists(csc_file_path):
            print(f"警告: CSC数据文件不存在: {csc_file_path}")
            print("CSC处理将被禁用")
            enable_csc = False
        
        # 检查是否使用样本数据（仅GeoNames模式）
        if mode in ['geonames', 'full']:
            if not os.path.exists(data_file):
                sample_file = 'source_data/geoname/allCountries_sample.txt'
                if os.path.exists(sample_file):
                    print(f"主数据文件不存在，使用样本数据: {sample_file}")
                    data_file = sample_file
                else:
                    print(f"错误: 数据文件不存在: {data_file}")
                    print("请确保数据文件存在，或提供正确的文件路径")
                    sys.exit(1)
        
        # 创建处理器
        processor = GeonamesProcessor(data_file, enable_csc=enable_csc, csc_file_path=csc_file_path if enable_csc else None)
        
        success = False
        
        if mode == 'csc':
            # 仅处理CSC数据集成（向后兼容模式）
            print("\n开始CSC数据集成...")
            # 在CSC模式下，只处理数据但不运行完整的GeoNames流程
            try:
                states_df, cities_df, state_names_df, city_names_df, csc_mapping_df = processor.process_data()
                processor.export_data(states_df, cities_df, state_names_df, city_names_df, csc_mapping_df)
                processor.generate_csc_report()
                success = True
            except Exception as e:
                logger.error(f"CSC数据处理失败: {e}")
                success = False
        else:
            # GeoNames数据处理（可能包含CSC集成）
            print("\n开始数据处理流程...")
            success = processor.run_full_pipeline()
            if success and enable_csc:
                processor.generate_csc_report()
        
        if success:
            print("\n数据库生成完成！")
            print(f"查看详细报告: {processor.sqlite_output_dir}/processing_report.txt")
            if mode in ['csc', 'full']:
                print(f"查看CSC报告: {processor.sqlite_output_dir}/csc_processing_report.txt")
            print(f"查看重复项报告: {processor.sqlite_output_dir}/duplicate_report.txt")
            print(f"查看处理日志: geonames_processing.log")
            print("\n使用说明:")
            print("  python main.py gen --full                   # 生成完整数据库（GeoNames + CSC）")
            print("  python main.py gen --csc                    # 仅处理CSC数据")
            print("  python main.py gen data_file.txt --full     # 指定数据文件并生成完整数据库")
            print("  python main.py convert --input-csv data.csv # CSV地理匹配转换")
            sys.exit(0)
        else:
            print("\n数据库生成失败！请查看日志了解详细错误信息")
            sys.exit(1)

if __name__ == "__main__":
    main()