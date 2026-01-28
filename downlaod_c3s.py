#!/usr/bin/env python3
"""
C3S数据下载命令行工具
"""
import sys
from pathlib import Path
import os
from dotenv import load_dotenv

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

load_dotenv()

from downloaders.c3s_downloader import C3SDownloader
from utils.config_manager import ConfigManager
import argparse


def main():
    parser = argparse.ArgumentParser(description='C3S数据批量下载工具')
    parser.add_argument('--start-year', type=int, default=2007, help='起始年份')
    parser.add_argument('--end-year', type=int, default=2024, help='结束年份')
    parser.add_argument('--start-month', type=int, help='起始月份 (1-12)')
    parser.add_argument('--end-month', type=int, help='结束月份 (1-12)')
    parser.add_argument('--dataset', type=str, default='era5_monthly',
                        help='数据集名称')
    parser.add_argument('--config', type=str, default='./config/config.yaml',
                        help='配置文件路径')
    parser.add_argument('--output-dir', type=str, help='输出目录')
    parser.add_argument('--variables', type=str, nargs='+',
                        help='要下载的变量列表')
    parser.add_argument('--list-datasets', action='store_true',
                        help='列出可用数据集')

    args = parser.parse_args()

    # 加载配置
    config_manager = ConfigManager()
    config = config_manager.load_config(args.config)

    # 覆盖配置中的输出目录
    if args.output_dir:
        config['output_base_dir'] = args.output_dir

    # 创建下载器
    downloader = C3SDownloader(config)

    if args.list_datasets:
        datasets = downloader.list_available_datasets()
        print("可用数据集:")
        for name, desc in datasets.items():
            print(f"  {name}: {desc}")
        return

    # 执行下载
    results = downloader.download_range(
        start_year=args.start_year,
        end_year=args.end_year,
        start_month=args.start_month,
        end_month=args.end_month,
        dataset_name=args.dataset,
        variables=args.variables
    )

    # 统计结果
    success_count = sum(1 for r in results.values() if r)
    total_count = len(results)

    print(f"\n下载完成!")
    print(f"成功: {success_count}/{total_count}")
    print(f"失败: {total_count - success_count}/{total_count}")

    if success_count < total_count:
        print("\n失败的任务:")
        for task, success in results.items():
            if not success:
                print(f"  {task}")


if __name__ == "__main__":
    main()