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
    parser.add_argument('--start_date', type=str, default='2007-01',
                        help='起始日期 (YYYY-MM 或 YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, default='2024-12',
                        help='结束日期 (YYYY-MM 或 YYYY-MM-DD)')
    parser.add_argument('--hours', type=str, nargs='+',
                        help='小时列表 (例如 00:00 06:00 12:00 18:00)')
    parser.add_argument('--dataset', type=str, default='era5_monthly',
                        help='数据集名称')
    parser.add_argument('--config', type=str, default='./config/config.yaml',
                        help='配置文件路径')
    parser.add_argument('--output_dir', type=str, help='输出目录')
    parser.add_argument('--variables', type=str, nargs='+',
                        help='要下载的变量列表')
    parser.add_argument('--list_datasets', action='store_true',
                        help='列出可用数据集')
    parser.add_argument('--use_cli', action='store_true',
                        help='使用命令行参数覆盖配置')
    parser.add_argument('--is_hourly', action='store_true',
                        help='将 YYYY-MM-DD 视为小时级数据')

    args = parser.parse_args()

    config_manager = ConfigManager()
    config = config_manager.load_config(args.config)

    if args.output_dir:
        config['output_base_dir'] = args.output_dir

    downloader = C3SDownloader(config)

    if args.list_datasets:
        datasets = downloader.list_available_datasets()
        print("可用数据集:")
        for name, desc in datasets.items():
            print(f"  {name}: {desc}")
        return

    c3s_cfg = config.get('c3s', {})
    c3s_defaults = c3s_cfg.get('download_parameters') or c3s_cfg.get('download_defaults', {})

    def pick_value(key: str, arg_value):
        if args.use_cli:
            return arg_value
        return c3s_defaults.get(key, arg_value)

    start_date = pick_value('start_date', args.start_date)
    end_date = pick_value('end_date', args.end_date)
    dataset_name = pick_value('dataset', args.dataset)
    variables = pick_value('variables', args.variables)
    hours = pick_value('hours', args.hours)
    is_hourly = bool(pick_value('is_hourly', args.is_hourly))

    def infer_mode() -> str:
        if start_date and end_date:
            if len(start_date) == 7 and len(end_date) == 7:
                return 'monthly'
            if len(start_date) == 10 and len(end_date) == 10:
                return 'hourly' if is_hourly else 'daily'
        raise ValueError("无法判断下载粒度，请提供合法的 start_date/end_date")

    mode = infer_mode()

    if mode == 'hourly':
        dataset_cfg = config.get('c3s', {}).get('datasets', {}).get(dataset_name, {})
        dataset_time = dataset_cfg.get('time')
        if not isinstance(dataset_time, list):
            raise ValueError("hourly 模式要求数据集配置中 time 为小时列表")
        results = downloader.download_hourly_range(
            start_date=start_date,
            end_date=end_date,
            dataset_name=dataset_name,
            variables=variables,
            hours=hours
        )
    elif mode == 'daily':
        results = downloader.download_daily_range(
            start_date=start_date,
            end_date=end_date,
            dataset_name=dataset_name,
            variables=variables,
            hours=hours
        )
    else:
        results = downloader.download_monthly_range(
            start_date=start_date,
            end_date=end_date,
            dataset_name=dataset_name,
            variables=variables
        )

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