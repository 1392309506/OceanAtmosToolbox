#!/usr/bin/env python3
"""
CMEMS数据下载命令行工具
"""
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from downloaders.cmems_downloader import CMEMSDownloader
from utils.config_manager import ConfigManager
import argparse

import os
from dotenv import load_dotenv
load_dotenv()

username = os.getenv('CMEMS_USERNAME')
password = os.getenv('CMEMS_PASSWORD')

def main():
    parser = argparse.ArgumentParser(description='CMEMS数据批量下载工具')
    parser.add_argument('--start_date', type=str, default='1993-01',
                        help='起始日期 (YYYY-MM 或 YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, default='2024-12',
                        help='结束日期 (YYYY-MM 或 YYYY-MM-DD)')
    parser.add_argument('--hours', type=str, nargs='+',
                        help='小时列表 (例如 00:00 06:00 12:00 18:00)')
    parser.add_argument('--dataset', type=str, default='glo12_monthly',
                        help='数据集名称')
    parser.add_argument('--config', type=str, default='./config/config.yaml',
                        help='配置文件路径')
    parser.add_argument('--output_dir', type=str, help='输出目录')
    parser.add_argument('--variables', type=str, nargs='+',
                        help='要下载的变量列表')
    parser.add_argument('--is_hourly', action='store_true',
                        help='将 YYYY-MM-DD 视为小时级数据')
    parser.add_argument('--use_cli', action='store_true',
                        help='使用命命令行参数覆盖配置')

    args = parser.parse_args()

    # 检查环境变量
    if not os.getenv('CMEMS_USERNAME') or not os.getenv('CMEMS_PASSWORD'):
        print("警告: 未设置CMEMS_USERNAME和CMEMS_PASSWORD环境变量")
        print("请执行: export CMEMS_USERNAME='your_username'")
        print("       export CMEMS_PASSWORD='your_password'")
        return

    # 加载配置
    config_manager = ConfigManager()
    config = config_manager.load_config(args.config)

    # 覆盖配置中的输出目录
    if args.output_dir:
        config['output_base_dir'] = args.output_dir

    # 创建下载器
    downloader = CMEMSDownloader(config)

    cmems_cfg = config.get('cmems', {})
    cmems_defaults = cmems_cfg.get('download_parameters') or cmems_cfg.get('download_defaults', {})

    def pick_value(key: str, arg_value):
        if args.use_cli:
            return arg_value
        return cmems_defaults.get(key, arg_value)

    start_date = pick_value('start_date', args.start_date)
    end_date = pick_value('end_date', args.end_date)
    dataset_name = pick_value('dataset', args.dataset)
    variables = pick_value('variables', args.variables)
    hours = pick_value('hours', args.hours)
    if hours is None:
        dataset_cfg = config.get('cmems', {}).get('datasets', {}).get(dataset_name, {})
        dataset_time = dataset_cfg.get('time')
        if isinstance(dataset_time, list):
            hours = dataset_time
    is_hourly = bool(pick_value('is_hourly', args.is_hourly))

    def infer_mode() -> str:
        if start_date and end_date:
            if len(start_date) == 7 and len(end_date) == 7:
                return 'monthly'
            if len(start_date) == 10 and len(end_date) == 10:
                return 'hourly' if is_hourly else 'daily'
        raise ValueError("无法判断下载粒度，请提供合法的 start_date/end_date")

    mode = infer_mode()

    # 执行下载
    if mode == 'hourly':
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
            variables=variables
        )
    else:
        results = downloader.download_monthly_range(
            start_date=start_date,
            end_date=end_date,
            dataset_name=dataset_name,
            variables=variables
        )

    # 统计结果
    success_count = sum(1 for r in results.values() if r)
    total_count = len(results)

    print(f"\n下载完成!")
    print(f"成功: {success_count}/{total_count}")
    print(f"失败: {total_count - success_count}/{total_count}")


if __name__ == "__main__":
    main()