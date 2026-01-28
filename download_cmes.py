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
    parser.add_argument('--start-date', type=str, default='1993-01',
                        help='起始日期 (YYYY-MM)')
    parser.add_argument('--end-date', type=str, default='2024-12',
                        help='结束日期 (YYYY-MM)')
    parser.add_argument('--dataset', type=str, default='glo12_monthly',
                        help='数据集名称')
    parser.add_argument('--config', type=str, default='./config/config.yaml',
                        help='配置文件路径')
    parser.add_argument('--output-dir', type=str, default='output', help='输出目录')
    parser.add_argument('--variables', type=str, nargs='+',
                        help='要下载的变量列表')

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

    # 执行下载
    results = downloader.download_monthly_range(
        start_date=args.start_date,
        end_date=args.end_date,
        dataset_name=args.dataset,
        variables=args.variables
    )

    # 统计结果
    success_count = sum(1 for r in results.values() if r)
    total_count = len(results)

    print(f"\n下载完成!")
    print(f"成功: {success_count}/{total_count}")
    print(f"失败: {total_count - success_count}/{total_count}")


if __name__ == "__main__":
    main()