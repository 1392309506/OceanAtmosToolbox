"""
C3S数据下载器
"""
import cdsapi
import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from pathlib import Path
import json

from downloaders.baseloader import BaseDownloader
import logging
logger = logging.getLogger(__name__)


class C3SDownloader(BaseDownloader):
    """C3S ERA5数据下载器"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config, "C3SDownloader")
        self.service_config = config.get('c3s', {})
        self.client: Optional[cdsapi.Client] = None

    def connect(self) -> bool:
        """连接到C3S API"""
        try:
            api_url = self.service_config.get('api_url') or os.getenv('CDSAPI_URL')
            api_key = self.service_config.get('api_key') or os.getenv('CDSAPI_KEY')
            self.client = cdsapi.Client(
                url=api_url,
                key=api_key
            )
            logger.info("C3S API连接成功")
            return True
        except Exception as e:
            logger.error(f"C3S API连接失败: {e}")
            return False

    def download_single(self, params: Dict[str, Any],
                        output_path: Path) -> bool:
        """下载单个月份数据"""
        try:
            # 获取数据集配置
            dataset_cfg = self.service_config['datasets'][params['dataset_name']]

            # 构建请求参数
            request_params = {
                "product_type": dataset_cfg.get('product_type', 'monthly_averaged_reanalysis'),
                "variable": params.get('variables') or dataset_cfg['variables'],
                "year": str(params['year']),
                "month": f"{params['month']:02d}",
                "data_format": dataset_cfg.get('data_format', 'netcdf')
            }

            if params.get('time') or dataset_cfg.get('time'):
                request_params["time"] = params.get('time') or dataset_cfg.get('time')

            # Add dataset-specific optional parameters when present.
            for optional_key in ("daily_statistic", "frequency", "statistic", "time_zone"):
                if optional_key in dataset_cfg:
                    request_params[optional_key] = dataset_cfg[optional_key]

            if 'data_format' in dataset_cfg:
                request_params['data_format'] = dataset_cfg['data_format']
            if 'download_format' in dataset_cfg:
                request_params['download_format'] = dataset_cfg['download_format']

            # 添加可选参数
            if 'day' in params:
                request_params['day'] = [f"{d:02d}" for d in params['day']]

            logger.info(f"下载C3S数据: {params}")
            if not self.client:
                raise RuntimeError("C3S 客户端未初始化")

            self.client.retrieve(
                dataset_cfg['name'],
                request_params,
                str(output_path)
            )

            # 验证文件
            if self.check_existing(output_path):
                logger.info(f"✅ 下载完成: {output_path}")
                return True
            else:
                logger.error(f"文件下载后验证失败: {output_path}")
                return False

        except Exception as e:
            logger.error(f"下载失败: {e}")
            return False


    def download_date_range(self, start_date: str, end_date: str,
                            dataset_name: str = "era5_hourly",
                            variables: Optional[List[str]] = None,
                            hours: Optional[List[str]] = None) -> Dict[str, bool]:
        """按日期范围下载（支持日/小时级）"""
        results = {}

        if not self.connect():
            logger.error("无法连接到C3S API")
            return results

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        if end_dt < start_dt:
            raise ValueError("end_date 不能早于 start_date")

        total_days = (end_dt - start_dt).days + 1
        current = 0

        current_dt = start_dt
        while current_dt <= end_dt:
            current += 1
            year = current_dt.year
            month = current_dt.month
            day = current_dt.day

            output_path = self.output_dir / f"{dataset_name}_{year}{month:02d}{day:02d}.nc"

            if self.check_existing(output_path):
                logger.info(f"⏭️ 文件已存在，跳过: {output_path}")
                results[current_dt.strftime("%Y-%m-%d")] = True
                current_dt += timedelta(days=1)
                continue

            params = {
                'dataset_name': dataset_name,
                'year': year,
                'month': month,
                'day': [day],
                'time': hours,
                'variables': variables
            }

            logger.info(f"正在下载 {current_dt.strftime('%Y-%m-%d')} 数据...")
            success = self.download_with_retry(params, output_path)
            results[current_dt.strftime("%Y-%m-%d")] = success

            logger.info(f"进度: {current}/{total_days} ({current / total_days * 100:.1f}%)")
            current_dt += timedelta(days=1)

        return results

    def list_available_datasets(self) -> Dict[str, Any]:
        """列出可用数据集"""
        datasets = self.service_config.get('datasets', {})
        return super().list_available_datasets(datasets)


    def download_monthly_range(self, start_date: str, end_date: str,
                               dataset_name: str = "era5_monthly",
                               variables: Optional[List[str]] = None) -> Dict[str, bool]:
        """下载月平均数据时间序列"""
        results = {}

        if not self.connect():
            logger.error("无法连接到C3S API")
            return results

        start_year, start_month = map(int, start_date.split('-'))
        end_year, end_month = map(int, end_date.split('-'))

        total_months = (end_year - start_year) * 12 + (end_month - start_month) + 1
        current = 0

        year, month = start_year, start_month
        while year < end_year or (year == end_year and month <= end_month):
            current += 1

            output_path = self.output_dir / f"{dataset_name}_{year}_{month:02d}.nc"

            if self.check_existing(output_path):
                logger.info(f"⏭️ 文件已存在，跳过: {output_path}")
                results[f"{year}-{month:02d}"] = True
            else:
                params = {
                    'dataset_name': dataset_name,
                    'year': year,
                    'month': month,
                    'variables': variables
                }

                logger.info(f"正在下载 {year}-{month:02d} 数据...")
                success = self.download_with_retry(params, output_path)
                results[f"{year}-{month:02d}"] = success

            if month == 12:
                year += 1
                month = 1
            else:
                month += 1

            logger.info(f"进度: {current}/{total_months} ({current / total_months * 100:.1f}%)")

        return results

    def download_daily_range(self, start_date: str, end_date: str,
                             dataset_name: str = "era5_daily",
                             variables: Optional[List[str]] = None,
                             hours: Optional[List[str]] = None) -> Dict[str, bool]:
        """按日期范围下载（日平均）"""
        return self.download_date_range(
            start_date=start_date,
            end_date=end_date,
            dataset_name=dataset_name,
            variables=variables,
            hours=hours
        )

    def download_hourly_range(self, start_date: str, end_date: str,
                              dataset_name: str = "era5_hourly",
                              variables: Optional[List[str]] = None,
                              hours: Optional[List[str]] = None) -> Dict[str, bool]:
        """按日期范围下载（小时级）"""
        return self.download_date_range(
            start_date=start_date,
            end_date=end_date,
            dataset_name=dataset_name,
            variables=variables,
            hours=hours
        )