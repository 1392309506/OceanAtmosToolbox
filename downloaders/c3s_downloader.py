"""
C3S数据下载器
"""
import cdsapi
import os
from typing import Dict, Any, List, Optional
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
        self.client = None

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
                "time": dataset_cfg.get('time', '00:00'),
                "format": dataset_cfg.get('format', 'netcdf')
            }

            # 添加可选参数
            if 'day' in params:
                request_params['day'] = [f"{d:02d}" for d in params['day']]

            logger.info(f"下载C3S数据: {params}")
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

    def download_range(self, start_year: int, end_year: int,
                       dataset_name: str = "era5_monthly",
                       start_month: Optional[int] = None,
                       end_month: Optional[int] = None,
                       variables: List[str] = None) -> Dict[str, bool]:
        """下载指定时间范围的数据"""
        results = {}

        if not self.connect():
            logger.error("无法连接到C3S API")
            return results

        def resolve_month_range(year: int) -> range:
            if start_month is None and end_month is None:
                return range(1, 13)
            if year == start_year and year == end_year:
                sm = start_month or 1
                em = end_month or 12
            elif year == start_year:
                sm = start_month or 1
                em = 12
            elif year == end_year:
                sm = 1
                em = end_month or 12
            else:
                sm = 1
                em = 12
            return range(sm, em + 1)

        total_months = sum(len(list(resolve_month_range(y))) for y in range(start_year, end_year + 1))
        current = 0

        for year in range(start_year, end_year + 1):
            for month in resolve_month_range(year):
                current += 1

                # 生成输出路径
                output_path = self.output_dir / f"{dataset_name}_{year}_{month:02d}.nc"

                # 检查是否已存在
                if self.check_existing(output_path):
                    logger.info(f"⏭️ 文件已存在，跳过: {output_path}")
                    results[f"{year}-{month:02d}"] = True
                    continue

                # 下载参数
                params = {
                    'dataset_name': dataset_name,
                    'year': year,
                    'month': month,
                    'variables': variables
                }

                logger.info(f"正在下载 {year}-{month:02d} 数据...")
                success = self.download_with_retry(params, output_path)
                results[f"{year}-{month:02d}"] = success

                # 记录进度
                logger.info(f"进度: {current}/{total_months} ({current / total_months * 100:.1f}%)")

        return results

    def list_available_datasets(self) -> Dict[str, Any]:
        """列出可用数据集"""
        # 这里可以扩展为从API获取数据集列表
        return {
            "era5_monthly": "ERA5月平均单层数据",
            "era5_hourly": "ERA5小时单层数据",
            "era5_pressure_levels": "ERA5气压层数据",
        }