"""
CMEMS数据下载器（工程化版本）
"""
from copernicusmarine import subset, get
from typing import Dict, Any, List
from pathlib import Path
from datetime import datetime
import os

from downloaders.baseloader import BaseDownloader
import logging
logger = logging.getLogger(__name__)


class CMEMSDownloader(BaseDownloader):
    """CMEMS海洋数据下载器"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config, "CMEMSDownloader")
        self.service_config = config.get('cmems', {})

    def connect(self) -> bool:
        """检查CMEMS凭据"""
        # CMEMS使用环境变量或命令行参数进行认证
        username = os.getenv('CMEMS_USERNAME')
        password = os.getenv('CMEMS_PASSWORD')

        if not username or not password:
            logger.warning("未设置CMEMS凭据环境变量")
            logger.info("请设置 CMEMS_USERNAME 和 CMEMS_PASSWORD 环境变量")
            return False

        return True

    def download_single(self, params: Dict[str, Any],
                        output_path: Path) -> bool:
        """下载单个月份数据"""
        try:
            dataset_cfg = self.service_config['datasets'][params['dataset_name']]

            # 构建下载参数
            download_params = {
                "dataset_id": dataset_cfg['dataset_id'],
                "variables": params.get('variables') or dataset_cfg['variables'],
                "start_datetime": params['start_datetime'],
                "end_datetime": params['end_datetime'],
                "minimum_longitude": dataset_cfg['spatial_range'][0],
                "maximum_longitude": dataset_cfg['spatial_range'][1],
                "minimum_latitude": dataset_cfg['spatial_range'][2],
                "maximum_latitude": dataset_cfg['spatial_range'][3],
                "minimum_depth": dataset_cfg['depth_range'][0],
                "maximum_depth": dataset_cfg['depth_range'][1],
                "output_filename": str(output_path),
                "force_download": params.get('force_download', False)
            }

            logger.info(f"下载CMEMS数据: {params['start_datetime'].strftime('%Y-%m')}")

            # 使用copernicusmarine库下载
            subset(**download_params)

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

    def download_monthly_range(self, start_date: str, end_date: str,
                               dataset_name: str = "glo12_monthly",
                               variables: List[str] = None) -> Dict[str, bool]:
        """下载月平均数据时间序列"""
        results = {}

        if not self.connect():
            logger.error("无法连接到CMEMS服务")
            return results

        # 解析日期范围
        start_year, start_month = map(int, start_date.split('-'))
        end_year, end_month = map(int, end_date.split('-'))

        # 计算总月数
        total_months = (end_year - start_year) * 12 + (end_month - start_month) + 1
        current = 0

        year, month = start_year, start_month
        while year < end_year or (year == end_year and month <= end_month):
            current += 1

            # 生成时间范围
            start_dt = datetime(year, month, 1)
            if month == 12:
                end_dt = datetime(year + 1, 1, 1)
            else:
                end_dt = datetime(year, month + 1, 1)

            # 生成输出路径
            output_path = self.output_dir / f"{dataset_name}_{year}_{month:02d}.nc"

            # 检查是否已存在
            if self.check_existing(output_path):
                logger.info(f"⏭️ 文件已存在，跳过: {output_path}")
                results[f"{year}-{month:02d}"] = True
            else:
                # 下载参数
                params = {
                    'dataset_name': dataset_name,
                    'start_datetime': start_dt,
                    'end_datetime': end_dt,
                    'variables': variables,
                    'force_download': False
                }

                logger.info(f"正在下载 {year}-{month:02d} 数据...")
                success = self.download_with_retry(params, output_path)
                results[f"{year}-{month:02d}"] = success

            # 更新月份
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1

            # 记录进度
            logger.info(f"进度: {current}/{total_months} ({current / total_months * 100:.1f}%)")

        return results

    def get_dataset_info(self, dataset_id: str = None) -> Dict[str, Any]:
        """获取数据集信息"""
        if not dataset_id:
            dataset_id = self.service_config['datasets']['glo12_monthly']['dataset_id']

        try:
            # 使用copernicusmarine的get函数获取数据集信息
            return get(dataset_id)
        except Exception as e:
            logger.error(f"获取数据集信息失败: {e}")
            return {}