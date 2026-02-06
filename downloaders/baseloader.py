"""
基础下载器抽象类
"""
import os
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Any

class BaseDownloader(ABC):
    """所有下载器的基类"""

    def __init__(self, config: Dict[str, Any], logger_name: str = "BaseDownloader"):
        """
        初始化下载器

        Args:
            config: 配置字典
            logger_name: 日志器名称
        """
        self.config = config
        self.logger = logging.getLogger(logger_name or self.__class__.__name__)

        general_cfg = config.get('general', {}) if isinstance(config, dict) else {}
        self.output_dir = Path(
            config.get('output_base_dir')
            or general_cfg.get('output_base_dir', './data')
        )
        self.max_retries = config.get('max_retries') or general_cfg.get('max_retries', 3)
        self.timeout = config.get('timeout') or general_cfg.get('timeout', 300)

        self.output_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def connect(self) -> bool:
        """连接到数据服务"""
        pass

    @abstractmethod
    def download_single(self, params: Dict[str, Any],
                        output_path: Path) -> bool:
        """下载单个文件"""
        pass

    # @retry_with_backoff(max_retries=3, base_delay=1)
    def download_with_retry(self, params: Dict[str, Any],
                            output_path: Path) -> bool:
        """带重试机制的下载"""
        return self.download_single(params, output_path)

    def generate_output_path(self, template: str,
                             params: Dict[str, Any]) -> Path:
        """根据模板生成输出路径"""
        # 支持 {year}, {month}, {day}, {variable} 等占位符
        filename = template.format(**params)
        return self.output_dir / filename

    def check_existing(self, filepath: Path) -> bool:
        """检查文件是否已存在且完整"""
        if not filepath.exists():
            return False

        # 简单的大小检查（可扩展为校验和检查）
        min_size = 1024  # 至少1KB
        return filepath.stat().st_size > min_size

    def log_progress(self, current: int, total: int,
                     message: str = "") -> None:
        """记录下载进度"""
        if total > 0:
            percent = (current / total) * 100
            self.logger.info(f"{message} [{current}/{total}] {percent:.1f}%")

    def list_available_datasets(self, datasets: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """列出可用数据集（基于配置）"""
        datasets = datasets or {}
        result: Dict[str, Any] = {}
        for name, cfg in datasets.items():
            if isinstance(cfg, dict):
                desc = cfg.get('name') or cfg.get('dataset_id') or ''
                result[name] = desc
            else:
                result[name] = cfg
        return result