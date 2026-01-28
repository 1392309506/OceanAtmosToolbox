"""
配置管理器
"""
import yaml
import os
from pathlib import Path
from typing import Dict, Any
import json


class ConfigManager:
    """管理配置文件的加载和更新"""

    def __init__(self):
        self.config_dir = Path()
        self.configs = {}

    def load_config(self, config_name: str = "config.yaml") -> Dict[str, Any]:
        """加载配置文件"""
        config_path = self.config_dir / config_name

        if not config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_path}")

        with open(config_path, 'r', encoding='utf-8') as f:
            if config_path.suffix in ['.yaml', '.yml']:
                config = yaml.safe_load(f)
            elif config_path.suffix == '.json':
                config = json.load(f)
            else:
                raise ValueError(f"不支持的配置文件格式: {config_path.suffix}")

        self.configs[config_name] = config

        # 覆盖环境变量
        self._apply_env_overrides(config)

        return config

    def _apply_env_overrides(self, config: Dict[str, Any], prefix: str = "") -> None:
        """用环境变量覆盖配置值"""
        for key, value in config.items():
            full_key = f"{prefix}_{key}" if prefix else key

            # 如果是字典，递归处理
            if isinstance(value, dict):
                self._apply_env_overrides(value, full_key.upper())
            else:
                # 检查环境变量
                env_key = f"OCEAN_{full_key.upper()}"
                if env_key in os.environ:
                    env_value = os.environ[env_key]
                    # 尝试转换为适当类型
                    if isinstance(value, bool):
                        config[key] = env_value.lower() in ['true', '1', 'yes']
                    elif isinstance(value, int):
                        config[key] = int(env_value)
                    elif isinstance(value, float):
                        config[key] = float(env_value)
                    else:
                        config[key] = env_value

    def save_config(self, config: Dict[str, Any],
                    config_name: str = "config.yaml") -> None:
        """保存配置到文件"""
        config_path = self.config_dir / config_name

        with open(config_path, 'w', encoding='utf-8') as f:
            if config_path.suffix in ['.yaml', '.yml']:
                yaml.dump(config, f, default_flow_style=False,
                          allow_unicode=True, sort_keys=False)
            elif config_path.suffix == '.json':
                json.dump(config, f, indent=2, ensure_ascii=False)

    def update_config(self, updates: Dict[str, Any],
                      config_name: str = "config.yaml") -> Dict[str, Any]:
        """更新配置"""
        if config_name not in self.configs:
            self.load_config(config_name)

        config = self.configs[config_name]

        # 递归更新配置
        self._deep_update(config, updates)

        # 保存更新后的配置
        self.save_config(config, config_name)

        return config

    def _deep_update(self, original: Dict[str, Any],
                     updates: Dict[str, Any]) -> None:
        """深度更新字典"""
        for key, value in updates.items():
            if key in original and isinstance(original[key], dict) and isinstance(value, dict):
                self._deep_update(original[key], value)
            else:
                original[key] = value