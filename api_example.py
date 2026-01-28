from utils.config_manager import ConfigManager
from downloaders.c3s_downloader import C3SDownloader
from downloaders.cmems_downloader import CMEMSDownloader

# 加载配置
config_manager = ConfigManager()
config = config_manager.load_config("config/config.yaml")

# 下载C3S数据
c3s_downloader = C3SDownloader(config)
results = c3s_downloader.download_range(2007, 2024)

# 下载CMEMS数据
cmems_downloader = CMEMSDownloader(config)
results = cmems_downloader.download_monthly_range("1993-01", "2024-12")