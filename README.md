# OceanAtmosToolbox

用于批量下载 C3S 与 CMEMS 海洋数据的工具集，支持命令行和简单 API 示例。

C3S：https://cds.climate.copernicus.eu/datasets

CMEMS: https://data.marine.copernicus.eu/products

后续将持续更新更多数据集并细化下载方法，以及数据预处理手段。

## 功能概览
- C3S ERA5 月平均/小时/气压层数据下载
- CMEMS 月平均海洋产品下载
- 统一配置管理（YAML）
- 可通过环境变量覆盖配置

## 目录结构
- downloaders/：下载器实现
- config/config.yaml：默认配置
- downlaod_c3s.py：C3S 命令行工具
- download_cmes.py：CMEMS 命令行工具
- see.py：用于查看nc文件
- api_example.py：API 示例

## 环境准备（推荐 conda）
> 下面示例使用名为 `down` 的环境。

```powershell
conda create -n down python=3.11
conda activate down
pip install cdsapi copernicusmarine python-dotenv pyyaml xarray
```

如果你要使用已有环境，请确保依赖已安装。

## 认证与环境变量
请创建 `.env` 文件（不要提交到 Git，或隐去个人信息），示例：

```
CMEMS_USERNAME=your_email
CMEMS_PASSWORD=your_password
CDSAPI_URL=https://cds.climate.copernicus.eu/api
CDSAPI_KEY=your_key
```

说明：
- `CMEMS_*` 用于 CMEMS 下载
- `CDSAPI_*` 用于 C3S 下载
- 当前代码会优先读取配置文件中的 `c3s.api_url`，如缺失则回退到环境变量

## 配置文件
默认配置位于 `config/config.yaml`，常用项：
- `general.output_base_dir`：输出目录
- `c3s.datasets`：C3S 数据集定义
- `cmems.datasets`：CMEMS 数据集定义

## C3S 下载
列出可用数据集：

```powershell
python downlaod_c3s.py --list-datasets
```

下载单个月份（示例：2007-01）：

```powershell
python downlaod_c3s.py --start-year 2007 --end-year 2007 --start-month 1 --end-month 1
```

## CMEMS 下载
下载单个月份（示例：1993-01）：

```powershell
python download_cmes.py --start-date 1993-01 --end-date 1993-01
```

## API 示例

```powershell
python api_example.py
```

## 注意事项
- .env 文件包含敏感信息，请加入 .gitignore 并使用 .env.example 共享模板
- 若 PowerShell 无法 `conda activate`，可使用：

```powershell
conda run -n down python downlaod_c3s.py --list-datasets
```
