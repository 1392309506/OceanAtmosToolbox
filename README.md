# OceanAtmosToolbox

面向 C3S（Copernicus Climate Data Store）与 CMEMS（Copernicus Marine）数据的批量下载工具集，提供命令行与 API 示例，并支持 YAML 配置与环境变量覆盖。

- C3S 数据集入口：https://cds.climate.copernicus.eu/datasets
- CMEMS 产品入口：https://data.marine.copernicus.eu/products

后续将持续更新更多数据集并细化下载方法，以及数据预处理手段。

## 功能概览
- C3S ERA5 小时/日/月尺度下载
- CMEMS 海洋产品小时/日/月尺度下载
- 统一配置管理（YAML）与环境变量覆盖
- 提供 NetCDF 快速查看工具

## 目录结构
- downloaders/：下载器实现
- utils/：配置管理
- config/config.yaml：默认配置
- downlaod_c3s.py：C3S 命令行工具
- download_cmes.py：CMEMS 命令行工具
- see.py：NetCDF 快速查看
- api_example.py：API 示例

## 环境准备（推荐 conda）
```powershell
conda create -n toolbox python=3.11
conda activate toolbox
pip install cdsapi copernicusmarine python-dotenv pyyaml xarray
```

## 认证与环境变量
建议在项目根目录创建 .env（不要提交到 Git），示例：

```
CMEMS_USERNAME=your_email
CMEMS_PASSWORD=your_password
CDSAPI_URL=https://cds.climate.copernicus.eu/api
CDSAPI_KEY=your_key
```

说明：
- `CMEMS_*` 用于 CMEMS 下载
- `CDSAPI_*` 用于 C3S 下载
- C3S 优先读取配置中的 `c3s.api_url`，缺失时回退到 `CDSAPI_URL`

## 配置文件
默认配置位于 [config/config.yaml](config/config.yaml)。

常用项：
- `general.output_base_dir`：输出目录（默认 ./data）
- `c3s.datasets`：C3S 数据集定义（变量、时间、格式等）
- `cmems.datasets`：CMEMS 数据集定义（变量、空间、深度等）

环境变量覆盖规则：
- 任何配置可用 `OCEAN_` 前缀覆盖，如 `OCEAN_GENERAL_OUTPUT_BASE_DIR=./data`
- 覆盖逻辑见 [utils/config_manager.py](utils/config_manager.py)

## C3S 使用示例
列出可用数据集：

```powershell
python downlaod_c3s.py --list_datasets
```

月平均数据（YYYY-MM）：

```powershell
python downlaod_c3s.py --start_date 2023-01 --end_date 2023-03 --dataset era5_monthly
```

日平均数据（YYYY-MM-DD）：

```powershell
python downlaod_c3s.py --start_date 2023-01-01 --end_date 2023-01-07 --dataset era5_daily
```

小时级数据（YYYY-MM-DD，需加 `--is_hourly`）：

```powershell
python downlaod_c3s.py --start_date 2023-01-01 --end_date 2023-01-03 --dataset era5_hourly --is_hourly
```

使用命令行覆盖配置：

```powershell
python downlaod_c3s.py --use_cli --variables 2m_temperature 10m_u_component_of_wind
```

## CMEMS 使用示例
月平均数据（YYYY-MM）：

```powershell
python download_cmes.py --start_date 2022-01 --end_date 2022-03 --dataset glo12v1_monthly
```

日平均数据（YYYY-MM-DD）：

```powershell
python download_cmes.py --start_date 2022-01-01 --end_date 2022-01-07 --dataset glo12v1_daily
```

小时级数据（YYYY-MM-DD，需加 `--is_hourly`）：

```powershell
python download_cmes.py --start_date 2022-01-01 --end_date 2022-01-02 --dataset glo12v4_hourly --is_hourly
```

使用命令行覆盖配置：

```powershell
python download_cmes.py --use_cli --variables thetao so uo vo
```

## API 示例
```powershell
python api_example.py
```

## NetCDF 查看
```powershell
python see.py path/to/file.nc
```

## 注意事项
- .env 文件包含敏感信息，请加入 .gitignore 并使用 .env.example 共享模板
- PowerShell 无法 conda activate 时可使用：

```powershell
conda run -n toolbox python downlaod_c3s.py --list_datasets
```
