import sys
import xarray as xr

def main(nc_file):
    try:
        # 尝试打开NetCDF文件
        ds = xr.open_dataset(nc_file)
        
        print("==== 文件结构 ====")
        print(ds)
        
        print("\n==== 变量名 ====")
        print(list(ds.data_vars.keys()))
        
        print("\n==== 坐标变量 ====")
        print(list(ds.coords.keys()))
        
        print("\n==== 维度信息 ====")
        for dim, size in ds.dims.items():
            print(f"{dim}: {size}")
            
        print("\n==== 属性信息 ====")
        for attr, value in ds.attrs.items():
            print(f"{attr}: {value}")
            
    except Exception as e:
        print(f"打开文件时出错: {e}")
        print("尝试使用更宽松的参数打开...")
        try:
            # 尝试使用更宽松的参数
            ds = xr.open_dataset(nc_file, decode_times=False)
            print("文件已打开，但时间解码被禁用")
            print("\n==== 文件结构 ====")
            print(ds)
        except Exception as e2:
            print(f"仍然无法打开文件: {e2}")
            sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python see.py 文件名.nc")
        print("示例: python see.py data.nc")
        sys.exit(1)
    else:
        main(sys.argv[1])