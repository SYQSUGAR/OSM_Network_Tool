import os
import shutil
import tempfile
import subprocess
import sys
import uuid
import osmnx as ox
import pandas as pd
import geopandas as gpd
from shapely import wkt

def _is_ascii(s):
    return all(ord(c) < 128 for c in s)

def _get_safe_ascii_temp_dir():
    default_temp = tempfile.gettempdir()
    if _is_ascii(default_temp):
        return default_temp
    drive = os.path.splitdrive(sys.executable)[0] or "C:"
    fallback = os.path.join(drive + os.sep, "osm_tool_temp")
    if not os.path.exists(fallback):
        os.makedirs(fallback, exist_ok=True)
    return fallback

# ================= 核心新增：源头空间数据格式化 =================
def _standardize_geometry(nodes_df, links_df, log_callback):
    """在数据读取后，立刻统一生成 geometry 并对齐坐标系 (EPSG:4326)"""
    log_callback("正在统一构建空间几何对象(Geometry)...")
    
    def safe_wkt_load(x):
        if isinstance(x, str):
            try: return wkt.loads(x)
            except: return None
        return x

    # 1. 处理 Link
    if 'geometry' in links_df.columns:
        links_df['geometry'] = links_df['geometry'].apply(safe_wkt_load)
        links_gdf = gpd.GeoDataFrame(links_df, geometry='geometry', crs="EPSG:4326")
    else:
        links_gdf = gpd.GeoDataFrame(links_df)

    # 2. 处理 Node
    if 'geometry' not in nodes_df.columns:
        x_col, y_col = None, None
        if 'x_coord' in nodes_df.columns and 'y_coord' in nodes_df.columns:
            x_col, y_col = 'x_coord', 'y_coord'
        elif 'lon' in nodes_df.columns and 'lat' in nodes_df.columns:
            x_col, y_col = 'lon', 'lat'
        elif '经度' in nodes_df.columns and '纬度' in nodes_df.columns:
            x_col, y_col = '经度', '纬度'
        
        if x_col and y_col:
            nodes_df[x_col] = pd.to_numeric(nodes_df[x_col], errors='coerce')
            nodes_df[y_col] = pd.to_numeric(nodes_df[y_col], errors='coerce')
            nodes_df['geometry'] = gpd.points_from_xy(nodes_df[x_col], nodes_df[y_col])
        else:
            log_callback("警告: Node 缺少坐标列，无法生成 geometry")
    else:
        nodes_df['geometry'] = nodes_df['geometry'].apply(safe_wkt_load)
         
    if 'geometry' in nodes_df.columns:
        nodes_gdf = gpd.GeoDataFrame(nodes_df, geometry='geometry', crs="EPSG:4326")
    else:
        nodes_gdf = gpd.GeoDataFrame(nodes_df)

    return nodes_gdf, links_gdf
# ===============================================================

def download_from_osmnx(city_name, log_callback=print):
    log_callback(f"开始从 osmnx 下载 '{city_name}' 的路网数据...")
    G = ox.graph_from_place(city_name, network_type='drive')
    nodes, edges = ox.graph_to_gdfs(G)
    # osmnx 默认出来的就是带 geometry 和 4326 的 GeoDataFrame
    log_callback("osmnx 数据下载并转换为 GeoDataFrames 成功。")
    return nodes, edges

def process_from_osm_file(osm_file_path, output_directory, log_callback=print):
    # ... (保持沙箱环境代码完全不变，为节省篇幅省略，直到下面读取CSV的部分) ...
    # 【注意：请保留你原来 downloader 中 process_from_osm_file 的沙箱处理代码】
    # 直接看文件最后的读取部分：
    
    link_csv_path = os.path.join(output_directory, 'link.csv')
    node_csv_path = os.path.join(output_directory, 'node.csv')

    if not os.path.exists(link_csv_path) or not os.path.exists(node_csv_path):
        raise FileNotFoundError("osm2gmns 未能成功生成 link.csv 或 node.csv 文件。")

    log_callback("解析完成！正在读取生成的 CSV 文件至内存...")
    
    def _read_generated_csv(path):
        try: return pd.read_csv(path, encoding='gbk')
        except UnicodeDecodeError: return pd.read_csv(path, encoding='utf-8')
        except Exception: return pd.read_csv(path, encoding='utf-8')

    links_df = _read_generated_csv(link_csv_path)
    nodes_df = _read_generated_csv(node_csv_path)
    
    # 核心修改：在 return 之前调用空间化函数
    nodes_gdf, links_gdf = _standardize_geometry(nodes_df, links_df, log_callback)
    log_callback("数据成功加载！准备进入下一步。")
    return nodes_gdf, links_gdf


def read_from_csv_files(link_path, node_path, log_callback=print):
    def _read_file(path):
        ext = os.path.splitext(path)[1].lower()
        if ext in ['.xlsx', '.xls']: return pd.read_excel(path)
        else:
            try: return pd.read_csv(path, encoding='gbk')
            except Exception: return pd.read_csv(path, encoding='utf-8')

    log_callback(f"正在读取 Link 文件: {link_path}")
    log_callback(f"正在读取 Node 文件: {node_path}")
    links_df = _read_file(link_path)
    nodes_df = _read_file(node_path)
    log_callback("文件读取成功。")
    
    # 核心修改：在 return 之前调用空间化函数
    nodes_gdf, links_gdf = _standardize_geometry(nodes_df, links_df, log_callback)
    return nodes_gdf, links_gdf