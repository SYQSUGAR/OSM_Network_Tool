import os
import shutil
import tempfile
import subprocess
import sys
import uuid
import pandas as pd
import geopandas as gpd
from shapely import wkt
import requests
import re

try:
    import osmnx as ox
except ImportError:
    pass

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

def _standardize_geometry(nodes_df, links_df, log_callback):
    log_callback("正在统一构建空间几何对象(Geometry)...")
    def safe_wkt_load(x):
        if isinstance(x, str):
            try: return wkt.loads(x)
            except: return None
        return x

    if 'geometry' in links_df.columns:
        links_df['geometry'] = links_df['geometry'].apply(safe_wkt_load)
        links_gdf = gpd.GeoDataFrame(links_df, geometry='geometry', crs="EPSG:4326")
    else:
        links_gdf = gpd.GeoDataFrame(links_df)

    if 'geometry' not in nodes_df.columns:
        x_col, y_col = None, None
        if 'x_coord' in nodes_df.columns and 'y_coord' in nodes_df.columns: x_col, y_col = 'x_coord', 'y_coord'
        elif 'lon' in nodes_df.columns and 'lat' in nodes_df.columns: x_col, y_col = 'lon', 'lat'
        elif '经度' in nodes_df.columns and '纬度' in nodes_df.columns: x_col, y_col = '经度', '纬度'
        
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

def search_city_boundaries(city_name, log_callback=print):
    """
    根据城市名搜索边界信息
    返回: GeoDataFrame (包含 candidates)
    """
    try:
        import osmnx as ox
    except ImportError:
        raise ImportError("需要安装 osmnx 库才能使用联网下载功能 (pip install osmnx)")

    log_callback(f"正在搜索 '{city_name}' 的地理边界信息...")
    try:
        # 使用 osmnx 获取所有匹配项
        gdf = ox.geocode_to_gdf(city_name)
        if gdf.empty:
            log_callback(f"未找到城市: {city_name}")
            return None
            
        log_callback(f"找到 {len(gdf)} 个匹配项。")
        # 确保包含必要的列 (osmnx版本差异处理)
        if 'display_name' not in gdf.columns:
            # 有些旧版本可能没有 display_name，尝试用 name + type 组合
            gdf['display_name'] = gdf['name'] if 'name' in gdf.columns else city_name
            
        return gdf
    except Exception as e:
        log_callback(f"搜索失败: {e}")
        raise e

def download_osm_by_area_id(area_id, output_path, log_callback=print):
    """
    根据确定的 Area ID 下载 OSM 数据
    """
    log_callback(f"正在根据 Area ID ({area_id}) 下载数据...")
    try:
        # 2. Construct Query
        query = f"""
        [out:xml][timeout:180];
        area({area_id})->.searchArea;
        (
          way["highway"](area.searchArea);
        );
        (._;>;);
        out meta;
        """
        
        # 3. Download
        overpass_url = "https://overpass-api.de/api/interpreter"
        response = requests.post(overpass_url, data={'data': query}, stream=True, timeout=180)
        
        if response.status_code != 200:
            raise RuntimeError(f"Overpass API 请求失败 (代码 {response.status_code}): {response.text[:200]}")
            
        log_callback(f"正在下载数据至: {output_path}")
        total_size = 0
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024): 
                if chunk:
                    f.write(chunk)
                    total_size += len(chunk)
                    
        log_callback(f"下载完成，文件大小: {total_size / 1024 / 1024:.2f} MB")
        return True
    except Exception as e:
        log_callback(f"下载失败: {e}")
        raise e

def download_osm_xml(city_name, output_path, log_callback=print):
    """
    下载 OSM XML 数据到指定路径 (简易版，直接取第一个匹配项)
    """
    gdf = search_city_boundaries(city_name, log_callback)
    if gdf is None or gdf.empty:
        raise ValueError(f"无法找到城市: {city_name}")
    
    row = gdf.iloc[0]
    osm_id = row['osmid'] if 'osmid' in row else row.name
    osm_type = row['osm_type'] if 'osm_type' in row else 'relation'
    
    area_id = int(osm_id)
    if osm_type == 'relation':
        area_id += 3600000000
    elif osm_type == 'way':
        area_id += 2400000000
        
    return download_osm_by_area_id(area_id, output_path, log_callback)

def process_from_osm_file(osm_file_path, output_directory, log_callback=print):
    osm_file_path = os.path.normpath(os.path.abspath(osm_file_path))
    output_directory = os.path.normpath(os.path.abspath(output_directory))

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    log_callback(f"准备解析本地 OSM 文件: {osm_file_path}")
    
    needs_safe_workspace = not (_is_ascii(osm_file_path) and _is_ascii(output_directory))
    safe_osm_path = osm_file_path
    safe_out_dir = output_directory
    safe_workspace = None

    if needs_safe_workspace:
        log_callback("⚠️ 检测到路径包含中文字符，正在构建全英文安全沙箱环境...")
        safe_base = _get_safe_ascii_temp_dir()
        safe_workspace = os.path.join(safe_base, f"osm_wksp_{uuid.uuid4().hex[:8]}")
        os.makedirs(safe_workspace, exist_ok=True)
        
        safe_osm_path = os.path.join(safe_workspace, "input.osm")
        safe_out_dir = os.path.join(safe_workspace, "output")
        os.makedirs(safe_out_dir, exist_ok=True)
        
        log_callback("正在将数据拷贝至安全沙箱...")
        shutil.copy2(osm_file_path, safe_osm_path)

    # ================== 核心修正：去除极易引发误判的底层探针 ==================
    script_content = """
import osm2gmns as og
import sys

osm_file = sys.argv[1]
out_dir = sys.argv[2]

try:
    sys.stdout.reconfigure(line_buffering=True)
    net = og.getNetFromFile(osm_file, network_types='auto')
    
    # 我们彻底移除了对 net.node_list 这种底层属性的硬核检查，
    # 全权交给 osm2gmns 处理，并在最后用 Pandas 来检验结果。
    
    try:
        og.consolidateComplexIntersections(net, auto_identify=True)
    except Exception as e:
        print(f"WARN: 合并复杂交叉口遇到异常 ({e})，为保证数据完整，已自动跳过该步骤。")
        
    og.outputNetToCSV(net, out_dir)
except Exception as e:
    print(f"FATAL_ERROR: {e}")
    sys.exit(1)
"""
    # =======================================================================
    
    script_dir = safe_workspace if needs_safe_workspace else output_directory
    temp_script_path = os.path.join(script_dir, "_temp_osm_parser.py")
    with open(temp_script_path, "w", encoding="utf-8") as f:
        f.write(script_content)

    log_callback("启动纯净后台解析器...")
    try:
        kwargs = {}
        if os.name == 'nt':
            kwargs['creationflags'] = subprocess.CREATE_NO_WINDOW

        process = subprocess.Popen(
            [sys.executable, temp_script_path, safe_osm_path, safe_out_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='ignore',
            **kwargs
        )

        for line in process.stdout:
            clean_line = line.strip()
            if clean_line:
                log_callback(f"[OSM2GMNS] {clean_line}")

        process.wait()

        if process.returncode != 0:
            raise RuntimeError("后台解析进程异常退出，请检查上述日志。")

        if needs_safe_workspace:
            log_callback("沙箱解析成功！正在将结果回传至中文目录...")
            temp_link = os.path.join(safe_out_dir, 'link.csv')
            temp_node = os.path.join(safe_out_dir, 'node.csv')
            real_link = os.path.join(output_directory, 'link.csv')
            real_node = os.path.join(output_directory, 'node.csv')
            
            if os.path.exists(temp_link) and os.path.exists(temp_node):
                shutil.copy2(temp_link, real_link)
                shutil.copy2(temp_node, real_node)
            else:
                raise FileNotFoundError("在安全沙箱中未找到生成的 csv 文件。")

    finally:
        if needs_safe_workspace and safe_workspace and os.path.exists(safe_workspace):
            try: shutil.rmtree(safe_workspace, ignore_errors=True)
            except: pass
        elif not needs_safe_workspace and os.path.exists(temp_script_path):
            try: os.remove(temp_script_path)
            except: pass

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
    
    # ====== 将防线设置在主程序中：用 Pandas 准确判断是否提取成功 ======
    if nodes_df.empty or links_df.empty:
        raise ValueError("解析失败：虽然文件下载成功，但未能提取到任何路网数据 (提取后的行数为0)。这可能是因为所选区域确实没有任何符合条件的道路。")
    # =================================================================

    nodes_gdf, links_gdf = _standardize_geometry(nodes_df, links_df, log_callback)
    log_callback("数据成功加载！准备进入下一步。")
    return nodes_gdf, links_gdf

def read_from_csv_files(link_path, node_path, log_callback=print):
    def _read_file(path):
        ext = os.path.splitext(path)[1].lower()
        if ext in ['.xlsx', '.xls']: return pd.read_excel(path)
        else:
            try: return pd.read_csv(path, encoding='gbk')
            except: return pd.read_csv(path, encoding='utf-8')

    log_callback(f"正在读取 Link 文件: {link_path}")
    log_callback(f"正在读取 Node 文件: {node_path}")
    links_df = _read_file(link_path)
    nodes_df = _read_file(node_path)
    log_callback("文件读取成功。")
    
    nodes_gdf, links_gdf = _standardize_geometry(nodes_df, links_df, log_callback)
    return nodes_gdf, links_gdf