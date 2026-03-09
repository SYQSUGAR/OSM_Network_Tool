import os
import osmnx as ox
import osm2gmns as og
import pandas as pd

def download_from_osmnx(city_name, log_callback=print):
    """
    模式1: 联网下载
    使用 osmnx 下载指定城市的道路网络数据。
    返回原始的 GeoDataFrames。
    """
    log_callback(f"开始从 osmnx 下载 '{city_name}' 的路网数据...")
    G = ox.graph_from_place(city_name, network_type='drive')
    nodes, edges = ox.graph_to_gdfs(G)
    log_callback("osmnx 数据下载并转换为 GeoDataFrames 成功。")
    # osmnx 直接返回 (nodes, edges)
    return nodes, edges

def process_from_osm_file(osm_file_path, output_directory, log_callback=print):
    """
    模式2: OSM数据处理
    使用 osm2gmns 处理本地的 .osm 文件。
    它会在指定的 output_directory 中生成 link.csv 和 node.csv，然后读取并返回它们。
    """
    log_callback(f"开始使用 osm2gmns 处理 .osm 文件: {osm_file_path}")
    log_callback(f"osm2gmns 输出目录: {output_directory}")

    # 确保输出目录存在
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    net = og.getNetFromFile(osm_file_path, network_types=('auto',), POI=False)
    og.consolidateComplexIntersections(net, auto_identify=True)
    og.outputNetToCSV(net, output_folder=output_directory)

    link_csv_path = os.path.join(output_directory, 'link.csv')
    node_csv_path = os.path.join(output_directory, 'node.csv')

    if not os.path.exists(link_csv_path) or not os.path.exists(node_csv_path):
        raise FileNotFoundError("osm2gmns 未能成功生成 link.csv 或 node.csv 文件。")

    log_callback("osm2gmns 处理完成，正在读取生成的 CSV 文件...")
    
    # 辅助读取函数，支持自动编码尝试
    def _read_generated_csv(path):
        try:
            return pd.read_csv(path, encoding='gbk')
        except UnicodeDecodeError:
            log_callback(f"GBK 读取失败，切换 UTF-8: {path}")
            return pd.read_csv(path, encoding='utf-8')
        except Exception:
             # 其他异常也尝试 utf-8
            return pd.read_csv(path, encoding='utf-8')

    links_df = _read_generated_csv(link_csv_path)
    nodes_df = _read_generated_csv(node_csv_path)
    
    log_callback("osm2gmns 生成的 CSV 文件读取成功。")
    # osm2gmns 处理后返回 (nodes, links)
    return nodes_df, links_df

def read_from_csv_files(link_path, node_path, log_callback=print):
    """
    模式3: 数据处理 (支持 CSV 和 Excel)
    直接从用户提供的两个文件中读取 link 和 node 数据。
    """
    def _read_file(path):
        ext = os.path.splitext(path)[1].lower()
        if ext in ['.xlsx', '.xls']:
            return pd.read_excel(path)
        else:
            # 尝试使用 gbk 读取，失败则尝试 utf-8
            try:
                return pd.read_csv(path, encoding='gbk')
            except UnicodeDecodeError:
                log_callback(f"GBK 读取失败，尝试使用 UTF-8 读取: {path}")
                return pd.read_csv(path, encoding='utf-8')
            except Exception as e:
                # 如果是其他错误，也尝试 utf-8 兜底，或者抛出异常
                log_callback(f"读取 CSV 出错 ({e})，尝试使用 UTF-8 读取: {path}")
                return pd.read_csv(path, encoding='utf-8')

    log_callback(f"正在读取 Link 文件: {link_path}")
    log_callback(f"正在读取 Node 文件: {node_path}")
    
    links_df = _read_file(link_path)
    nodes_df = _read_file(node_path)
    
    log_callback("文件读取成功。")
    # 读取后返回 (nodes, links)
    return nodes_df, links_df
