import os
import shutil
import tempfile
import subprocess
import sys
import uuid
import osmnx as ox
import pandas as pd

def _is_ascii(s):
    """检查字符串是否仅包含纯英文字符 (ASCII)"""
    return all(ord(c) < 128 for c in s)

def _get_safe_ascii_temp_dir():
    """获取一个绝对安全的纯英文临时目录"""
    default_temp = tempfile.gettempdir()
    if _is_ascii(default_temp):
        return default_temp
        
    # 如果用户的电脑用户名也是中文导致默认临时目录带中文，则强制使用系统盘根目录
    drive = os.path.splitdrive(sys.executable)[0] or "C:"
    fallback = os.path.join(drive + os.sep, "osm_tool_temp")
    if not os.path.exists(fallback):
        os.makedirs(fallback, exist_ok=True)
    return fallback

def download_from_osmnx(city_name, log_callback=print):
    """
    模式1: 联网下载
    """
    log_callback(f"开始从 osmnx 下载 '{city_name}' 的路网数据...")
    G = ox.graph_from_place(city_name, network_type='drive')
    nodes, edges = ox.graph_to_gdfs(G)
    log_callback("osmnx 数据下载并转换为 GeoDataFrames 成功。")
    return nodes, edges

def process_from_osm_file(osm_file_path, output_directory, log_callback=print):
    """
    模式2: OSM数据处理 (支持中文路径安全沙箱版)
    """
    osm_file_path = os.path.normpath(os.path.abspath(osm_file_path))
    output_directory = os.path.normpath(os.path.abspath(output_directory))

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    log_callback(f"准备解析本地 OSM 文件: {osm_file_path}")
    
    # ================= 核心修复：中文路径自动转移沙箱机制 =================
    # 判断原始路径是否包含中文
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
    # =====================================================================

    # 我们将你测试成功的代码写成一个临时的独立 py 文件
    script_content = """
import osm2gmns as og
import sys

osm_file = sys.argv[1]
out_dir = sys.argv[2]

try:
    sys.stdout.reconfigure(line_buffering=True)
    net = og.getNetFromFile(osm_file, network_types='auto')
    og.consolidateComplexIntersections(net, auto_identify=True)
    og.outputNetToCSV(net, out_dir)
except Exception as e:
    print(f"FATAL_ERROR: {e}")
    sys.exit(1)
"""
    # 临时脚本存放在安全的英文目录下
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

        # 实时读取 C 库日志
        for line in process.stdout:
            clean_line = line.strip()
            if clean_line:
                log_callback(f"[OSM2GMNS] {clean_line}")

        process.wait()

        if process.returncode != 0:
            raise RuntimeError("后台解析进程异常退出，请检查上述日志。")

        # ================= 沙箱数据回传 =================
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
        # ================================================

    finally:
        # 无论成功与否，彻底清理临时构建的沙箱和脚本
        if needs_safe_workspace and safe_workspace and os.path.exists(safe_workspace):
            try:
                shutil.rmtree(safe_workspace, ignore_errors=True)
            except:
                pass
        elif not needs_safe_workspace and os.path.exists(temp_script_path):
            try:
                os.remove(temp_script_path)
            except:
                pass

    # ---- 下面是解析完成后的读取逻辑，保持不变 ----
    link_csv_path = os.path.join(output_directory, 'link.csv')
    node_csv_path = os.path.join(output_directory, 'node.csv')

    if not os.path.exists(link_csv_path) or not os.path.exists(node_csv_path):
        raise FileNotFoundError("osm2gmns 未能成功生成 link.csv 或 node.csv 文件。")

    log_callback("解析完成！正在读取生成的 CSV 文件至内存...")
    
    def _read_generated_csv(path):
        try:
            return pd.read_csv(path, encoding='gbk')
        except UnicodeDecodeError:
            log_callback(f"GBK 读取失败，切换 UTF-8: {path}")
            return pd.read_csv(path, encoding='utf-8')
        except Exception:
            return pd.read_csv(path, encoding='utf-8')

    links_df = _read_generated_csv(link_csv_path)
    nodes_df = _read_generated_csv(node_csv_path)
    
    log_callback("数据成功加载！准备进入下一步。")
    return nodes_df, links_df


def read_from_csv_files(link_path, node_path, log_callback=print):
    """
    模式3: 直接读取 CSV
    """
    def _read_file(path):
        ext = os.path.splitext(path)[1].lower()
        if ext in ['.xlsx', '.xls']:
            return pd.read_excel(path)
        else:
            try:
                return pd.read_csv(path, encoding='gbk')
            except UnicodeDecodeError:
                log_callback(f"GBK 读取失败，尝试使用 UTF-8 读取: {path}")
                return pd.read_csv(path, encoding='utf-8')
            except Exception as e:
                log_callback(f"读取 CSV 出错 ({e})，尝试使用 UTF-8 读取: {path}")
                return pd.read_csv(path, encoding='utf-8')

    log_callback(f"正在读取 Link 文件: {link_path}")
    log_callback(f"正在读取 Node 文件: {node_path}")
    
    links_df = _read_file(link_path)
    nodes_df = _read_file(node_path)
    
    log_callback("文件读取成功。")
    return nodes_df, links_df