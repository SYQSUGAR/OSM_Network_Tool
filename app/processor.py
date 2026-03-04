import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point, LineString
import os
import networkx as nx
import re

# ==============================================================================
# 核心处理类 (Core Processing Logic)
# ==============================================================================

class DataProcessor:
    def __init__(self):
        self.links_df = None
        self.nodes_df = None
        self.source_links_df = None
        self.source_nodes_df = None
        self.block_stats_df = None
        self.mapping_files = {}
        self.preview_links_gdf = None
        self.preview_nodes_gdf = None

    # --- Stage 1: Pre-processing ---
    def run_preprocessing(self, links_df, nodes_df, node_map_path, link_map_path, attr_map_path, log_callback=print):
        self.links_df = links_df.copy()
        self.nodes_df = nodes_df.copy()
        log_callback("步骤 1/4: 读取映射文件...")
        self._read_mapping_files(node_map_path, link_map_path, attr_map_path, log_callback)
        
        log_callback("步骤 2/4: 处理Link数据...")
        self._process_link_headers(log_callback)
        self._process_link_attributes(log_callback)
        
        log_callback("步骤 3/4: 处理Node数据...")
        self._process_node_headers(log_callback)
        
        log_callback("步骤 4/4: 检查区块连通性...")
        self._check_blocks(log_callback)
        
        # 备份原始处理结果
        self.source_links_df = self.links_df.copy()
        self.source_nodes_df = self.nodes_df.copy()
        
        return self.links_df, self.nodes_df, self.block_stats_df

    # --- Stage 2: Filtering and Completion (Preview Generation) ---
    def generate_preview_data(self, filter_criteria, log_callback=print):
        """
        根据筛选条件生成预览数据，并保存到 preview 变量中。
        不会修改 self.links_df 和 self.nodes_df，而是基于 self.source_* 生成。
        """
        log_callback("正在生成预览数据...")
        
        if self.source_links_df is None or self.source_nodes_df is None:
             raise ValueError("请先运行预处理。")

        # 使用 source 数据的副本进行操作
        temp_links_df = self.source_links_df.copy()
        temp_nodes_df = self.source_nodes_df.copy()
        
        log_callback(f"步骤 1/3: 保留区块: {filter_criteria}...")
        temp_links_df = temp_links_df[temp_links_df['区块ID'].isin(filter_criteria)]
        temp_nodes_df = temp_nodes_df[temp_nodes_df['区块ID'].isin(filter_criteria)]
        
        log_callback("步骤 2/3: 补全路段...")
        temp_links_df = self._complete_links(temp_links_df, temp_nodes_df, log_callback)
        
        log_callback("步骤 3/3: 移除重复路段...")
        temp_links_df = self._remove_duplicate_links(temp_links_df, log_callback)
        
        # 转换为 GeoDataFrame 并保存到 preview 变量
        log_callback("正在转换为 GeoDataFrame...")
        try:
            if 'geometry' in temp_links_df.columns:
                 # 确保 geometry 列是字符串或 WKT 对象
                temp_links_df['geometry'] = temp_links_df['geometry'].apply(lambda x: wkt.loads(x) if isinstance(x, str) else x)
                self.preview_links_gdf = gpd.GeoDataFrame(temp_links_df, geometry='geometry')
                # 为预览数据设置默认坐标参考系为 WGS84
                if self.preview_links_gdf.crs is None:
                    self.preview_links_gdf.set_crs(epsg=4326, inplace=True)
            
            if 'geometry' in temp_nodes_df.columns:
                temp_nodes_df['geometry'] = temp_nodes_df['geometry'].apply(lambda x: wkt.loads(x) if isinstance(x, str) else x)
                self.preview_nodes_gdf = gpd.GeoDataFrame(temp_nodes_df, geometry='geometry')
                if self.preview_nodes_gdf.crs is None:
                    self.preview_nodes_gdf.set_crs(epsg=4326, inplace=True)
                
            log_callback("预览数据生成成功！")
            return True
        except Exception as e:
            log_callback(f"转换 GeoDataFrame 失败: {e}")
            import traceback
            log_callback(traceback.format_exc())
            return False

    def export_preview_data(self, output_dir, log_callback=print):
        """导出当前预览数据"""
        if self.preview_links_gdf is None or self.preview_nodes_gdf is None:
            raise ValueError("没有可导出的预览数据，请先生成预览。")
            
        # 将 GeoDataFrame 转回 DataFrame 以便复用 export_results 逻辑 (或直接导出)
        # 这里为了保持一致性，我们把 geometry 列转回 WKT 字符串给 export_results 使用 (如果它需要)
        # 但 export_results 内部又会转回 geometry...
        # 简单起见，我们直接用 GeoDataFrame 导出 SHP，用 pd.DataFrame 导出 Excel
        
        log_callback("开始导出预览数据...")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        link_excel_path = os.path.join(output_dir, "link_processed.xlsx")
        node_excel_path = os.path.join(output_dir, "node_processed.xlsx")
        link_shp_path = os.path.join(output_dir, "link_processed.shp")
        node_shp_path = os.path.join(output_dir, "node_processed.shp")
        
        # 导出 Excel (去掉 geometry 列如果它是对象)
        links_df_export = pd.DataFrame(self.preview_links_gdf.drop(columns='geometry'))
        # 把 geometry 加回来作为字符串
        links_df_export['geometry'] = self.preview_links_gdf['geometry'].apply(lambda x: x.wkt)
        
        nodes_df_export = pd.DataFrame(self.preview_nodes_gdf.drop(columns='geometry'))
        nodes_df_export['geometry'] = self.preview_nodes_gdf['geometry'].apply(lambda x: x.wkt)

        log_callback(f"正在写入: {os.path.basename(link_excel_path)}")
        links_df_export.to_excel(link_excel_path, index=False)
        
        log_callback(f"正在写入: {os.path.basename(node_excel_path)}")
        nodes_df_export.to_excel(node_excel_path, index=False)
        
        log_callback(f"正在生成: {os.path.basename(link_shp_path)}")
        self.preview_links_gdf.to_file(link_shp_path, encoding='gbk')
        
        log_callback(f"正在生成: {os.path.basename(node_shp_path)}")
        self.preview_nodes_gdf.to_file(node_shp_path, encoding='gbk')
        
        log_callback(f"文件已保存至: {output_dir}")
        return output_dir

    # 兼容旧接口
    def run_filtering_and_completion(self, filter_criteria, log_callback=print):
        # 这个方法现在可以调用 generate_preview_data，然后更新 self.links_df 以保持向后兼容
        # 但为了清晰，我们假设外部现在应该使用 generate_preview_data
        # 如果必须保留这个方法给 WorkerThread 调用...
        
        success = self.generate_preview_data(filter_criteria, log_callback)
        if success:
            # 更新 self.links_df 以供可能的后续使用 (虽然现在主要用 preview_gdf)
            # 注意：这里我们把 gdf 转回 df
            self.links_df = pd.DataFrame(self.preview_links_gdf)
            self.nodes_df = pd.DataFrame(self.preview_nodes_gdf)
            # geometry 列现在是对象，如果后续代码期望字符串，可能需要转换，但 DataProcessor 内部应该能处理
            return self.links_df, self.nodes_df
        else:
            raise Exception("生成数据失败")

    # --- Private Helper Methods for Stage 1 ---
    def _read_mapping_files(self, node_map_path, link_map_path, attr_map_path, log_callback=print):
        log_callback("  读取中...")
        # Link 表头映射
        link_title_raw = pd.read_excel(link_map_path, index_col=0)
        link_title_map = link_title_raw.set_index('OSM表头')
        # Node 表头映射
        node_title_raw = pd.read_excel(node_map_path, index_col=0)
        node_title_map = node_title_raw.set_index('OSM表头')
        # 属性映射
        attr_map = pd.read_excel(attr_map_path, index_col=[0, 1])

        self.mapping_files = {
            'link_map': link_title_map,
            'link_map_raw': link_title_raw,
            'node_map': node_title_map,
            'node_map_raw': node_title_raw,
            'attr_map': attr_map
        }

    def _get_chn_title(self, name, map_type='link'):
        """获取指定原始表头对应的中文表头名"""
        raw_map = self.mapping_files.get(f'{map_type}_map_raw')
        if raw_map is not None:
            # 检查 name 是否在索引中
            if name in raw_map.index:
                return raw_map.loc[name, '中文表头']
        # 如果没找到，返回 None 或者 name 本身? 最好返回 None 以便调用者处理
        return None
        
    def get_chn_title_public(self, name, map_type='link'):
        """公开的获取中文表头方法"""
        return self._get_chn_title(name, map_type)

    def _process_link_headers(self, log_callback=print):
        log_callback("  替换Link表头...")
        link_map = self.mapping_files['link_map']
        original_links = self.links_df.copy()

        # 保留有用的列
        valid_cols = link_map[link_map.index.notnull() & link_map['中文表头'].notnull()].index.tolist()
        self.links_df = self.links_df[[col for col in valid_cols if col in self.links_df.columns]]

        # 替换为中文表头
        self.links_df.columns = link_map.loc[self.links_df.columns, '中文表头'].tolist()

        # 添加原本没有的列
        missing_cols = link_map[link_map.index.isnull()]['中文表头'].tolist()
        self.links_df.loc[:, missing_cols] = 1.0

        # 添加后续判断需要的列 (例如 is_link)
        if 'is_link' in original_links.columns:
            self.links_df.loc[:, 'is_link'] = original_links['is_link']

    def _process_link_attributes(self, log_callback=print):
        log_callback("  更新/填充Link属性...")
        attr_map = self.mapping_files['attr_map']
        road_level_col = self._get_chn_title('道路等级')
        lanes_col = self._get_chn_title('机动车道数')
        lane_width_col = self._get_chn_title('机动车道宽度')
        sep_col = self._get_chn_title('机非分隔')
        non_motor_width_col = self._get_chn_title('非机动车道宽度')
        name_col = self._get_chn_title('道路名称')
        from_node_col = self._get_chn_title('起点')
        to_node_col = self._get_chn_title('终点')

        # 筛选掉无效的道路等级
        valid_levels = attr_map.index.levels[0]
        self.links_df = self.links_df[self.links_df[road_level_col].isin(valid_levels)]

        road_index = self.links_df[[road_level_col, 'is_link']].values.tolist()
        
        # 替换车道数 (仅当为空时)
        lanes_null_mask = self.links_df[lanes_col].isnull()
        if lanes_null_mask.any():
            lane_null_road_index = self.links_df.loc[lanes_null_mask, [road_level_col, 'is_link']].values.tolist()
            self.links_df.loc[lanes_null_mask, lanes_col] = attr_map.loc[lane_null_road_index, '机动车道数'].tolist()

        # 批量替换属性
        self.links_df[lane_width_col] = attr_map.loc[road_index, '机动车道宽度'].tolist()
        self.links_df[sep_col] = attr_map.loc[road_index, '机非分隔'].tolist()
        self.links_df[non_motor_width_col] = attr_map.loc[road_index, '非机动车道宽度'].tolist()
        self.links_df[road_level_col] = attr_map.loc[road_index, '道路等级Num'].tolist()

        # 补全道路名称
        name_null_mask = self.links_df[name_col].isnull()
        self.links_df.loc[name_null_mask, name_col] = pd.Series(self.links_df[name_null_mask].index).apply(lambda r: f"{r}路").tolist()

        # 更新数据类型
        for col, dtype in {
            from_node_col: int, to_node_col: int, lanes_col: int,
            non_motor_width_col: int, sep_col: int, road_level_col: int
        }.items():
            if col in self.links_df.columns:
                self.links_df[col] = self.links_df[col].astype(dtype)
        
        # 按最终顺序排列
        final_cols = self.mapping_files['link_map_raw'][~self.mapping_files['link_map_raw']['中文表头'].isnull()]['中文表头']
        self.links_df = self.links_df[[col for col in final_cols if col in self.links_df.columns]]

    def _process_node_headers(self, log_callback=print):
        log_callback("  替换Node表头...")
        node_map = self.mapping_files['node_map']
        
        # 确保geometry列存在
        if 'geometry' not in self.nodes_df.columns and 'x_coord' in self.nodes_df.columns:
            self.nodes_df['geometry'] = self.nodes_df.apply(lambda r: f"POINT({r['x_coord']} {r['y_coord']})", axis=1)

        # 保留并重命名列
        valid_cols = node_map[node_map.index.notnull()].index.tolist()
        self.nodes_df = self.nodes_df[[col for col in valid_cols if col in self.nodes_df.columns]]
        self.nodes_df.columns = node_map.loc[self.nodes_df.columns, '中文表头'].tolist()

        # 填充类型
        type_col = self._get_chn_title('类型', 'node')
        if type_col:
            self.nodes_df.loc[:, type_col] = 2 # 默认为2

    def _check_blocks(self, log_callback=print):
        log_callback("  使用 networkx 进行连通性分析...")
        from_node_col = self._get_chn_title('起点')
        to_node_col = self._get_chn_title('终点')
        node_id_col = self._get_chn_title('编号', 'node')

        G = nx.from_pandas_edgelist(self.links_df, from_node_col, to_node_col, create_using=nx.Graph())
        
        # 获取连通分量
        components = list(nx.connected_components(G))
        node_to_block = {node: i for i, comp in enumerate(components) for node in comp}
        
        # 添加中文列名: 区块ID
        self.nodes_df['区块ID'] = self.nodes_df[node_id_col].map(node_to_block)
        self.links_df['区块ID'] = self.links_df[from_node_col].map(node_to_block)

        # 计算出入度 (使用中文列名)
        G_di = nx.from_pandas_edgelist(self.links_df, from_node_col, to_node_col, create_using=nx.DiGraph())
        in_degree = dict(G_di.in_degree())
        out_degree = dict(G_di.out_degree())
        self.nodes_df['入度'] = self.nodes_df[node_id_col].map(in_degree).fillna(0).astype(int)
        self.nodes_df['出度'] = self.nodes_df[node_id_col].map(out_degree).fillna(0).astype(int)
        self.nodes_df['是否断头路'] = ((self.nodes_df['入度'] == 0) | (self.nodes_df['出度'] == 0)).astype(int)

        # 生成统计信息 (使用中文列名)
        block_link_counts = self.links_df.groupby('区块ID').size().rename('路段数')
        block_node_counts = self.nodes_df.groupby('区块ID').size().rename('节点数')
        stats_df = pd.concat([block_link_counts, block_node_counts], axis=1).reset_index()

        # 确保数量为整数
        stats_df['路段数'] = stats_df['路段数'].fillna(0).astype(int)
        stats_df['节点数'] = stats_df['节点数'].fillna(0).astype(int)

        # 计算占比
        total_links = len(self.links_df)
        total_nodes = len(self.nodes_df)
        stats_df['路段占比'] = (stats_df['路段数'] / total_links * 100).apply(lambda x: f"{x:.2f}%")
        stats_df['节点占比'] = (stats_df['节点数'] / total_nodes * 100).apply(lambda x: f"{x:.2f}%")

        # 按最终顺序排列并赋值
        self.block_stats_df = stats_df[['区块ID', '路段数', '路段占比', '节点数', '节点占比']]

    # --- Private Helper Methods for Stage 2 ---
    def _filter_blocks(self, filter_criteria, log_callback=print):
        # Deprecated: use generate_preview_data logic instead
        pass

    def _complete_links(self, links_df, nodes_df, log_callback=print):
        log_callback("  补全反向路段...")
        dead_end_nodes = nodes_df[nodes_df['是否断头路'] == 1][self._get_chn_title('编号', 'node')]
        if dead_end_nodes.empty:
            return links_df

        from_node_col = self._get_chn_title('起点')
        to_node_col = self._get_chn_title('终点')
        geom_col = self._get_chn_title('矢量数据', 'link')

        # 找到所有与断头路节点相连的路段
        dead_end_links = links_df[
            (links_df[from_node_col].isin(dead_end_nodes)) |
            (links_df[to_node_col].isin(dead_end_nodes))
        ].copy()

        if dead_end_links.empty:
            return links_df

        # 创建反向路段
        reversed_links = dead_end_links.copy()
        reversed_links[from_node_col], reversed_links[to_node_col] = reversed_links[to_node_col], reversed_links[from_node_col]
        reversed_links[geom_col] = reversed_links[geom_col].apply(self._reverse_geometry_string)
        
        return pd.concat([links_df, reversed_links], ignore_index=True)

    def _reverse_geometry_string(self, geometry_str):
        if not isinstance(geometry_str, str) or not geometry_str.upper().startswith('LINESTRING'):
            return geometry_str
        try:
            coords_str = re.search(r'\((.*)\)', geometry_str).group(1)
            coords = [c.strip().split() for c in coords_str.split(',')]
            reversed_coords = reversed(coords)
            reversed_coords_str = ", ".join([f"{c[0]} {c[1]}" for c in reversed_coords])
            return f"LINESTRING ({reversed_coords_str})"
        except Exception:
            return geometry_str # 如果解析失败，返回原字符串

    def _remove_duplicate_links(self, links_df, log_callback=print):
        log_callback("  移除重复路段...")
        from_node_col = self._get_chn_title('起点')
        to_node_col = self._get_chn_title('终点')
        return links_df.drop_duplicates(subset=[from_node_col, to_node_col], keep='first')

# ==============================================================================
# 文件导出辅助函数 (Export Helper Functions)
# ==============================================================================

def export_results(links_df, nodes_df, output_dir, is_raw=False, log_callback=print):
    """将最终的DataFrame导出为Excel和Shapefile。"""
    log_callback("  开始导出文件...")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    suffix = "raw" if is_raw else "processed"
    link_excel_path = os.path.join(output_dir, f"link_{suffix}.xlsx")
    node_excel_path = os.path.join(output_dir, f"node_{suffix}.xlsx")
    link_shp_path = os.path.join(output_dir, f"link_{suffix}.shp")
    node_shp_path = os.path.join(output_dir, f"node_{suffix}.shp")
    
    log_callback(f"    正在写入: {os.path.basename(link_excel_path)}")
    links_df.to_excel(link_excel_path, index=False)
    log_callback(f"    正在写入: {os.path.basename(node_excel_path)}")
    nodes_df.to_excel(node_excel_path, index=False)
    
    _create_link_shp(links_df.copy(), link_shp_path, log_callback)
    _create_node_shp(nodes_df.copy(), node_shp_path, log_callback)
    
    log_callback(f"  文件已保存至: {output_dir}")
    return output_dir

def _create_link_shp(links_df, path, log_callback=print):
    """从Link DataFrame创建并保存Shapefile。"""
    log_callback(f"    正在生成: {os.path.basename(path)}")
    if 'geometry' not in links_df.columns or links_df['geometry'].isnull().all():
        log_callback("    警告: Link DataFrame中无有效的geometry列，跳过SHP生成。")
        return
    try:
        # 如果 geometry 列已经是对象，直接用，否则 loads
        if links_df['geometry'].dtype == object and isinstance(links_df['geometry'].iloc[0], str):
             links_df['geometry'] = links_df['geometry'].apply(wkt.loads)
             
        gdf = gpd.GeoDataFrame(links_df, geometry='geometry')
        gdf.to_file(path, encoding='gbk')
    except Exception as e:
        log_callback(f"    错误: 生成Link SHP失败 - {e}")

def _create_node_shp(nodes_df, path, log_callback=print):
    """从Node DataFrame创建并保存Shapefile。"""
    log_callback(f"    正在生成: {os.path.basename(path)}")
    if 'geometry' not in nodes_df.columns or nodes_df['geometry'].isnull().all():
        log_callback("    警告: Node DataFrame中无有效的geometry列，跳过SHP生成。")
        return
    try:
        if nodes_df['geometry'].dtype == object and isinstance(nodes_df['geometry'].iloc[0], str):
             nodes_df['geometry'] = nodes_df['geometry'].apply(wkt.loads)
             
        gdf = gpd.GeoDataFrame(nodes_df, geometry='geometry')
        gdf.to_file(path, encoding='gbk')
    except Exception as e:
        log_callback(f"    错误: 生成Node SHP失败 - {e}")
