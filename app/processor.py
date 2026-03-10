import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point, LineString
import os
import networkx as nx
import re
import utm # Need to install: pip install utm

# ==============================================================================
# 核心处理类 (Core Processing Logic)
# ==============================================================================

class DataProcessor:
    def __init__(self):
        self.links_df = None
        self.nodes_df = None
        # 原始数据备份
        self.source_links_df = None
        self.source_nodes_df = None
        
        # 全量处理后的数据 (含所有区块，已补全，已去重)
        self.processed_links_gdf = None
        self.processed_nodes_gdf = None
        
        # 筛选后的预览数据
        self.preview_links_gdf = None
        self.preview_nodes_gdf = None
        
        self.block_stats_df = None
        self.mapping_files = {}

    def update_mappings(self, node_map_df, link_map_df, attr_map_df):
        """
        更新内存中的映射配置。
        接收 pandas DataFrame 格式的映射表。
        """
        try:
            # 清理列名空格
            link_map_df.columns = link_map_df.columns.str.strip()
            node_map_df.columns = node_map_df.columns.str.strip()
            attr_map_df.columns = attr_map_df.columns.str.strip()

            # 数据清洗: 去除字符串列的前后空格，并将空字符串转换为 NaN
            # 解决用户反馈的 "明明是空的单元格，.notnull()判断出来确实true" 的问题
            for df in [link_map_df, node_map_df, attr_map_df]:
                # 仅处理 object 类型的列 (通常是字符串)
                str_cols = df.select_dtypes(include=['object']).columns
                for col in str_cols:
                    # 转为字符串 -> 去空格 -> 将空字符串或 'nan' 替换为 None
                    df[col] = df[col].astype(str).str.strip()
                    df[col] = df[col].replace({'': None, 'nan': None, 'None': None})

            # ------------------------------------------------------------------
            # Link 表头映射处理
            # ------------------------------------------------------------------
            # 必须包含 'OSM表头' 和 '中文表头'
            if 'OSM表头' not in link_map_df.columns:
                 # 尝试自动推断或报错，这里假设第一列是OSM，第二列是中文
                 pass 

            # 1. 以 OSM表头 为索引 (用于筛选和重命名 OSM 数据)
            link_osm_index = link_map_df.set_index('OSM表头', drop=False)
            
            # 2. 以 属性/属性名称 为索引 (用于查找字段名)
            link_attr_col = '属性'
            if link_attr_col in link_map_df.columns:
                # 去除属性列的空格
                link_map_df[link_attr_col] = link_map_df[link_attr_col].astype(str).str.strip()
                link_attr_index = link_map_df.set_index(link_attr_col, drop=False)
            else:
                link_attr_index = None

            # ------------------------------------------------------------------
            # Node 表头映射处理
            # ------------------------------------------------------------------
            node_osm_index = node_map_df.set_index('OSM表头', drop=False)
            
            node_attr_col = '属性' if '属性' in node_map_df.columns else '属性名称'
            if node_attr_col in node_map_df.columns:
                node_map_df[node_attr_col] = node_map_df[node_attr_col].astype(str).str.strip()
                node_attr_index = node_map_df.set_index(node_attr_col, drop=False)
            else:
                node_attr_index = None

            # ------------------------------------------------------------------
            # 属性映射处理 (Road Level + Channel -> Attributes)
            # ------------------------------------------------------------------
            # 严格使用 ['OSM道路等级', '渠道'] 作为索引
            required_indices = ['OSM道路等级', '渠道']
            missing_indices = [col for col in required_indices if col not in attr_map_df.columns]
            
            if not missing_indices:
                # 严格按照用户要求设置数据类型
                # 字符串类型: OSM道路等级, 道路等级
                # 整型: 渠道, 道路等级Num, 机动车道数, 机非分隔
                # 浮点型: 机动车道宽度, 非机动车道宽度
                
                # 1. 字符串
                str_cols = ['OSM道路等级', '道路等级']
                for col in str_cols:
                    if col in attr_map_df.columns:
                        attr_map_df[col] = attr_map_df[col].astype(str).str.strip()
                
                # 2. 整型 (先转numeric处理非数字，再fillna 0，再转int)
                int_cols = ['渠道', '道路等级Num', '机动车道数', '机非分隔']
                for col in int_cols:
                    if col in attr_map_df.columns:
                        attr_map_df[col] = pd.to_numeric(attr_map_df[col], errors='coerce').fillna(0).astype(int)
                        
                # 3. 浮点型
                float_cols = ['机动车道宽度', '非机动车道宽度']
                for col in float_cols:
                    if col in attr_map_df.columns:
                        attr_map_df[col] = pd.to_numeric(attr_map_df[col], errors='coerce')

                # 设置索引
                attr_map = attr_map_df.set_index(required_indices)
            else:
                print(f"Error: Attribute mapping missing required columns: {missing_indices}. Please check your mapping file.")
                # 这里不应该 Fallback，直接返回空或报错更好，但为了不完全崩溃，暂时设为空映射或原样
                # 既然用户说以后都规定为OSM道路等级，这里我们就不做任何猜测了
                # 如果没有这两个列，属性映射将无法正常工作，后续 lookup 会失败
                attr_map = attr_map_df 
                # 可以在这里抛出异常，或者让 UI 层捕获
                # raise ValueError(f"Attribute mapping file must contain columns: {required_indices}")

            self.mapping_files = {
                'link_osm_index': link_osm_index,
                'link_attr_index': link_attr_index,
                'node_osm_index': node_osm_index,
                'node_attr_index': node_attr_index,
                'attr_map': attr_map
            }
            return True
        except Exception as e:
            print(f"Error updating mappings: {e}")
            import traceback
            traceback.print_exc()
            return False

    # --- Stage 1: Full Processing (Preprocessing + Completion + Deduplication) ---
    def run_full_processing(self, links_df, nodes_df, log_callback=print):
        """
        执行全流程处理：
        1. 检查映射文件 (已预先加载)
        2. 预处理 Link/Node (表头、属性)
        3. 连通性分析 (计算 Block ID)
        4. 路段补全 (针对全量数据)
        5. 移除重复路段 (针对全量数据)
        6. 生成 GeoDataFrame 并保存为类变量
        """
        self.links_df = links_df.copy()
        self.nodes_df = nodes_df.copy()
        
        # 1. 检查映射
        if not self.mapping_files:
            log_callback("错误: 映射关系未加载，请先在参数配置页点击'应用'。")
            raise ValueError("Mappings not loaded")
            
        # 2. 预处理
        log_callback("步骤 1/5: 处理Link数据...")
        self._process_link_headers(log_callback)
        self._process_link_attributes(log_callback)
        
        log_callback("步骤 2/5: 处理Node数据...")
        self._process_node_headers(log_callback)
        
        # 3. 连通性分析
        log_callback("步骤 3/5: 检查区块连通性...")
        self._check_blocks(log_callback)
        
        # 4. 路段补全 (全量)
        log_callback("步骤 4/5: 全量路段补全与去重...")
        self.links_df = self._complete_links(self.links_df, self.nodes_df, log_callback)
        
        # 5. 移除重复 (全量)
        self.links_df = self._remove_duplicate_links(self.links_df, log_callback)

        # 重新计算 Node 的出入度 (因为路段补全和去重改变了拓扑结构)
        log_callback("更新节点拓扑属性...")
        self._update_node_topology(log_callback)

        # 重新计算区块统计信息 (在补全和去重之后)
        log_callback("更新区块统计信息...")
        self._update_block_stats(log_callback)
        
        # 备份原始处理结果 (含 Block ID, 补全, 去重)
        self.source_links_df = self.links_df.copy()
        self.source_nodes_df = self.nodes_df.copy()
        
        # 6. 生成 GeoDataFrame
        log_callback("步骤 5/5: 生成全量 GeoDataFrame...")
        try:
            # 处理 Link Geometry
            if 'geometry' in self.links_df.columns:
                self.links_df['geometry'] = self.links_df['geometry'].apply(lambda x: wkt.loads(x) if isinstance(x, str) else x)
                self.processed_links_gdf = gpd.GeoDataFrame(self.links_df, geometry='geometry')
                if self.processed_links_gdf.crs is None:
                    self.processed_links_gdf.set_crs(epsg=4326, inplace=True)
            
            # 处理 Node Geometry
            if 'geometry' in self.nodes_df.columns:
                self.nodes_df['geometry'] = self.nodes_df['geometry'].apply(lambda x: wkt.loads(x) if isinstance(x, str) else x)
                self.processed_nodes_gdf = gpd.GeoDataFrame(self.nodes_df, geometry='geometry')
                if self.processed_nodes_gdf.crs is None:
                    self.processed_nodes_gdf.set_crs(epsg=4326, inplace=True)
            
            log_callback("全量数据处理完成 (已缓存，请前往筛选页导出)！")
            return True

        except Exception as e:
            log_callback(f"处理失败: {e}")
            import traceback
            log_callback(traceback.format_exc())
            return False

    def _export_full_processed_data(self, output_dir, log_callback=print):
        # Deprecated: Export logic moved to export_preview_data
        pass

    # --- Stage 2: Filtering (Preview Generation) ---
    def generate_preview_data(self, filter_criteria, log_callback=print):
        """
        根据筛选条件 (区块ID列表) 从全量 processed 数据中筛选出预览数据。
        """
        log_callback("正在生成预览数据 (基于全量处理结果筛选)...")
        
        if self.processed_links_gdf is None or self.processed_nodes_gdf is None:
             raise ValueError("请先运行全流程处理 (步骤1)。")

        try:
            # 筛选 Links
            log_callback(f"筛选区块: {filter_criteria}")
            self.preview_links_gdf = self.processed_links_gdf[self.processed_links_gdf['区块ID'].isin(filter_criteria)].copy()
            
            # 筛选 Nodes
            self.preview_nodes_gdf = self.processed_nodes_gdf[self.processed_nodes_gdf['区块ID'].isin(filter_criteria)].copy()
            
            log_callback(f"筛选完成: {len(self.preview_links_gdf)} 路段, {len(self.preview_nodes_gdf)} 节点")
            return True
        except Exception as e:
            log_callback(f"筛选失败: {e}")
            import traceback
            log_callback(traceback.format_exc())
            return False

    def export_preview_data(self, output_dir, encoding='gbk', target_crs=None, log_callback=print):
        """
        导出当前筛选后的预览数据
        target_crs: 目标坐标系 (如 'EPSG:32650', '32650', 或 'Auto')
        """
        if self.preview_links_gdf is None or self.preview_nodes_gdf is None:
            raise ValueError("没有可导出的预览数据，请先生成预览。")
            
        log_callback(f"开始导出筛选后的数据 (编码: {encoding})...")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # 准备导出的 GDF (可能需要投影)
        export_links_gdf = self.preview_links_gdf.copy()
        export_nodes_gdf = self.preview_nodes_gdf.copy()
        
        if target_crs:
            try:
                final_crs = None
                
                # 1. 自动计算 UTM (Auto)
                if str(target_crs).lower() == 'auto':
                    log_callback("正在自动计算 UTM 投影...")
                    # 获取中心点坐标 (WGS84)
                    # 确保当前是 4326
                    if export_links_gdf.crs != "EPSG:4326":
                         temp_gdf = export_links_gdf.to_crs("EPSG:4326")
                    else:
                         temp_gdf = export_links_gdf
                         
                    centroid = temp_gdf.unary_union.centroid
                    # 使用 utm 库计算 Zone
                    lat, lon = centroid.y, centroid.x
                    _, _, zone_number, zone_letter = utm.from_latlon(lat, lon)
                    
                    # 构造 EPSG 代码
                    # 北半球: 326xx, 南半球: 327xx
                    is_northern = zone_letter >= 'N'
                    base_epsg = 32600 if is_northern else 32700
                    epsg_code = base_epsg + zone_number
                    final_crs = f"EPSG:{epsg_code}"
                    log_callback(f"  计算结果: UTM Zone {zone_number}{zone_letter} -> {final_crs}")
                
                # 2. 指定 EPSG
                else:
                    if str(target_crs).isdigit():
                        final_crs = f"EPSG:{target_crs}"
                    else:
                        final_crs = target_crs

                if final_crs:
                    log_callback(f"正在进行坐标投影转换: {final_crs}...")
                    export_links_gdf = export_links_gdf.to_crs(final_crs)
                    export_nodes_gdf = export_nodes_gdf.to_crs(final_crs)
                    
            except Exception as e:
                log_callback(f"警告: 坐标转换失败 ({e})，将使用原始坐标系导出。")
                import traceback
                log_callback(traceback.format_exc())
        
        # 导出文件名 (筛选版)
        link_excel_path = os.path.join(output_dir, "link_processed.xlsx")
        node_excel_path = os.path.join(output_dir, "node_processed.xlsx")
        link_shp_path = os.path.join(output_dir, "link_processed.shp")
        node_shp_path = os.path.join(output_dir, "node_processed.shp")
        
        # 导出 Link
        log_callback(f"正在写入: {os.path.basename(link_excel_path)}")
        links_df_export = pd.DataFrame(export_links_gdf.drop(columns='geometry'))
        links_df_export['geometry'] = export_links_gdf['geometry'].apply(lambda x: x.wkt)
        links_df_export.to_excel(link_excel_path, index=False)
        
        log_callback(f"正在生成: {os.path.basename(link_shp_path)}")
        export_links_gdf.to_file(link_shp_path, encoding=encoding)
        
        # 导出 Node
        log_callback(f"正在写入: {os.path.basename(node_excel_path)}")
        nodes_df_export = pd.DataFrame(export_nodes_gdf.drop(columns='geometry'))
        nodes_df_export['geometry'] = export_nodes_gdf['geometry'].apply(lambda x: x.wkt)
        nodes_df_export.to_excel(node_excel_path, index=False)
        
        # 导出 Node SHP (之前遗漏的部分)
        log_callback(f"正在生成: {os.path.basename(node_shp_path)}")
        export_nodes_gdf.to_file(node_shp_path, encoding=encoding)
        
        log_callback(f"文件已保存至: {output_dir}")
        return output_dir

    # 兼容旧接口
    def run_preprocessing(self, *args, **kwargs):
        pass

    def _read_mapping_files(self, node_map_path, link_map_path, attr_map_path, log_callback=print):
        log_callback("  读取中...")
        # Link 表头映射
        link_df = pd.read_excel(link_map_path)
        # Node 表头映射
        node_df = pd.read_excel(node_map_path)
        # 属性映射
        attr_df = pd.read_excel(attr_map_path)

        self.update_mappings(node_df, link_df, attr_df)

    def _get_column_name(self, attribute, map_type='link', target='chn'):
        """
        统一的列名获取函数 (对应 Notebook 中的 getRoadChnTit 等)。
        
        Args:
            attribute (str): 属性名称 (如 '道路等级', '机动车道数')
            map_type (str): 'link' 或 'node'
            target (str): 'chn' (中文表头) 或 'osm' (OSM表头)
        
        Returns:
            str: 对应的表头名称。如果找不到且 target='chn'，返回 attribute 本身。
        """
        # 1. 获取对应的索引表
        index_key = f'{map_type}_attr_index'
        mapping_df = self.mapping_files.get(index_key)
        
        if mapping_df is None:
            return attribute if target == 'chn' else None
            
        # 2. 查找
        if attribute in mapping_df.index:
            col_name = '中文表头' if target == 'chn' else 'OSM表头'
            val = mapping_df.loc[attribute, col_name]
            
            # 处理 Series (如果有重复索引) - 取第一个
            if isinstance(val, pd.Series):
                val = val.iloc[0]
                
            if pd.isna(val) or val == '':
                return attribute if target == 'chn' else None
            return val
            
        # 3. Fallback
        return attribute if target == 'chn' else None


    # 兼容旧代码的 helper
    def _get_chn_title(self, name, map_type='link'):
        return self._get_column_name(name, map_type, 'chn')
        
    def _get_osm_title(self, name, map_type='link'):
        return self._get_column_name(name, map_type, 'osm')

    def _process_link_headers(self, log_callback=print):
        log_callback("  替换Link表头...")
        link_map_index = self.mapping_files.get('link_osm_index')
        if link_map_index is None:
            return

        original_links = self.links_df.copy()

        # 1. 保留有用的列 (OSM表头存在于映射文件中)
        # 筛选条件: OSM表头不为空 & 中文表头不为空
        # 使用 boolean indexing 直接筛选 DataFrame 行
        valid_rows = link_map_index[link_map_index.index.notnull() & link_map_index['中文表头'].notnull()]
        valid_osm_cols = valid_rows.index.tolist()
        
        # 只保留存在于原始数据中的列
        existing_cols = [col for col in valid_osm_cols if col in self.links_df.columns]
        
        self.links_df = self.links_df[[col for col in existing_cols]]

        # 2. 替换为中文表头
        # map() 可能会有性能问题如果 index 不唯一，但这里应该是唯一的
        new_columns = link_map_index.loc[self.links_df.columns, '中文表头'].tolist()
        self.links_df.columns = new_columns

        # 3. 补充属性 (OSM映射文件中，OSM表头为空的行，对应的中文表头)
        # 这些通常是后续计算需要的空白列或固定值列
        missing_osm_rows = link_map_index[link_map_index.index.isnull()]
        if not missing_osm_rows.empty:
            missing_chn_cols = missing_osm_rows['中文表头'].dropna().tolist()
            for col in missing_chn_cols:
                self.links_df[col] = 1.0 # 按照 Notebook 逻辑填充 1.0

        # 4. 添加后续用于进行判断但不在中文表头中的属性 (如 is_link -> 渠道)
        # Notebook: RoadTSTitCsv.loc[:, 'is_link'] = roadOSMCSV['is_link']
        # 我们需要找到 '渠道' 对应的 中文列名，以及 '渠道' 对应的 原始OSM表头
        channel_chn_col = self._get_column_name('渠道', 'link', 'chn')
        channel_osm_col = self._get_column_name('渠道', 'link', 'osm')
        
        # 如果获取到了原始数据的 OSM 表头名
        if channel_osm_col and channel_osm_col in original_links.columns and channel_chn_col:
             self.links_df[channel_chn_col] = original_links[channel_osm_col]

    def _process_link_attributes(self, log_callback=print):
        log_callback("  更新/填充Link属性...")
        attr_map = self.mapping_files.get('attr_map')
        if attr_map is None:
            return

        # 获取各属性对应的中文列名
        road_level_col = self._get_column_name('道路等级', 'link', 'chn')
        channel_col = self._get_column_name('渠道', 'link', 'chn')
        
        # 属性列 (目标列)
        width_col = self._get_column_name('机动车道宽度', 'link', 'chn')
        sep_col = self._get_column_name('机非分隔', 'link', 'chn')
        nm_width_col = self._get_column_name('非机动车道宽度', 'link', 'chn')
        lanes_col = self._get_column_name('机动车道数', 'link', 'chn')
        name_col = self._get_column_name('道路名称', 'link', 'chn')
        from_col = self._get_column_name('起点', 'link', 'chn')
        to_col = self._get_column_name('终点', 'link', 'chn')

        # 1. 筛选有效道路等级
        # 假设 attr_map 的 index level 0 是道路等级
        if road_level_col in self.links_df.columns:
            valid_levels = attr_map.index.levels[0].tolist()
            # 确保类型一致 (通常是 str)
            self.links_df = self.links_df[self.links_df[road_level_col].isin(valid_levels)]
        else:
            log_callback(f"警告: 找不到 '{road_level_col}' 列，无法筛选道路等级。")
            return

        # 2. 准备索引 (道路等级, 渠道)
        # 确保 channel_col 存在
        if channel_col not in self.links_df.columns:
             log_callback("警告: 找不到渠道/is_link列，默认设为0")
             self.links_df[channel_col] = 0
        
        # 3. 批量更新属性 (基于 Notebook 逻辑)
        try:
            # 获取属性映射表的列名
            attr_cols = attr_map.columns.tolist()
            
            # 构造索引元组
            # 确保类型匹配：道路等级(str), 渠道(int)
            self.links_df[road_level_col] = self.links_df[road_level_col].astype(str)
            self.links_df[channel_col] = self.links_df[channel_col].astype(int)
            
            # 使用列表推导式构造 (level, channel) 元组
            road_index = self.links_df[[road_level_col, channel_col]].values.tolist()
            
            # 批量获取属性值并更新
            # 注意：如果 road_index 中的某个元组不在 attr_map.index 中，loc 会报错
            # 我们需要先过滤或使用 reindex
            
            # 为了严谨且简单，我们先获取 attr_map 中存在的索引
            valid_attr_map = attr_map[~attr_map.index.duplicated(keep='first')]
            # 直接使用 loc 获取 (可能会有缺失，缺失的会返回 NaN)
            # 使用 reindex 是最安全且最接近 Notebook 逻辑的方式
            matched_attrs = valid_attr_map.loc[road_index,:]
            # 更新各列
            if width_col and '机动车道宽度' in attr_cols:
                self.links_df[width_col] = matched_attrs['机动车道宽度'].values

            if sep_col and '机非分隔' in attr_cols:
                self.links_df[sep_col] = matched_attrs['机非分隔'].values
                
            if nm_width_col and '非机动车道宽度' in attr_cols:
                self.links_df[nm_width_col] = matched_attrs['非机动车道宽度'].values
            
            if lanes_col and '机动车道数' in attr_cols:
                self.links_df[lanes_col] = matched_attrs['机动车道数'].values

            if road_level_col and '道路等级Num' in attr_cols:
                # 只有匹配成功的部分才更新等级为数字，否则保留原等级？
                # Notebook 逻辑是直接赋值。
                self.links_df[road_level_col] = matched_attrs['道路等级Num'].values
            
            # 仅保留中文表头不为空的那些属性 (Notebook: RoadTSCsv = RoadTSCsv[roadTitleEO[~roadTitleEO['中文表头'].isnull()]['中文表头']])
            link_map = self.mapping_files.get('link_osm_index')
            if link_map is not None:
                valid_cols = link_map[link_map['中文表头'].notnull()]['中文表头'].unique().tolist()
                self.links_df = self.links_df[[c for c in valid_cols if c in self.links_df.columns]]
        except Exception as e:
            log_callback(f"警告: 更新属性时出错: {e}")
            import traceback
            log_callback(traceback.format_exc())

        # 4. 补全道路名称

        # 4. 补全道路名称
        if name_col in self.links_df.columns:
            name_null_mask = self.links_df[name_col].isnull()
            if name_null_mask.any():
                # 使用 index + '路'
                self.links_df.loc[name_null_mask, name_col] = pd.Series(self.links_df[name_null_mask].index).apply(lambda r: f"{r}路").tolist()

        # 5. 类型转换 (astype int)
        cols_to_int = [from_col, to_col, lanes_col, nm_width_col, sep_col, road_level_col]
        for col in cols_to_int:
            if col and col in self.links_df.columns:
                try:
                    self.links_df[col] = self.links_df[col].fillna(0).astype(int)
                except:
                    pass

        # 6. 按最终顺序排列
        # 从 link_map_raw 中获取所有 中文表头 (非空)
        link_map_raw = self.mapping_files.get('link_osm_index') # 其实用 link_map_raw (osm index drop=False)
        # 但是我们需要的是原始顺序吗？
        # Notebook: RoadTSTitCsv (which has cols from mapping).
        # 我们使用 link_map_index (sorted by OSM header?). No, original order in excel.
        # 最好保持 Excel 中的顺序
        if link_map_raw is not None:
             final_cols = link_map_raw[link_map_raw['中文表头'].notnull()]['中文表头'].unique().tolist()
             self.links_df = self.links_df[[c for c in final_cols if c in self.links_df.columns]]

    def _process_node_headers(self, log_callback=print):
        log_callback("  替换Node表头...")
        node_map_index = self.mapping_files.get('node_osm_index')
        if node_map_index is None:
            return

        # 确保geometry列存在
        # 使用 _get_column_name 查找 '经度' 和 '纬度' 对应的 OSM 表头
        x_col = self._get_column_name('经度', 'node', 'osm')
        y_col = self._get_column_name('纬度', 'node', 'osm')
        
        # 如果找不到映射，尝试使用默认值 (x_coord, y_coord 是 osm2gmns 的默认输出)
        if not x_col: x_col = 'x_coord'
        if not y_col: y_col = 'y_coord'

        if 'geometry' not in self.nodes_df.columns and x_col in self.nodes_df.columns and y_col in self.nodes_df.columns:
            self.nodes_df['geometry'] = self.nodes_df.apply(lambda r: f"POINT({r[x_col]} {r[y_col]})", axis=1)

        # 1. 筛选与重命名
        # 筛选 OSM表头 不为空的行
        valid_rows = node_map_index[node_map_index.index.notnull()]
        valid_osm_cols = valid_rows.index.tolist()
        
        # 找到存在的列
        existing_cols = [col for col in valid_osm_cols if col in self.nodes_df.columns]
        
        self.nodes_df = self.nodes_df[existing_cols]
        self.nodes_df.columns = node_map_index.loc[self.nodes_df.columns, '中文表头'].tolist()

        # 2. 填充默认类型
        type_col = self._get_column_name('类型', 'node', 'chn')
        if type_col:
            self.nodes_df[type_col] = 2 

    def _check_blocks(self, log_callback=print):
        log_callback("  使用 networkx 进行连通性分析...")
        from_col = self._get_column_name('起点', 'link', 'chn')
        to_col = self._get_column_name('终点', 'link', 'chn')
        node_id_col = self._get_column_name('编号', 'node', 'chn')

        if not (from_col in self.links_df.columns and to_col in self.links_df.columns):
            log_callback("错误: 找不到起点/终点列，无法检查连通性。")
            return

        G = nx.from_pandas_edgelist(self.links_df, from_col, to_col, create_using=nx.Graph())
        
        # 获取连通分量
        components = list(nx.connected_components(G))
        node_to_block = {node: i for i, comp in enumerate(components) for node in comp}
        
        # 添加区块ID
        self.nodes_df['区块ID'] = self.nodes_df[node_id_col].map(node_to_block)
        self.links_df['区块ID'] = self.links_df[from_col].map(node_to_block)

        # 计算出入度
        G_di = nx.from_pandas_edgelist(self.links_df, from_col, to_col, create_using=nx.DiGraph())
        in_degree = dict(G_di.in_degree())
        out_degree = dict(G_di.out_degree())
        
        self.nodes_df['入度'] = self.nodes_df[node_id_col].map(in_degree).fillna(0).astype(int)
        self.nodes_df['出度'] = self.nodes_df[node_id_col].map(out_degree).fillna(0).astype(int)
        self.nodes_df['是否断头路'] = ((self.nodes_df['入度'] == 0) | (self.nodes_df['出度'] == 0)).astype(int)

        # 统计
        block_link_counts = self.links_df.groupby('区块ID').size().rename('路段数')
        block_node_counts = self.nodes_df.groupby('区块ID').size().rename('节点数')
        stats_df = pd.concat([block_link_counts, block_node_counts], axis=1).reset_index()

        stats_df['路段数'] = stats_df['路段数'].fillna(0).astype(int)
        stats_df['节点数'] = stats_df['节点数'].fillna(0).astype(int)
        
        total_links = len(self.links_df)
        total_nodes = len(self.nodes_df)
        if total_links > 0:
            stats_df['路段占比'] = (stats_df['路段数'] / total_links * 100).apply(lambda x: f"{x:.2f}%")
        else:
            stats_df['路段占比'] = "0.00%"
            
        if total_nodes > 0:
            stats_df['节点占比'] = (stats_df['节点数'] / total_nodes * 100).apply(lambda x: f"{x:.2f}%")
        else:
            stats_df['节点占比'] = "0.00%"

        self.block_stats_df = stats_df[['区块ID', '路段数', '路段占比', '节点数', '节点占比']]

    def _update_node_topology(self, log_callback=print):
        """
        更新 Node 的入度、出度和断头路状态 (在路段补全/去重后调用)。
        """
        from_col = self._get_column_name('起点', 'link', 'chn')
        to_col = self._get_column_name('终点', 'link', 'chn')
        node_id_col = self._get_column_name('编号', 'node', 'chn')
        
        if not (from_col in self.links_df.columns and to_col in self.links_df.columns):
            return

        # 重新构建图计算出入度
        G_di = nx.from_pandas_edgelist(self.links_df, from_col, to_col, create_using=nx.DiGraph())
        in_degree = dict(G_di.in_degree())
        out_degree = dict(G_di.out_degree())
        
        # 更新 Nodes DataFrame
        self.nodes_df['入度'] = self.nodes_df[node_id_col].map(in_degree).fillna(0).astype(int)
        self.nodes_df['出度'] = self.nodes_df[node_id_col].map(out_degree).fillna(0).astype(int)
        self.nodes_df['是否断头路'] = ((self.nodes_df['入度'] == 0) | (self.nodes_df['出度'] == 0)).astype(int)

    def _update_block_stats(self, log_callback=print):
        """
        更新区块统计信息 (在补全和去重后调用)
        """
        # 统计
        if '区块ID' not in self.links_df.columns or '区块ID' not in self.nodes_df.columns:
            return

        block_link_counts = self.links_df.groupby('区块ID').size().rename('路段数')
        block_node_counts = self.nodes_df.groupby('区块ID').size().rename('节点数')
        stats_df = pd.concat([block_link_counts, block_node_counts], axis=1).reset_index()

        stats_df['路段数'] = stats_df['路段数'].fillna(0).astype(int)
        stats_df['节点数'] = stats_df['节点数'].fillna(0).astype(int)
        
        total_links = len(self.links_df)
        total_nodes = len(self.nodes_df)
        if total_links > 0:
            stats_df['路段占比'] = (stats_df['路段数'] / total_links * 100).apply(lambda x: f"{x:.2f}%")
        else:
            stats_df['路段占比'] = "0.00%"
            
        if total_nodes > 0:
            stats_df['节点占比'] = (stats_df['节点数'] / total_nodes * 100).apply(lambda x: f"{x:.2f}%")
        else:
            stats_df['节点占比'] = "0.00%"

        self.block_stats_df = stats_df[['区块ID', '路段数', '路段占比', '节点数', '节点占比']]

    def _complete_links(self, links_df, nodes_df, log_callback=print):
        log_callback("  补全反向路段...")
        node_id_col = self._get_column_name('编号', 'node', 'chn')
        if '是否断头路' not in nodes_df.columns:
            return links_df
            
        dead_end_nodes = nodes_df[nodes_df['是否断头路'] == 1][node_id_col]
        if dead_end_nodes.empty:
            return links_df

        from_col = self._get_column_name('起点', 'link', 'chn')
        to_col = self._get_column_name('终点', 'link', 'chn')
        geom_col = self._get_column_name('矢量数据', 'link', 'chn') # 通常是 'geometry'
        if not geom_col or geom_col not in links_df.columns:
            geom_col = 'geometry' # fallback

        # 找到所有与断头路节点相连的路段
        dead_end_links = links_df[
            (links_df[from_col].isin(dead_end_nodes)) |
            (links_df[to_col].isin(dead_end_nodes))
        ].copy()

        if dead_end_links.empty:
            return links_df

        # 创建反向路段
        reversed_links = dead_end_links.copy()
        reversed_links[from_col], reversed_links[to_col] = reversed_links[to_col], reversed_links[from_col]
        
        if geom_col in reversed_links.columns:
            reversed_links[geom_col] = reversed_links[geom_col].apply(self._reverse_geometry_string)
        
        return pd.concat([links_df, reversed_links], ignore_index=True)

    def _remove_duplicate_links(self, links_df, log_callback=print):
        log_callback("  移除重复路段...")
        from_col = self._get_column_name('起点', 'link', 'chn')
        to_col = self._get_column_name('终点', 'link', 'chn')
        return links_df.drop_duplicates(subset=[from_col, to_col], keep='first')

    def _reverse_geometry_string(self, geom):
        """
        核心修复：现在的 geom 已经是真正的 Shapely 几何对象了，不再是字符串。
        我们直接反转其内部的坐标点。
        """
        # 如果是真正的 Shapely LineString 对象，直接反转坐标
        if isinstance(geom, LineString):
            return LineString(list(geom.coords)[::-1])
            
        # 兜底逻辑：如果因为某种原因传入的还是字符串
        if isinstance(geom, str) and geom.upper().startswith('LINESTRING'):
            try:
                coords_str = re.search(r'\((.*)\)', geom).group(1)
                coords = [c.strip().split() for c in coords_str.split(',')]
                reversed_coords_str = ", ".join([f"{c[0]} {c[1]}" for c in coords[::-1]])
                return f"LINESTRING ({reversed_coords_str})"
            except Exception:
                return geom
                
        return geom

# ==============================================================================
# 文件导出辅助函数 (Export Helper Functions)
# ==============================================================================

# ==============================================================================
# 文件导出辅助函数 (Export Helper Functions)
# ==============================================================================

# ==============================================================================
# 文件导出辅助函数 (Export Helper Functions)
# ==============================================================================

def _apply_crs(gdf, target_crs, log_callback):
    """辅助函数：处理坐标系投影"""
    if not target_crs:
        return gdf
    try:
        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)
            
        final_crs = None
        if str(target_crs).lower() == 'auto':
            # 自动计算 UTM
            temp_gdf = gdf.to_crs("EPSG:4326") if gdf.crs != "EPSG:4326" else gdf
            centroid = temp_gdf.unary_union.centroid
            lat, lon = centroid.y, centroid.x
            _, _, zone_number, zone_letter = utm.from_latlon(lat, lon)
            is_northern = zone_letter >= 'N'
            base_epsg = 32600 if is_northern else 32700
            final_crs = f"EPSG:{base_epsg + zone_number}"
            log_callback(f"    自动推断 UTM 投影: {final_crs}")
        elif str(target_crs).isdigit():
            final_crs = f"EPSG:{target_crs}"
        else:
            final_crs = target_crs

        if final_crs:
            log_callback(f"    执行投影转换 -> {final_crs}")
            return gdf.to_crs(final_crs)
            
    except Exception as e:
        log_callback(f"    警告: 坐标转换失败 ({e})，将使用原始坐标系导出。")
    return gdf

def export_results(links_df, nodes_df, output_dir, is_raw=False, encoding='gbk', target_crs=None, log_callback=print):
    """将最终的DataFrame导出为Excel和Shapefile。"""
    log_callback(f"  开始导出文件 (编码: {encoding})...")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    suffix = "raw" if is_raw else "processed"
    link_excel_path = os.path.join(output_dir, f"link_{suffix}.xlsx")
    node_excel_path = os.path.join(output_dir, f"node_{suffix}.xlsx")
    link_shp_path = os.path.join(output_dir, f"link_{suffix}.shp")
    node_shp_path = os.path.join(output_dir, f"node_{suffix}.shp")
    
    # 导出 Excel（剥离 geometry 转换成 WKT 以免 Excel 报错）
    log_callback(f"    正在写入: {os.path.basename(link_excel_path)}")
    links_export = links_df.copy()
    if 'geometry' in links_export.columns:
        links_export['geometry'] = links_export['geometry'].apply(lambda x: x.wkt if hasattr(x, 'wkt') else str(x))
    links_export.to_excel(link_excel_path, index=False)
    
    log_callback(f"    正在写入: {os.path.basename(node_excel_path)}")
    nodes_export = nodes_df.copy()
    if 'geometry' in nodes_export.columns:
        nodes_export['geometry'] = nodes_export['geometry'].apply(lambda x: x.wkt if hasattr(x, 'wkt') else str(x))
    nodes_export.to_excel(node_excel_path, index=False)
    
    # 导出 SHP (使用统一的精简函数)
    _create_shp(links_df.copy(), link_shp_path, encoding, target_crs, "Link", log_callback)
    _create_shp(nodes_df.copy(), node_shp_path, encoding, target_crs, "Node", log_callback)
    
    log_callback(f"  文件已保存至: {output_dir}")
    return output_dir

def _create_shp(df, path, encoding, target_crs, name, log_callback):
    """高度复用的 Shapefile 生成器"""
    log_callback(f"    正在生成: {os.path.basename(path)}")
    if 'geometry' not in df.columns:
        log_callback(f"    警告: {name} DataFrame中无 geometry 列，跳过SHP生成。")
        return
        
    try:
        # 万一有残留的字符串，保底转换，然后踢掉空值
        def safe_wkt_load(x):
            if isinstance(x, str):
                try: return wkt.loads(x)
                except: return None
            return x
            
        df['geometry'] = df['geometry'].apply(safe_wkt_load)
        valid_df = df.dropna(subset=['geometry']).copy()
        
        if valid_df.empty:
            log_callback(f"    警告: {name} 没有有效的几何数据，跳过SHP生成。")
            return
            
        # 生成 GeoDataFrame 并应用坐标系
        gdf = gpd.GeoDataFrame(valid_df, geometry='geometry')
        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)
            
        gdf = _apply_crs(gdf, target_crs, log_callback)
        gdf.to_file(path, encoding=encoding)
    except Exception as e:
        log_callback(f"    错误: 生成 {name} SHP失败 - {e}")