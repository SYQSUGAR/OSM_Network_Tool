import sys
import os
import json
import pandas as pd
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QLabel, QLineEdit, QPushButton, 
                             QTextEdit, QFileDialog, QProgressBar, QMessageBox, 
                             QGroupBox, QFrame, QRadioButton, QButtonGroup, QTabWidget, QStackedWidget, QCheckBox,
                             QTableView, QHeaderView, QSplitter, QComboBox, QColorDialog, QTableWidget, QTableWidgetItem)
from PyQt6.QtGui import QStandardItemModel, QStandardItem, QColor
from PyQt6.QtCore import QThread, pyqtSignal, Qt
from shapely import wkt
import geopandas as gpd

try:
    from PyQt6.QtWebEngineWidgets import QWebEngineView
    WEB_ENGINE_AVAILABLE = True
except ImportError:
    WEB_ENGINE_AVAILABLE = False
    print("Warning: PyQt6-WebEngine not found. Map visualization will be disabled.")

from .downloader import download_from_osmnx, process_from_osm_file, read_from_csv_files
from .processor import DataProcessor, export_results

class WorkerThread(QThread):
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(bool, object)

    def __init__(self, task_type, **kwargs):
        super().__init__()
        self.task_type = task_type
        self.kwargs = kwargs

    def run(self):
        try:
            self.log_signal.emit(f"--- 任务开始 [{self.task_type}] ---")
            
            # ===================
            #  步骤 1: 数据获取 (如果需要)
            # ===================
            nodes_df, links_df = None, None
            # 这些任务类型需要先获取数据
            if self.task_type in ['preprocess', 'export_raw', 'preview_raw']:
                mode = self.kwargs.get('mode')
                input_val = self.kwargs.get('input_val')
                output_dir = self.kwargs.get('output_dir')
                self.log_signal.emit("步骤 1/2: 正在获取源数据...")
                
                if mode == 'online':
                    nodes_df, links_df = download_from_osmnx(input_val, log_callback=self.log_signal.emit)
                elif mode == 'osm':
                    nodes_df, links_df = process_from_osm_file(input_val, output_dir, log_callback=self.log_signal.emit)
                elif mode == 'csv':
                    nodes_df, links_df = read_from_csv_files(input_val['link'], input_val['node'], log_callback=self.log_signal.emit)

                if nodes_df is None or links_df is None:
                    raise ValueError("数据获取步骤未能返回有效的数据。")

            # ===================
            #  步骤 2: 根据任务类型执行处理
            # ===================
            processor = self.kwargs.get('processor')
            result = None

            if self.task_type == 'preprocess':
                self.log_signal.emit("步骤 2/2: 正在执行数据预处理与全量计算...")
                # Note: processor already has mappings loaded via update_mappings
                processor.run_full_processing(
                    links_df, nodes_df,
                    log_callback=self.log_signal.emit
                )
                self.log_signal.emit(f"--- 处理成功完成！---")
                self.finished_signal.emit(True, "处理成功")

            elif self.task_type == 'export_raw':
                self.log_signal.emit("步骤 2/2: 正在直接导出原始数据...")
                result = export_results(links_df, nodes_df, self.kwargs.get('output_dir'), is_raw=True, 
                                        encoding=self.kwargs.get('encoding', 'gbk'),
                                        log_callback=self.log_signal.emit)
                self.log_signal.emit(f"--- 原始数据导出成功！---")
                self.finished_signal.emit(True, result)

            elif self.task_type == 'preview_raw':
                self.log_signal.emit("步骤 2/2: 正在生成原始数据预览...")
                # 简单转换为 GDF 供预览，不进行映射和连通性分析
                # 为了兼容 processor.preview_links_gdf 的结构，我们需要做最小化的转换
                
                # 尝试转换 geometry
                if 'geometry' in links_df.columns:
                    links_df['geometry'] = links_df['geometry'].apply(lambda x: wkt.loads(x) if isinstance(x, str) else x)
                processor.preview_links_gdf = gpd.GeoDataFrame(links_df, geometry='geometry')
                if processor.preview_links_gdf.crs is None:
                    processor.preview_links_gdf.set_crs(epsg=4326, inplace=True)

                if 'geometry' in nodes_df.columns:
                    nodes_df['geometry'] = nodes_df['geometry'].apply(lambda x: wkt.loads(x) if isinstance(x, str) else x)
                processor.preview_nodes_gdf = gpd.GeoDataFrame(nodes_df, geometry='geometry')
                if processor.preview_nodes_gdf.crs is None:
                    processor.preview_nodes_gdf.set_crs(epsg=4326, inplace=True)
                
                self.log_signal.emit(f"--- 原始数据预览准备就绪！---")
                self.finished_signal.emit(True, "preview_ready")
            
            elif self.task_type == 'filter_and_export':
                self.log_signal.emit("正在执行筛选和导出...")
                # 重新生成 preview data (基于筛选)
                processor.generate_preview_data(self.kwargs.get('filter_criteria'), log_callback=self.log_signal.emit)
                result = processor.export_preview_data(self.kwargs.get('output_dir'), 
                                                      encoding=self.kwargs.get('encoding', 'gbk'),
                                                      log_callback=self.log_signal.emit)
                self.log_signal.emit(f"--- 筛选和导出成功！---")
                self.finished_signal.emit(True, result)

            elif self.task_type == 'preview_processed':
                self.log_signal.emit("正在生成筛选后的预览数据...")
                processor.generate_preview_data(self.kwargs.get('filter_criteria'), log_callback=self.log_signal.emit)
                self.log_signal.emit(f"--- 预览数据生成完毕！---")
                self.finished_signal.emit(True, "preview_ready")

            elif self.task_type == 'export_preview_current':
                self.log_signal.emit("正在导出当前预览数据...")
                result = processor.export_preview_data(self.kwargs.get('output_dir'), 
                                                      encoding=self.kwargs.get('encoding', 'gbk'),
                                                      log_callback=self.log_signal.emit)
                self.log_signal.emit(f"--- 导出成功！---")
                self.finished_signal.emit(True, result)

        except Exception as e:
            self.log_signal.emit(f"错误: {str(e)}")
            import traceback
            self.log_signal.emit(traceback.format_exc())
            self.finished_signal.emit(False, str(e))

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("OSM路网工具")
        self.resize(1200, 800)
        
        self.last_osm_dir = "."
        self.last_config_dir = "."
        self.settings_file = "settings.json"
        
        # Paths Setup
        # Determine Application Root
        if getattr(sys, 'frozen', False):
            # If run as exe (PyInstaller)
            self.app_root = os.path.dirname(sys.executable)
            # If using --onedir, resources might be next to exe. 
            # If using --onefile, defaults might be in sys._MEIPASS.
            # Here we assume a simple structure where resources are external or copied to _MEIPASS
            # For simplicity in this project context, we assume resources are placed next to the exe
            self.base_path = self.app_root
        else:
            # If run from source
            self.app_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            self.base_path = self.app_root

        # 1. User Configs (Always external, writable) -> config/
        self.config_dir = os.path.join(self.app_root, "config")
        if not os.path.exists(self.config_dir): os.makedirs(self.config_dir)

        # 2. Default Resources (Read-only templates) -> resources/
        # Try finding resources in sys._MEIPASS first (if bundled), then local
        if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
             self.resources_dir = os.path.join(sys._MEIPASS, "resources")
        else:
             self.resources_dir = os.path.join(self.base_path, "resources")
        
        # Fallback: if not found in bundle, check external folder
        if not os.path.exists(self.resources_dir):
            self.resources_dir = os.path.join(self.app_root, "resources")

        if not os.path.exists(self.resources_dir): 
            # Attempt to create it if totally missing (shouldn't happen if deployed correctly)
            try: os.makedirs(self.resources_dir) 
            except: pass

        # Mappings file paths
        self.DEFAULT_NODE_MAP = os.path.join(self.resources_dir, "节点表头映射关系.xlsx")
        self.DEFAULT_LINK_MAP = os.path.join(self.resources_dir, "路网表头映射关系.xlsx")
        self.DEFAULT_ATTR_MAP = os.path.join(self.resources_dir, "路网字段属性映射关系.xlsx")
        
        # User modified paths (stored in config folder)
        self.USER_NODE_MAP = os.path.join(self.config_dir, "user_node_header_mapping.xlsx")
        self.USER_LINK_MAP = os.path.join(self.config_dir, "user_link_header_mapping.xlsx")
        self.USER_ATTR_MAP = os.path.join(self.config_dir, "user_attr_mapping.xlsx")

        # Table Widgets References
        self.node_table = None
        self.link_table = None
        self.attr_table = None

        # 用于动态指向当前应输出日志的 QTextEdit
        self.current_log_widget = None
        self.current_progress_bar = None

        # 持有核心处理器实例
        self.processor = DataProcessor()
        
        # 可视化相关变量
        self.viz_link_color = "#3388ff" 
        self.viz_node_color = "#ff3333" 
        self.last_viz_splitter_sizes = [600, 400] # 存储属性表隐藏前的比例
        
        self.init_style()
        
        # Central Widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(10, 10, 10, 10)
        
        # Header Title (Removed)
        # title_label = QLabel("OSM路网数据处理工具")
        # title_label.setObjectName("TitleLabel")
        # title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        # main_layout.addWidget(title_label)
        
        # Main Tab Widget
        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)
        
        # 初始化两个主标签页
        self.init_main_interface()
        self.init_settings_interface()
        
        # Load settings (UI state)
        self.load_settings()
        
        # Load Mappings (Data state)
        self.load_mappings_on_startup()
        
        # Defaults
        self.init_defaults()

    def init_style(self):
        self.setStyleSheet("""
            QMainWindow { background-color: #f5f6fa; }
            QLabel#TitleLabel { font-size: 24px; font-weight: bold; color: #2c3e50; margin-bottom: 5px; }
            QLabel { font-size: 13px; color: #2f3640; }
            QLineEdit { padding: 6px; border: 1px solid #dcdde1; border-radius: 4px; background-color: white; }
            QPushButton { padding: 6px 12px; border-radius: 4px; background-color: #ecf0f1; border: 1px solid #bdc3c7; }
            QPushButton:hover { background-color: #bdc3c7; }
            
            QPushButton#PrimaryBtn { background-color: #3498db; color: white; border: none; font-weight: bold; }
            QPushButton#PrimaryBtn:hover { background-color: #2980b9; }
            QPushButton#PrimaryBtn:disabled { background-color: #95a5a6; }
            
            QPushButton#SuccessBtn { background-color: #2ecc71; color: white; border: none; font-weight: bold; }
            QPushButton#SuccessBtn:hover { background-color: #27ae60; }
            QPushButton#SuccessBtn:disabled { background-color: #95a5a6; }

            QGroupBox { font-weight: bold; border: 1px solid #dcdde1; border-radius: 6px; margin-top: 6px; padding-top: 5px; }
            QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 5px; }
            
            QTableView, QTableWidget { border: 1px solid #dcdde1; gridline-color: #ecf0f1; }
            QHeaderView::section { background-color: #ecf0f1; padding: 4px; border: none; border-right: 1px solid #bdc3c7; font-weight: bold; }
            QTabWidget::pane { border: 1px solid #bdc3c7; background: white; }
            QTabBar::tab { background: #ecf0f1; padding: 8px 20px; border-top-left-radius: 4px; border-top-right-radius: 4px; margin-right: 2px; }
            QTabBar::tab:selected { background: white; border-bottom: 2px solid #3498db; color: #3498db; font-weight: bold; }
            
            /* Disabled Style */
            QLabel:disabled { color: #95a5a6; }
            QGroupBox:disabled { color: #95a5a6; border-color: #bdc3c7; }
        """)

    # =============================================================================================
    #  Tab 1: Main Interface (Process, Filter, Visualize)
    # =============================================================================================
    def init_main_interface(self):
        main_tab = QWidget()
        layout = QHBoxLayout(main_tab)
        
        # --- Left Column (Acquisition + Filter) ---
        left_column = QWidget()
        left_layout = QVBoxLayout(left_column)
        left_layout.setContentsMargins(0, 0, 0, 0)
        
        # 1. Data Acquisition & Config Area
        acq_group = QGroupBox("1. 数据获取与配置")
        acq_layout = QVBoxLayout(acq_group)
        acq_layout.setContentsMargins(10, 5, 10, 10)
        acq_layout.setSpacing(5)

        
        # Mode Selection
        mode_layout = QHBoxLayout()
        self.mode_bg = QButtonGroup(self)
        self.radio_online = QRadioButton("联网下载")
        self.radio_osm = QRadioButton("OSM文件")
        self.radio_csv = QRadioButton("CSV文件")
        self.radio_online.setChecked(True)
        self.mode_bg.addButton(self.radio_online, 1)
        self.mode_bg.addButton(self.radio_osm, 2)
        self.mode_bg.addButton(self.radio_csv, 3)
        self.mode_bg.idClicked.connect(self.on_mode_changed)
        mode_layout.addWidget(self.radio_online)
        mode_layout.addWidget(self.radio_osm)
        mode_layout.addWidget(self.radio_csv)
        acq_layout.addLayout(mode_layout)

        # Input Stack
        self.input_stack = QStackedWidget()
        
        # Page 1: Online
        p1 = QWidget()
        l1 = QHBoxLayout(p1)
        l1.setContentsMargins(0,0,0,0)
        l1.addWidget(QLabel("城市:"))
        self.city_input = QLineEdit()
        self.city_input.setPlaceholderText("Beijing, China")
        l1.addWidget(self.city_input)
        self.input_stack.addWidget(p1)

        # Page 2: OSM
        p2 = QWidget()
        l2 = QHBoxLayout(p2)
        l2.setContentsMargins(0,0,0,0)
        self.osm_file_input = QLineEdit()
        btn_osm = QPushButton("浏览")
        btn_osm.clicked.connect(lambda: self.browse_file(self.osm_file_input, "OSM Files (*.osm *.pbf)", 'osm'))
        l2.addWidget(QLabel("文件:"))
        l2.addWidget(self.osm_file_input)
        l2.addWidget(btn_osm)
        self.input_stack.addWidget(p2)

        # Page 3: CSV
        p3 = QWidget()
        l3 = QVBoxLayout(p3)
        l3.setContentsMargins(0,0,0,0)
        row_link = QHBoxLayout()
        self.link_file_input = QLineEdit()
        btn_link = QPushButton("Link")
        btn_link.clicked.connect(lambda: self.browse_file(self.link_file_input, "CSV (*.csv)", 'osm'))
        row_link.addWidget(self.link_file_input)
        row_link.addWidget(btn_link)
        
        row_node = QHBoxLayout()
        self.node_file_input = QLineEdit()
        btn_node = QPushButton("Node")
        btn_node.clicked.connect(lambda: self.browse_file(self.node_file_input, "CSV (*.csv)", 'osm'))
        row_node.addWidget(self.node_file_input)
        row_node.addWidget(btn_node)
        l3.addLayout(row_link)
        l3.addLayout(row_node)
        self.input_stack.addWidget(p3)
        
        acq_layout.addWidget(self.input_stack)

        # Common Configs
        grid_config = QHBoxLayout()
        self.out_input = QLineEdit()
        self.out_input.setText(os.path.join(os.getcwd(), "output"))
        btn_out = QPushButton("输出目录")
        btn_out.clicked.connect(self.browse_dir)
        
        self.encoding_combo = QComboBox()
        self.encoding_combo.addItems(["gbk", "utf-8"])
        
        grid_config.addWidget(self.out_input)
        grid_config.addWidget(btn_out)
        grid_config.addWidget(QLabel("编码:"))
        grid_config.addWidget(self.encoding_combo)
        acq_layout.addLayout(grid_config)
        
        # Format Conversion Toggle
        self.format_conversion_checkbox = QCheckBox("启用格式转换 (使用'参数配置'页的映射表)")
        self.format_conversion_checkbox.setChecked(True)
        self.format_conversion_checkbox.toggled.connect(self.on_format_conversion_toggled)
        acq_layout.addWidget(self.format_conversion_checkbox)

        # Preprocess Button
        self.run_btn = QPushButton("开始预处理")
        self.run_btn.setObjectName("PrimaryBtn")
        self.run_btn.clicked.connect(self.start_preprocess)
        acq_layout.addWidget(self.run_btn)
        
        left_layout.addWidget(acq_group)

        # 2. Data Filtering Area
        self.filter_group = QGroupBox("2. 数据筛选与导出")
        filter_layout = QVBoxLayout(self.filter_group)
        filter_layout.setContentsMargins(10, 5, 10, 10)
        filter_layout.setSpacing(5)
        
        # Stats
        self.stats_widget = QWidget()
        stats_l = QHBoxLayout(self.stats_widget)
        stats_l.setContentsMargins(0,0,0,0)
        self.lbl_blocks = QLabel("区块: --")
        self.lbl_links = QLabel("路段: --")
        self.lbl_nodes = QLabel("节点: --")
        stats_l.addWidget(self.lbl_blocks)
        stats_l.addStretch()
        stats_l.addWidget(self.lbl_links)
        stats_l.addStretch()
        stats_l.addWidget(self.lbl_nodes)
        filter_layout.addWidget(self.stats_widget)
        
        # Selection Controls
        sel_ctrl_layout = QHBoxLayout()
        self.btn_select_all = QPushButton("全选")
        self.btn_select_all.clicked.connect(self.select_all_blocks)
        self.btn_deselect_all = QPushButton("全部取消")
        self.btn_deselect_all.clicked.connect(self.deselect_all_blocks)
        sel_ctrl_layout.addWidget(self.btn_select_all)
        sel_ctrl_layout.addWidget(self.btn_deselect_all)
        sel_ctrl_layout.addStretch()
        filter_layout.addLayout(sel_ctrl_layout)
        
        # Table
        self.block_table_view = QTableView()
        self.block_table_model = QStandardItemModel()
        self.block_table_view.setModel(self.block_table_model)
        self.block_table_view.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        filter_layout.addWidget(self.block_table_view)
        
        # Export Button (Always visible/enabled if conversion off?)
        self.export_filtered_btn = QPushButton("导出数据")
        self.export_filtered_btn.setObjectName("SuccessBtn")
        self.export_filtered_btn.clicked.connect(self.start_filter_export)
        filter_layout.addWidget(self.export_filtered_btn)
        
        # Log Area
        self.log_area = QTextEdit()
        self.log_area.setReadOnly(True)
        self.log_area.setMaximumHeight(100)
        filter_layout.addWidget(self.log_area)
        
        # Progress Bar
        self.progress_bar = QProgressBar()
        self.progress_bar.hide()
        filter_layout.addWidget(self.progress_bar)
        
        left_layout.addWidget(self.filter_group)
        
        # Set stretch for left column
        left_layout.setStretch(0, 0) # Acquisition
        left_layout.setStretch(1, 1) # Filter

        # Use Splitter for resizable columns
        self.main_splitter = QSplitter(Qt.Orientation.Horizontal)
        self.main_splitter.addWidget(left_column)
        
        # --- Right Column (Visualization) ---
        self.viz_group = QGroupBox("3. 可视化展示")
        right_layout = QVBoxLayout(self.viz_group)
        
        # Controls
        viz_ctrl = QHBoxLayout()
        self.cb_show_links = QCheckBox("Link"); self.cb_show_links.setChecked(True)
        self.cb_show_nodes = QCheckBox("Node"); self.cb_show_nodes.setChecked(True)
        viz_ctrl.addWidget(self.cb_show_links)
        viz_ctrl.addWidget(self.cb_show_nodes)
        
        viz_ctrl.addWidget(QLabel("Link颜色:"))
        self.combo_link_attr = QComboBox()
        viz_ctrl.addWidget(self.combo_link_attr)
        self.btn_link_color = QPushButton("")
        self.btn_link_color.setFixedSize(20, 20)
        self.btn_link_color.setStyleSheet(f"background-color: {self.viz_link_color}; border: none;")
        self.btn_link_color.clicked.connect(lambda: self.pick_color('link'))
        viz_ctrl.addWidget(self.btn_link_color)

        viz_ctrl.addWidget(QLabel("Node颜色:"))
        self.combo_node_attr = QComboBox()
        viz_ctrl.addWidget(self.combo_node_attr)
        self.btn_node_color = QPushButton("")
        self.btn_node_color.setFixedSize(20, 20)
        self.btn_node_color.setStyleSheet(f"background-color: {self.viz_node_color}; border: none;")
        self.btn_node_color.clicked.connect(lambda: self.pick_color('node'))
        viz_ctrl.addWidget(self.btn_node_color)
        
        viz_ctrl.addStretch()
        
        self.preview_btn = QPushButton("生成预览")
        self.preview_btn.clicked.connect(self.start_preview_generation)
        viz_ctrl.addWidget(self.preview_btn)

        self.btn_update_map = QPushButton("更新地图")
        self.btn_update_map.clicked.connect(self.update_viz_map)
        viz_ctrl.addWidget(self.btn_update_map)
        
        # Show/Hide Attr Table Button (Main Viz Control)
        self.btn_show_attr = QPushButton("属性表")
        self.btn_show_attr.setCheckable(True)
        self.btn_show_attr.clicked.connect(self.toggle_attr_table_visibility)
        viz_ctrl.addWidget(self.btn_show_attr)
        
        right_layout.addLayout(viz_ctrl)
        
        # Map
        self.viz_splitter = QSplitter(Qt.Orientation.Vertical)
        
        if WEB_ENGINE_AVAILABLE:
            self.web_view = QWebEngineView()
            self.web_view.setHtml("<html><body><h3 align='center'>请先生成预览数据</h3></body></html>")
            self.viz_splitter.addWidget(self.web_view)
        else:
            lbl = QLabel("WebEngine not available")
            lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.viz_splitter.addWidget(lbl)
            
        # Attribute Table Widget (Collapsible)
        self.attr_widget = QWidget()
        attr_layout = QVBoxLayout(self.attr_widget)
        attr_layout.setContentsMargins(0, 0, 0, 0)
        
        # Header for Attr Widget
        attr_header = QHBoxLayout()
        # Removed redundant toggle button
        
        self.btn_update_attr = QPushButton("更新数据")
        self.btn_update_attr.clicked.connect(self.update_attribute_tables)
        
        self.btn_maximize_attr = QPushButton("最大化")
        self.btn_maximize_attr.setCheckable(True)
        self.btn_maximize_attr.clicked.connect(self.maximize_attr_table)
        
        attr_header.addWidget(self.btn_update_attr)
        attr_header.addStretch()
        attr_header.addWidget(self.btn_maximize_attr)
        attr_layout.addLayout(attr_header)
        
        # Tabbed Table for Links/Nodes
        self.attr_tabs = QTabWidget()
        self.link_attr_table = QTableWidget()
        self.node_attr_table = QTableWidget()
        self.attr_tabs.addTab(self.link_attr_table, "Link属性")
        self.attr_tabs.addTab(self.node_attr_table, "Node属性")
        attr_layout.addWidget(self.attr_tabs)
        
        self.attr_widget.show() # Container manages visibility now
        
        # Container for bottom part (to handle splitter behavior)
        self.bottom_viz_container = QWidget()
        bottom_layout = QVBoxLayout(self.bottom_viz_container)
        bottom_layout.setContentsMargins(0,0,0,0)
        bottom_layout.addWidget(self.attr_widget)
        
        self.viz_splitter.addWidget(self.bottom_viz_container)
        self.viz_splitter.setCollapsible(0, False) # Map always visible unless covered
        self.viz_splitter.setCollapsible(1, True)
        
        self.bottom_viz_container.hide() # Initial state: Hidden
        
        right_layout.addWidget(self.viz_splitter)
            
        self.main_splitter.addWidget(self.viz_group)
        
        # Set initial sizes (Left bigger than before, e.g., 40% : 60% or 45% : 55%)
        # Previous was 1:2 (33% : 66%). Let's try 450 : 750 (approx 3:5)
        self.main_splitter.setSizes([450, 750])
        
        layout.addWidget(self.main_splitter)

        self.tabs.addTab(main_tab, "数据处理")

    # =============================================================================================
    #  Tab 2: Settings (Mappings)
    # =============================================================================================
    def init_settings_interface(self):
        settings_tab = QWidget()
        layout = QVBoxLayout(settings_tab)
        
        # Row 1: Node Header (Left) | Link Header (Right)
        row1 = QHBoxLayout()
        self.node_table_group, self.node_table = self.create_mapping_group("节点表头映射", "node")
        self.link_table_group, self.link_table = self.create_mapping_group("路网表头映射", "link")
        
        row1.addWidget(self.node_table_group)
        row1.addWidget(self.link_table_group)
        # Increase stretch for row1 tables to show more content (3/5 increase requested)
        # Assuming original balance was somewhat equal, we can set stretch factors.
        # If we want Row 1 to be taller than Row 2 (Attr mapping), we can set stretch.
        # Let's try Row 1 : Row 2 = 3 : 2 (Row 1 gets 60% height)
        # User said "increase height by 3/5", maybe they meant "show 3/5 more rows"?
        # Or relative to the bottom table?
        # Let's interpret as making the top section significantly taller.
        layout.addLayout(row1, 7) 
        
        # Row 2: Link Attributes (Full width)
        self.attr_table_group, self.attr_table = self.create_mapping_group("路网字段属性映射", "attr")
        layout.addWidget(self.attr_table_group, 9)
        
        # Row 3: Buttons
        btn_layout = QHBoxLayout()
        btn_apply = QPushButton("应用配置")
        btn_apply.setObjectName("PrimaryBtn")
        btn_apply.clicked.connect(self.apply_settings)
        
        btn_export_config = QPushButton("导出当前配置")
        btn_export_config.clicked.connect(self.export_current_config)

        btn_layout.addStretch()
        btn_layout.addWidget(btn_export_config)
        btn_layout.addWidget(btn_apply)
        
        layout.addLayout(btn_layout)
        
        self.tabs.addTab(settings_tab, "参数配置")

    def create_mapping_group(self, title, map_type):
        group = QGroupBox(title)
        layout = QVBoxLayout(group)
        
        # Toolbar
        toolbar = QHBoxLayout()
        btn_load = QPushButton("导入文件...")
        btn_restore = QPushButton("恢复默认")
        
        # Table
        table = QTableWidget()
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        
        # Connect buttons
        btn_load.clicked.connect(lambda: self.import_mapping_file(table))
        btn_restore.clicked.connect(lambda: self.restore_defaults(table, map_type))
        
        toolbar.addWidget(btn_load)
        toolbar.addStretch()
        toolbar.addWidget(btn_restore)
        
        layout.addLayout(toolbar)
        layout.addWidget(table)
        
        return group, table

    def load_mappings_on_startup(self):
        """Startup: Load User Config if exists, else load Default and save as User Config."""
        # 1. Node Map
        self._load_or_create_config(self.USER_NODE_MAP, self.DEFAULT_NODE_MAP, self.node_table)
        # 2. Link Map
        self._load_or_create_config(self.USER_LINK_MAP, self.DEFAULT_LINK_MAP, self.link_table)
        # 3. Attr Map
        self._load_or_create_config(self.USER_ATTR_MAP, self.DEFAULT_ATTR_MAP, self.attr_table)
        
        # Initial update to processor
        self.update_processor_mappings()

    def _load_or_create_config(self, user_path, default_path, table):
        # Determine source path
        src_path = user_path
        if not os.path.exists(user_path):
            # Use default file
            if os.path.exists(default_path):
                # Load default and save to user path
                try:
                    df = pd.read_excel(default_path)
                    df.to_excel(user_path, index=False)
                    src_path = user_path
                except Exception as e:
                    print(f"Failed to init user config for {default_path}: {e}")
                    src_path = default_path # Fallback to reading default directly
            else:
                # No file found
                print(f"Warning: Default mapping file {default_path} not found.")
                return

        # Load into table
        self.load_table_data(table, src_path)

    def import_mapping_file(self, table):
        file, _ = QFileDialog.getOpenFileName(self, "导入Excel文件", self.last_config_dir, "Excel Files (*.xlsx *.xls)")
        if file:
            self.last_config_dir = os.path.dirname(file)
            self.load_table_data(table, file)

    def restore_defaults(self, table, map_type):
        reply = QMessageBox.question(self, "确认", "确定要恢复默认设置吗？这将覆盖当前修改。", 
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            path = ""
            if map_type == 'node': path = self.DEFAULT_NODE_MAP
            elif map_type == 'link': path = self.DEFAULT_LINK_MAP
            elif map_type == 'attr': path = self.DEFAULT_ATTR_MAP
            
            if os.path.exists(path):
                self.load_table_data(table, path)
            else:
                QMessageBox.warning(self, "错误", f"找不到默认文件: {path}")

    def load_table_data(self, table, file_path):
        try:
            # Load excel, ignore '备注' column if exists
            df = pd.read_excel(file_path)
            
            # Filter out '备注' or 'comments' columns
            cols = [c for c in df.columns if "备注" not in str(c) and "comment" not in str(c).lower()]
            df = df[cols]
            
            table.setRowCount(df.shape[0])
            table.setColumnCount(df.shape[1])
            table.setHorizontalHeaderLabels(df.columns.astype(str))
            
            for i in range(df.shape[0]):
                for j in range(df.shape[1]):
                    val = df.iat[i, j]
                    item = QTableWidgetItem(str(val) if pd.notnull(val) else "")
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter) # 居中对齐
                    table.setItem(i, j, item)
        except Exception as e:
            QMessageBox.warning(self, "加载失败", f"无法加载表格: {e}")

    def apply_settings(self):
        """Save tables to User Config files and update Processor."""
        try:
            # 1. Save to User Files
            self.save_table_to_file(self.node_table, self.USER_NODE_MAP)
            self.save_table_to_file(self.link_table, self.USER_LINK_MAP)
            self.save_table_to_file(self.attr_table, self.USER_ATTR_MAP)
            
            # 2. Update Processor
            if self.update_processor_mappings():
                QMessageBox.information(self, "成功", "配置已保存并应用！")
            else:
                QMessageBox.warning(self, "警告", "配置保存成功，但在更新处理器时出错。")
                
        except Exception as e:
             QMessageBox.critical(self, "错误", f"应用配置失败: {e}")

    def save_table_to_file(self, table, file_path):
        df = self.get_table_data(table)
        df.to_excel(file_path, index=False)

    def get_table_data(self, table):
        rows = table.rowCount()
        cols = table.columnCount()
        headers = [table.horizontalHeaderItem(j).text() for j in range(cols)]
        data = []
        for i in range(rows):
            row_data = []
            for j in range(cols):
                item = table.item(i, j)
                row_data.append(item.text() if item else "")
            data.append(row_data)
        return pd.DataFrame(data, columns=headers)

    def update_processor_mappings(self):
        node_df = self.get_table_data(self.node_table)
        link_df = self.get_table_data(self.link_table)
        attr_df = self.get_table_data(self.attr_table)
        
        return self.processor.update_mappings(node_df, link_df, attr_df)

    def save_settings_tables(self):
        # Deprecated by apply_settings, but kept for compatibility if called elsewhere
        # We can just log or pass
        pass

    def save_table_data(self, table, file_path):
        # Deprecated
        pass

    # =============================================================================================
    #  Logic & Events
    # =============================================================================================
    
    def on_format_conversion_toggled(self, checked):
        # 1. Start Preprocessing Button
        self.run_btn.setEnabled(checked)
        
        # 2. Filtering Table & Stats (Disabled when unchecked)
        self.stats_widget.setEnabled(checked)
        self.block_table_view.setEnabled(checked)
        self.btn_select_all.setEnabled(checked)
        self.btn_deselect_all.setEnabled(checked)
        
        # 3. Visualization (Disabled when unchecked)
        self.viz_group.setEnabled(checked)
        
        # 4. Log Area (Always Enabled)
        self.log_area.setEnabled(True)
        
        # 5. Export Button (Always Enabled)
        self.export_filtered_btn.setEnabled(True)

    def start_preprocess(self):
        # 只有在启用格式转换时才能点击此按钮
        self.start_worker_task('preprocess')

    def start_filter_export(self):
        # 检查是否启用格式转换
        if self.format_conversion_checkbox.isChecked():
            # 正常筛选导出
            # 1. 获取用户选择的区块
            selected_block_ids = []
            for i in range(self.block_table_model.rowCount()):
                index = self.block_table_model.index(i, 0)
                container = self.block_table_view.indexWidget(index)
                if container:
                    cb = container.layout().itemAt(0).widget()
                    if cb.isChecked():
                        selected_block_ids.append(int(self.block_table_model.item(i, 1).text()))
            
            # 2. 检查逻辑
            if not selected_block_ids:
                QMessageBox.warning(self, "提示", "必须至少选择一个区块。")
                return

            stats_df = self.processor.block_stats_df
            if stats_df is not None and not stats_df.empty:
                # 找到路段数最多的区块ID
                # 假设 '路段数' 列是 int
                max_links_idx = stats_df['路段数'].idxmax()
                max_block_id = stats_df.loc[max_links_idx, '区块ID']
                
                if len(selected_block_ids) == 1:
                    sel_id = selected_block_ids[0]
                    if sel_id != max_block_id:
                        reply = QMessageBox.question(
                            self, "提示", 
                            f"您选择的区块 (ID: {sel_id}) 不是最大的路网区块 (最大ID: {max_block_id})。\n是否继续导出？",
                            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                            QMessageBox.StandardButton.No
                        )
                        if reply == QMessageBox.StandardButton.No:
                            return
                else:
                    # 选择了多个
                    reply = QMessageBox.question(
                        self, "提示",
                        "您选择了多个区块，导出的路网可能不连续。\n是否继续导出？",
                        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                        QMessageBox.StandardButton.No
                    )
                    if reply == QMessageBox.StandardButton.No:
                        return

            self.start_worker_task('filter_and_export')
        else:
            # 原始导出
            self.start_worker_task('export_raw')

    def start_preview_generation(self):
        if self.format_conversion_checkbox.isChecked():
            # 正常生成预览 (基于筛选)
            self.start_worker_task('preview_processed')
        else:
            # 原始数据预览
            self.start_worker_task('preview_raw')

    def start_worker_task(self, task_type):
        # 收集通用参数
        mode_id = self.mode_bg.checkedId()
        modes = {1: 'online', 2: 'osm', 3: 'csv'}
        mode = modes.get(mode_id)
        
        input_val = None
        if mode == 'online':
            input_val = self.city_input.text().strip()
            # 验证输入: 对于需要获取数据的任务，输入不能为空
            if not input_val and task_type in ['preprocess', 'export_raw', 'preview_raw']:
                QMessageBox.warning(self, "输入错误", "请输入城市名称。")
                return

        elif mode == 'osm':
            input_val = self.osm_file_input.text().strip()
            if not input_val and task_type in ['preprocess', 'export_raw', 'preview_raw']:
                QMessageBox.warning(self, "输入错误", "请选择OSM文件。")
                return

        elif mode == 'csv':
            link_f = self.link_file_input.text().strip()
            node_f = self.node_file_input.text().strip()
            if (not link_f or not node_f) and task_type in ['preprocess', 'export_raw', 'preview_raw']:
                QMessageBox.warning(self, "输入错误", "请选择CSV文件。")
                return
            input_val = {"link": link_f, "node": node_f}

        output_dir = self.out_input.text().strip()
        encoding = self.encoding_combo.currentText()
        
        # 收集筛选条件
        selected_block_ids = []
        if task_type in ['filter_and_export', 'preview_processed']:
            # Get selected blocks
            for i in range(self.block_table_model.rowCount()):
                index = self.block_table_model.index(i, 0)
                container = self.block_table_view.indexWidget(index)
                if container:
                    cb = container.layout().itemAt(0).widget()
                    if cb.isChecked():
                        selected_block_ids.append(int(self.block_table_model.item(i, 1).text()))
            
            # Validation for processed tasks
            if not self.processor.processed_links_gdf is None and not selected_block_ids and task_type == 'filter_and_export':
                 # Validation already done in start_filter_export, but kept as safety
                 QMessageBox.warning(self, "提示", "请选择至少一个区块。")
                 return

        # Prepare kwargs
        task_kwargs = {
            'processor': self.processor,
            'mode': mode,
            'input_val': input_val,
            'output_dir': output_dir,
            'encoding': encoding,
            'filter_criteria': selected_block_ids
        }
        
        # UI State Update
        self.set_ui_busy(True)
        self.log_area.clear()
        self.current_log_widget = self.log_area # Shared log area
        
        self.worker = WorkerThread(task_type, **task_kwargs)
        self.worker.log_signal.connect(self.log)
        self.worker.finished_signal.connect(self.on_task_finished)
        self.worker.start()

    def set_ui_busy(self, busy):
        self.run_btn.setEnabled(not busy and self.format_conversion_checkbox.isChecked())
        self.export_filtered_btn.setEnabled(not busy)
        self.preview_btn.setEnabled(not busy)
        if busy:
            self.progress_bar.show()
            self.progress_bar.setRange(0, 0) # indeterminate
        else:
            self.progress_bar.hide()

    def on_task_finished(self, success, result):
        self.set_ui_busy(False)
        if success:
            if result == "处理成功":
                self.update_stats_table()
                QMessageBox.information(self, "成功", "预处理完成！")
            elif result == "preview_ready":
                QMessageBox.information(self, "成功", "预览数据已生成，请点击'更新地图'查看。")
                self.update_viz_combos()
                self.update_viz_map()
            else:
                QMessageBox.information(self, "成功", f"操作成功。\n结果: {result}")
        else:
             QMessageBox.critical(self, "失败", f"任务失败: {result}")

    def update_stats_table(self):
        stats_df = self.processor.block_stats_df
        if stats_df is not None:
            self.lbl_blocks.setText(f"区块: {len(stats_df)}")
            self.lbl_links.setText(f"路段: {len(self.processor.links_df)}")
            self.lbl_nodes.setText(f"节点: {len(self.processor.nodes_df)}")
            
            # Populate Table
            self.block_table_model.clear()
            headers = ["选择", "区块ID", "路段数", "路段占比", "节点数", "节点占比"]
            self.block_table_model.setHorizontalHeaderLabels(headers)
            
            # Find largest block
            max_links_idx = stats_df['路段数'].idxmax()
            max_block_id = stats_df.loc[max_links_idx, '区块ID']
            
            for i, row in stats_df.iterrows():
                items = [
                    QStandardItem(),
                    QStandardItem(str(row["区块ID"])),
                    QStandardItem(str(row["路段数"])),
                    QStandardItem(str(row["路段占比"])),
                    QStandardItem(str(row["节点数"])),
                    QStandardItem(str(row["节点占比"]))
                ]
                for item in items[1:]: 
                    item.setEditable(False)
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                
                self.block_table_model.appendRow(items)

                # Checkbox container alignment (already centered but ensuring)
                cb = QCheckBox()
                # Default logic: Select largest block only
                if row["区块ID"] == max_block_id:
                    cb.setChecked(True)
                else:
                    cb.setChecked(False)
                    
                w = QWidget(); l = QHBoxLayout(w); l.addWidget(cb); l.setAlignment(Qt.AlignmentFlag.AlignCenter); l.setContentsMargins(0,0,0,0)
                self.block_table_view.setIndexWidget(self.block_table_model.index(i, 0), w)
        else:
            self.block_table_model.clear()

    def select_all_blocks(self):
        for i in range(self.block_table_model.rowCount()):
            index = self.block_table_model.index(i, 0)
            container = self.block_table_view.indexWidget(index)
            if container:
                cb = container.layout().itemAt(0).widget()
                cb.setChecked(True)

    def deselect_all_blocks(self):
        for i in range(self.block_table_model.rowCount()):
            index = self.block_table_model.index(i, 0)
            container = self.block_table_view.indexWidget(index)
            if container:
                cb = container.layout().itemAt(0).widget()
                cb.setChecked(False)

    def export_current_config(self):
        dir_path = QFileDialog.getExistingDirectory(self, "选择保存目录", self.last_config_dir)
        if dir_path:
            try:
                # Save Node Mapping
                node_path = os.path.join(dir_path, "节点表头映射关系.xlsx")
                self.save_table_to_file(self.node_table, node_path)
                
                # Save Link Mapping
                link_path = os.path.join(dir_path, "路网表头映射关系.xlsx")
                self.save_table_to_file(self.link_table, link_path)
                
                # Save Attr Mapping
                attr_path = os.path.join(dir_path, "路网字段属性映射关系.xlsx")
                self.save_table_to_file(self.attr_table, attr_path)
                
                QMessageBox.information(self, "成功", f"配置已导出至:\n{dir_path}")
            except Exception as e:
                QMessageBox.critical(self, "错误", f"导出配置失败: {e}")

    # --- Attribute Table Logic ---
    
    def toggle_attr_table_visibility(self):
        """Show/Hide the bottom attribute table area."""
        visible = self.btn_show_attr.isChecked()
        if visible:
            self.bottom_viz_container.show() # Show container
            
            # Restore previous sizes if available
            current_height = self.viz_splitter.height()
            if self.last_viz_splitter_sizes and sum(self.last_viz_splitter_sizes) > 0:
                 # Adjust proportionally to current height
                 ratio = current_height / sum(self.last_viz_splitter_sizes)
                 new_sizes = [int(s * ratio) for s in self.last_viz_splitter_sizes]
                 # Ensure attribute table has some height (at least 50px)
                 if new_sizes[1] < 50:
                     new_sizes = [int(current_height*0.6), int(current_height*0.4)]
                 self.viz_splitter.setSizes(new_sizes)
            else:
                 # Default split
                 self.viz_splitter.setSizes([int(current_height*0.6), int(current_height*0.4)])
            
            # Auto-load data if empty
            if self.link_attr_table.rowCount() == 0:
                self.update_attribute_tables()
        else:
            # Save current sizes before hiding (only if attribute table is visible/non-zero)
            sizes = self.viz_splitter.sizes()
            if sizes[1] > 0:
                self.last_viz_splitter_sizes = sizes
            
            self.bottom_viz_container.hide() # Hide entire container
            # Force splitter to give all space to map
            self.viz_splitter.setSizes([1000, 0])

    def toggle_attr_table(self):
        # Removed
        pass

    def update_attribute_tables(self):
        """Populate the attribute tables based on SELECTED blocks in the left panel."""
        if self.processor.processed_links_gdf is None:
            QMessageBox.warning(self, "提示", "请先进行数据预处理。")
            return

        # 1. Get selected blocks
        selected_block_ids = []
        for i in range(self.block_table_model.rowCount()):
            index = self.block_table_model.index(i, 0)
            container = self.block_table_view.indexWidget(index)
            if container:
                cb = container.layout().itemAt(0).widget()
                if cb.isChecked():
                    selected_block_ids.append(int(self.block_table_model.item(i, 1).text()))
        
        if not selected_block_ids:
            QMessageBox.warning(self, "提示", "请至少选择一个区块。")
            return
            
        # 2. Filter Data
        links = self.processor.processed_links_gdf[self.processor.processed_links_gdf['区块ID'].isin(selected_block_ids)]
        nodes = self.processor.processed_nodes_gdf[self.processor.processed_nodes_gdf['区块ID'].isin(selected_block_ids)]
        
        # 3. Populate Tables
        # Drop geometry column for display
        links_display = links.drop(columns='geometry') if 'geometry' in links.columns else links
        nodes_display = nodes.drop(columns='geometry') if 'geometry' in nodes.columns else nodes
        
        self._populate_table_widget(self.link_attr_table, links_display)
        self._populate_table_widget(self.node_attr_table, nodes_display)
        
    def _populate_table_widget(self, table, df):
        table.setRowCount(0)
        table.setColumnCount(len(df.columns))
        table.setHorizontalHeaderLabels(df.columns.astype(str))
        table.setRowCount(len(df))
        
        # Performance optimization: Disable sorting while populating
        table.setSortingEnabled(False)
        
        for i in range(len(df)):
            for j in range(len(df.columns)):
                val = df.iat[i, j]
                item = QTableWidgetItem(str(val))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                table.setItem(i, j, item)
        
        table.setSortingEnabled(True)

    def maximize_attr_table(self):
        """Toggle maximize/restore for attribute table."""
        is_max = self.btn_maximize_attr.isChecked()
        if is_max:
            self.btn_maximize_attr.setText("还原")
            # Hide map (index 0)
            self.viz_splitter.setSizes([0, 1000])
        else:
            self.btn_maximize_attr.setText("最大化")
            # Restore map (approx 60/40)
            total = self.viz_splitter.height()
            self.viz_splitter.setSizes([int(total*0.6), int(total*0.4)])

    # --- Visualization Helpers ---
    def update_viz_combos(self):
        # Update attribute combos based on available columns in preview gdf
        if self.processor.preview_links_gdf is not None:
            cols = self.processor.preview_links_gdf.columns.tolist()
            current = self.combo_link_attr.currentText()
            self.combo_link_attr.clear()
            self.combo_link_attr.addItems([c for c in cols if c != 'geometry'])
            if current in cols: self.combo_link_attr.setCurrentText(current)
            else: 
                # Prefer '道路等级' or 'highway'
                default = '道路等级' if '道路等级' in cols else ('highway' if 'highway' in cols else cols[0])
                self.combo_link_attr.setCurrentText(default)

        if self.processor.preview_nodes_gdf is not None:
            cols = self.processor.preview_nodes_gdf.columns.tolist()
            self.combo_node_attr.clear()
            self.combo_node_attr.addItems([c for c in cols if c != 'geometry'])

    def update_viz_map(self):
        if not WEB_ENGINE_AVAILABLE: return
        
        links = self.processor.preview_links_gdf
        nodes = self.processor.preview_nodes_gdf
        
        if links is None and nodes is None:
            return
            
        try:
            # Create Map
            m = folium.Map(location=[39.9, 116.4], zoom_start=12, tiles='CartoDB positron')
            
            # Auto-center
            bounds = None
            if links is not None and not links.empty:
                bounds = links.total_bounds
            elif nodes is not None and not nodes.empty:
                bounds = nodes.total_bounds
                
            if bounds is not None:
                m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])

            # Draw Links
            if self.cb_show_links.isChecked() and links is not None:
                attr = self.combo_link_attr.currentText()
                # Simple style function
                def style_fn(feature):
                    return {
                        'color': self.viz_link_color,
                        'weight': 2,
                        'opacity': 0.7
                    }
                folium.GeoJson(
                    links,
                    name="Links",
                    style_function=style_fn,
                    tooltip=folium.GeoJsonTooltip(fields=[attr] if attr else None)
                ).add_to(m)

            # Draw Nodes
            if self.cb_show_nodes.isChecked() and nodes is not None:
                for _, row in nodes.iterrows():
                    folium.CircleMarker(
                        location=[row.geometry.y, row.geometry.x],
                        radius=3,
                        color=self.viz_node_color,
                        fill=True,
                        popup=str(row.to_dict())
                    ).add_to(m)

            # Save to temp
            data = m._repr_html_()
            self.web_view.setHtml(data)
            
        except Exception as e:
            self.log(f"Map Error: {e}")

    def pick_color(self, target):
        color = QColorDialog.getColor()
        if color.isValid():
            hex_color = color.name()
            if target == 'link':
                self.viz_link_color = hex_color
                self.btn_link_color.setStyleSheet(f"background-color: {hex_color}; border: none;")
            else:
                self.viz_node_color = hex_color
                self.btn_node_color.setStyleSheet(f"background-color: {hex_color}; border: none;")

    def log(self, msg):
        if self.current_log_widget:
            self.current_log_widget.append(msg)

    # --- Common File Helpers ---
    def browse_file(self, line_edit, filters, dir_type):
        d = self.last_osm_dir if dir_type == 'osm' else self.last_config_dir
        f, _ = QFileDialog.getOpenFileName(self, "选择文件", d, filters)
        if f:
            line_edit.setText(f)
            if dir_type == 'osm': self.last_osm_dir = os.path.dirname(f)
            else: self.last_config_dir = os.path.dirname(f)

    def browse_dir(self):
        d = QFileDialog.getExistingDirectory(self, "选择目录", self.last_osm_dir)
        if d:
            self.out_input.setText(d)
            self.last_osm_dir = d

    def on_mode_changed(self, id):
        self.input_stack.setCurrentIndex(id - 1)

    def load_settings(self):
        if not os.path.exists(self.settings_file): return
        try:
            with open(self.settings_file, 'r') as f:
                data = json.load(f)
                self.last_osm_dir = data.get('last_osm_dir', '.')
                self.city_input.setText(data.get('city', ''))
                self.out_input.setText(data.get('out_dir', 'output'))
                self.format_conversion_checkbox.setChecked(data.get('fmt_conv', True))
                # Restore others...
        except: pass

    def init_defaults(self):
        # Trigger toggle logic
        self.on_format_conversion_toggled(self.format_conversion_checkbox.isChecked())

    def closeEvent(self, event):
        # Save settings on exit
        data = {
            'last_osm_dir': self.last_osm_dir,
            'city': self.city_input.text(),
            'out_dir': self.out_input.text(),
            'fmt_conv': self.format_conversion_checkbox.isChecked()
        }
        with open(self.settings_file, 'w') as f:
            json.dump(data, f)
        event.accept()

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
