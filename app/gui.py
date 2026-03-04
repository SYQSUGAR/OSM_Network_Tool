import sys
import os
import json
import tempfile
import folium
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QLabel, QLineEdit, QPushButton, 
                             QTextEdit, QFileDialog, QProgressBar, QMessageBox, 
                             QGroupBox, QFrame, QRadioButton, QButtonGroup, QTabWidget, QStackedWidget, QCheckBox,
                             QTableView, QHeaderView, QSplitter, QComboBox, QColorDialog)
from PyQt6.QtGui import QStandardItemModel, QStandardItem
from PyQt6.QtCore import QThread, pyqtSignal, Qt, QUrl
from PyQt6.QtGui import QIcon, QFont
from importlib import metadata
WEB_ENGINE_AVAILABLE = False
WEB_ENGINE_IMPORT_ERROR = None
try:
    from PyQt6.QtWebEngineWidgets import QWebEngineView
    # 只要 QWebEngineView 导入成功就认为可用；核心模块异常仅用于诊断显示
    WEB_ENGINE_AVAILABLE = True
    try:
        from PyQt6.QtWebEngineCore import QWebEngineProfile  # 进一步确保模块可加载
    except Exception as _core_e:
        WEB_ENGINE_IMPORT_ERROR = _core_e
except Exception as e:
    WEB_ENGINE_IMPORT_ERROR = e
    WEB_ENGINE_AVAILABLE = False
    print(f"[WebEngine] 导入失败: {e!r}")


# 导入我们的模块
# 使用相对导入，这是在包内进行模块导入的标准方式
from .downloader import download_from_osmnx, process_from_osm_file, read_from_csv_files
from .processor import DataProcessor, export_results

class WorkerThread(QThread):
    log_signal = pyqtSignal(str)
    # 信号可以发送任何类型的对象
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
            if self.task_type in ['preprocess', 'export_raw']:
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
            result = None
            if self.task_type == 'preprocess':
                self.log_signal.emit("步骤 2/2: 正在执行数据预处理...")
                processor = self.kwargs.get('processor')
                processor.run_preprocessing(
                    links_df, nodes_df,
                    self.kwargs.get('node_header_map'),
                    self.kwargs.get('link_header_map'),
                    self.kwargs.get('attr_map'),
                    log_callback=self.log_signal.emit
                )
                self.log_signal.emit(f"--- 预处理成功完成！---")
                self.finished_signal.emit(True, "预处理成功") # 只发送成功信号

            elif self.task_type == 'export_raw':
                self.log_signal.emit("步骤 2/2: 正在直接导出原始数据...")
                result = export_results(nodes_df, links_df, self.kwargs.get('output_dir'), is_raw=True, log_callback=self.log_signal.emit)
                self.log_signal.emit(f"--- 原始数据导出成功！---")
                self.finished_signal.emit(True, result)
            
            elif self.task_type == 'filter_and_export':
                self.log_signal.emit("正在执行筛选和导出...")
                processor = self.kwargs.get('processor')
                if not isinstance(processor, DataProcessor):
                    raise TypeError("任务 'filter_and_export' 需要一个有效的 DataProcessor 实例。")
                
                # 兼容旧逻辑: 直接生成并导出，但也更新 preview 变量
                processor.generate_preview_data(self.kwargs.get('filter_criteria'), log_callback=self.log_signal.emit)
                result = processor.export_preview_data(self.kwargs.get('output_dir'), log_callback=self.log_signal.emit)

                self.log_signal.emit(f"--- 筛选和导出成功！---")
                self.finished_signal.emit(True, result)

            elif self.task_type == 'preview':
                self.log_signal.emit("正在生成预览数据...")
                processor = self.kwargs.get('processor')
                processor.generate_preview_data(self.kwargs.get('filter_criteria'), log_callback=self.log_signal.emit)
                self.log_signal.emit(f"--- 预览数据生成完毕！---")
                self.finished_signal.emit(True, "preview_ready")

            elif self.task_type == 'export_preview':
                self.log_signal.emit("正在导出预览数据...")
                processor = self.kwargs.get('processor')
                result = processor.export_preview_data(self.kwargs.get('output_dir'), log_callback=self.log_signal.emit)
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
        self.resize(1000, 750) # 稍微加大窗口以适应地图
        
        self.last_osm_dir = "."
        self.last_config_dir = "."
        self.settings_file = "settings.json"

        # 用于动态指向当前应输出日志的 QTextEdit
        self.current_log_widget = None
        self.current_progress_bar = None

        # 持有核心处理器实例
        self.processor = DataProcessor()
        
        # 可视化相关变量
        self.viz_link_color = "#3388ff" # 默认蓝色
        self.viz_node_color = "#ff3333" # 默认红色
        
        self.init_style()
        
        # Central Widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(15)
        main_layout.setContentsMargins(20, 20, 20, 20)
        
        # Header Title
        title_label = QLabel("OSM路网数据处理工具")
        title_label.setObjectName("TitleLabel")
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        main_layout.addWidget(title_label)
        
        # Main Tab Widget
        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)
        
        # 初始化所有主标签页
        self.init_main_tabs()
        
        # Load settings before setting defaults
        self.load_settings()

        # Initialize defaults
        self.init_defaults()

    def init_style(self):
        self.setStyleSheet("""
            QMainWindow { background-color: #f5f6fa; }
            QLabel#TitleLabel { font-size: 24px; font-weight: bold; color: #2c3e50; margin-bottom: 10px; }
            
            /* ===== 标签样式 ===== */
            QLabel { font-size: 14px; color: #2f3640; font-weight: 500; }
            QLabel:disabled { color: #a4b0be; } /* 补充：文字被禁用时变灰 */
            
            /* ===== 输入框样式 ===== */
            QLineEdit { padding: 8px; border: 1px solid #dcdde1; border-radius: 4px; background-color: white; font-size: 13px; color: #2f3640; }
            QLineEdit:focus { border: 1px solid #3498db; }
            QLineEdit:disabled { background-color: #f1f2f6; color: #a4b0be; border: 1px solid #dcdde1; } /* 补充：输入框被禁用时背景和文字变灰 */
            
            /* ===== 按钮通用样式 ===== */
            QPushButton { padding: 8px 16px; border-radius: 4px; font-weight: bold; font-size: 13px; border: none; }
            
            /* ===== 浏览文件按钮样式 ===== */
            QPushButton#BrowseBtn { background-color: #7f8fa6; color: white; }
            QPushButton#BrowseBtn:hover { background-color: #718093; }
            QPushButton#BrowseBtn:disabled { background-color: #ced6e0; color: #f5f6fa; } /* 补充：浏览按钮被禁用时变灰 */
            
            /* ===== 运行按钮样式 ===== */
            QPushButton#RunBtn { background-color: #3498db; color: white; font-size: 15px; padding: 12px; margin-top: 10px; }
            QPushButton#RunBtn:hover { background-color: #2980b9; }
            QPushButton#RunBtn:disabled { background-color: #bdc3c7; }

            /* ===== 预览按钮样式 ===== */
            QPushButton#PreviewBtn { background-color: #2ecc71; color: white; font-size: 15px; padding: 12px; margin-top: 10px; }
            QPushButton#PreviewBtn:hover { background-color: #27ae60; }
            QPushButton#PreviewBtn:disabled { background-color: #bdc3c7; }
            
            /* ===== 其他布局样式 ===== */
            QGroupBox { background-color: white; border: 1px solid #e1e1e1; border-radius: 6px; margin-top: 12px; padding-top: 24px; font-weight: bold; font-size: 14px; color: #2c3e50; }
            QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top left; padding: 0 5px; left: 10px; background-color: white; }
            QTabWidget::pane { border: 1px solid #e1e1e1; background: white; border-radius: 4px; }
            QTabBar::tab { background: #dcdde1; color: #2c3e50; padding: 10px 20px; border-top-left-radius: 4px; border-top-right-radius: 4px; margin-right: 2px; font-weight: bold; }
            QTabBar::tab:selected { background: white; border-bottom: 2px solid #3498db; color: #3498db; }
            QTabBar::tab:disabled { background: #f0f0f0; color: #b0b0b0; } /* 禁用标签页的样式 */
            QRadioButton { spacing: 8px; font-size: 14px; color: #2c3e50; }
            QFrame[frameShape="4"] {
                margin: 8px 2px;
                border: none;
                border-top: 1px solid #dcdde1;
            }
            QFrame#ConfigSeparator {
                min-height: 1px;
                max-height: 1px;
                border: none;
                border-top: 1px solid #b2bec3;
                margin: 12px 2px;
            }
            /* 为筛选日志组单独设置较小的顶部内边距 */
            QGroupBox#FilterLogGroup {
                padding-top: 10px;
            }
            /* 为预处理日志组也设置较小的顶部内边距 */
            QGroupBox#PreprocessLogGroup {
                padding-top: 10px;
            }

            /* 为数据概览组也设置较小的顶部内边距 */
            QGroupBox#StatsGroup {
                padding-top: 10px;
            }

            /* ===== 表格样式 ===== */
            QTableView { border: 1px solid #e1e1e1; gridline-color: #e1e1e1; }
            QHeaderView::section {
                background-color: #e8ecf0; /* 表头背景色 */
                padding: 6px;
                font-weight: bold;
                border: 1px solid #dcdde1;
                border-left: none;
            }
        """)

    def init_main_tabs(self):
        # === 主标签页 1: 数据预处理 ===
        preprocess_widget = QWidget()
        self.init_preprocess_tab(preprocess_widget) # 将逻辑拆分到新函数
        self.tabs.addTab(preprocess_widget, "数据预处理")

        # === 主标签页 2: 数据筛选 ===
        self.filter_widget = QWidget()
        self.init_filter_tab(self.filter_widget) # 将逻辑拆分到新函数
        self.tabs.addTab(self.filter_widget, "数据筛选")

        # === 主标签页 3: 可视化展示 ===
        self.viz_tab = QWidget()
        self.init_viz_tab(self.viz_tab) # 传入父控件
        self.tabs.addTab(self.viz_tab, "可视化展示")

        # 默认禁用筛选和可视化标签页
        self.tabs.setTabEnabled(1, False)
        self.tabs.setTabEnabled(2, False)

    def init_preprocess_tab(self, parent_widget):
        layout = QVBoxLayout(parent_widget)
        
        # 1. Data Acquisition Group
        acq_group = QGroupBox("1. 数据获取")
        acq_layout = QVBoxLayout(acq_group)

        # Mode selection layout
        mode_layout = QHBoxLayout()
        self.mode_bg = QButtonGroup(self)
        self.radio_online = QRadioButton("联网下载")
        self.radio_osm = QRadioButton("OSM数据处理 (.osm)")
        self.radio_csv = QRadioButton("CSV数据处理 (.csv)")
        self.radio_online.setChecked(True)
        self.mode_bg.addButton(self.radio_online, 1)
        self.mode_bg.addButton(self.radio_osm, 2)
        self.mode_bg.addButton(self.radio_csv, 3)
        self.mode_bg.idClicked.connect(self.on_mode_changed)
        mode_layout.addWidget(self.radio_online)
        mode_layout.addWidget(self.radio_osm)
        mode_layout.addWidget(self.radio_csv)
        mode_layout.addStretch()
        acq_layout.addLayout(mode_layout)

        # Separator Line
        separator = QFrame()
        separator.setFrameShape(QFrame.Shape.HLine)
        acq_layout.addWidget(separator)

        # Input Stack
        self.input_stack = QStackedWidget()
        acq_layout.addWidget(self.input_stack)

        # Page 1: Online Download Input
        page_online = QWidget()
        pd_layout = QHBoxLayout(page_online)
        pd_layout.setContentsMargins(0, 0, 0, 0)
        pd_layout.addWidget(QLabel("目标城市:"))
        self.city_input = QLineEdit()
        self.city_input.setPlaceholderText("例如: Beijing, China")
        pd_layout.addWidget(self.city_input)
        self.input_stack.addWidget(page_online)

        # Page 2: OSM File Input
        page_osm = QWidget()
        osm_layout = QHBoxLayout(page_osm)
        osm_layout.setContentsMargins(0, 0, 0, 0)
        osm_layout.addWidget(QLabel("OSM文件:"))
        self.osm_file_input = QLineEdit()
        self.osm_btn = QPushButton("选择文件")
        self.osm_btn.setObjectName("BrowseBtn")
        self.osm_btn.clicked.connect(lambda: self.browse_file(self.osm_file_input, "OSM Files (*.osm *.pbf)", 'osm'))
        osm_layout.addWidget(self.osm_file_input)
        osm_layout.addWidget(self.osm_btn)
        self.input_stack.addWidget(page_osm)

        # Page 3: CSV Files Input
        page_csv = QWidget()
        pf_layout = QVBoxLayout(page_csv)
        pf_layout.setContentsMargins(0, 0, 0, 0)

        # Link file input
        link_layout = QHBoxLayout()
        link_label = QLabel("Link文件:")
        link_label.setFixedWidth(70)
        link_layout.addWidget(link_label)
        self.link_file_input = QLineEdit()
        self.link_btn = QPushButton("选择文件")
        self.link_btn.setObjectName("BrowseBtn")
        self.link_btn.clicked.connect(lambda: self.browse_file(self.link_file_input, "CSV Files (*.csv)", 'osm'))
        link_layout.addWidget(self.link_file_input)
        link_layout.addWidget(self.link_btn)
        pf_layout.addLayout(link_layout)

        # Node file input
        node_layout = QHBoxLayout()
        node_label = QLabel("Node文件:")
        node_label.setFixedWidth(70)
        node_layout.addWidget(node_label)
        self.node_file_input = QLineEdit()
        self.node_btn = QPushButton("选择文件")
        self.node_btn.setObjectName("BrowseBtn")
        self.node_btn.clicked.connect(lambda: self.browse_file(self.node_file_input, "CSV Files (*.csv)", 'osm'))
        node_layout.addWidget(self.node_file_input)
        node_layout.addWidget(self.node_btn)
        pf_layout.addLayout(node_layout)

        self.input_stack.addWidget(page_csv)
        
        layout.addWidget(acq_group)
        
        # 2. Configuration Group
        config_group = QGroupBox("2. 参数配置")
        config_layout = QVBoxLayout()

        # Format Conversion Checkbox
        self.format_conversion_checkbox = QCheckBox("启用格式转换 (需要映射文件)")
        self.format_conversion_checkbox.setChecked(True)
        self.format_conversion_checkbox.toggled.connect(self._update_config_widgets_state)
        config_layout.addWidget(self.format_conversion_checkbox)
        
        # Mapping files group
        self.mapping_files_group = QWidget()
        mapping_files_layout = QVBoxLayout(self.mapping_files_group)
        mapping_files_layout.setContentsMargins(0, 5, 0, 5)
        
        # Node Header Mapping (New)
        node_header_layout = QHBoxLayout()
        node_header_label = QLabel("节点表头映射:")
        node_header_label.setFixedWidth(100)
        self.node_header_input = QLineEdit()
        self.node_header_btn = QPushButton("选择文件")
        self.node_header_btn.setObjectName("BrowseBtn")
        self.node_header_btn.clicked.connect(lambda: self.browse_file(self.node_header_input, "Excel Files (*.xlsx *.xls)", 'config'))
        node_header_layout.addWidget(node_header_label)
        node_header_layout.addWidget(self.node_header_input)
        node_header_layout.addWidget(self.node_header_btn)
        mapping_files_layout.addLayout(node_header_layout)

        # Header Mapping
        header_layout = QHBoxLayout()
        header_label = QLabel("路网表头映射:")
        header_label.setFixedWidth(100)
        self.header_input = QLineEdit()
        self.header_btn = QPushButton("选择文件")
        self.header_btn.setObjectName("BrowseBtn")
        self.header_btn.clicked.connect(lambda: self.browse_file(self.header_input, "Excel Files (*.xlsx *.xls)", 'config'))
        header_layout.addWidget(header_label)
        header_layout.addWidget(self.header_input)
        header_layout.addWidget(self.header_btn)
        mapping_files_layout.addLayout(header_layout)
        
        # Attribute Mapping
        attr_layout = QHBoxLayout()
        attr_label = QLabel("属性映射:")
        attr_label.setFixedWidth(100)
        self.attr_input = QLineEdit()
        self.attr_btn = QPushButton("选择文件")
        self.attr_btn.setObjectName("BrowseBtn")
        self.attr_btn.clicked.connect(lambda: self.browse_file(self.attr_input, "Excel Files (*.xlsx *.xls)", 'config'))
        attr_layout.addWidget(attr_label)
        attr_layout.addWidget(self.attr_input)
        attr_layout.addWidget(self.attr_btn)
        mapping_files_layout.addLayout(attr_layout)

        config_layout.addWidget(self.mapping_files_group)

        # Separator
        config_separator = QFrame()
        config_separator.setFrameShape(QFrame.Shape.HLine)
        config_separator.setObjectName("ConfigSeparator") # Give it a unique name
        config_layout.addWidget(config_separator)
        
        # Output Directory
        out_layout = QHBoxLayout()
        out_label = QLabel("输出目录:")
        out_label.setFixedWidth(100)
        self.out_input = QLineEdit()
        self.out_input.setText(os.path.join(os.getcwd(), "output"))
        self.out_btn = QPushButton("选择路径")
        self.out_btn.setObjectName("BrowseBtn")
        self.out_btn.clicked.connect(self.browse_dir)
        out_layout.addWidget(out_label)
        out_layout.addWidget(self.out_input)
        out_layout.addWidget(self.out_btn)
        config_layout.addLayout(out_layout)
        
        config_group.setLayout(config_layout)
        layout.addWidget(config_group)
        
        # 4. Action Area
        self.run_btn = QPushButton("开始预处理") # 重命名按钮
        self.run_btn.setObjectName("RunBtn")
        self.run_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.run_btn.clicked.connect(self.start_process)
        layout.addWidget(self.run_btn)
        
        # 5. Logs
        log_group = QGroupBox("运行日志")
        log_group.setObjectName("PreprocessLogGroup") # 为其设置一个唯一的对象名称
        log_layout = QVBoxLayout()
        self.preprocess_progress = QProgressBar()
        self.preprocess_progress.setRange(0, 0)
        self.preprocess_progress.setTextVisible(False)
        self.preprocess_progress.hide()
        log_layout.addWidget(self.preprocess_progress)
        self.preprocess_log_area = QTextEdit()
        self.preprocess_log_area.setReadOnly(True)
        log_layout.addWidget(self.preprocess_log_area)
        log_group.setLayout(log_layout)
        layout.addWidget(log_group, 1) # 添加到布局并设置拉伸因子

    def init_filter_tab(self, parent_widget):
        filter_layout = QVBoxLayout(parent_widget)

        # 1. 统计信息展示区
        stats_group = QGroupBox("数据概览")
        stats_group.setObjectName("StatsGroup") # 为其设置一个唯一的对象名称
        stats_layout = QHBoxLayout()
        self.block_count_label = QLabel("区块数量: --")
        self.link_count_label = QLabel("路段总数: --")
        self.node_count_label = QLabel("节点总数: --")
        stats_layout.addWidget(self.block_count_label)
        stats_layout.addStretch()
        stats_layout.addWidget(self.link_count_label)
        stats_layout.addStretch()
        stats_layout.addWidget(self.node_count_label)
        stats_group.setLayout(stats_layout)
        filter_layout.addWidget(stats_group)
        
        # 2. 动态内容区 (用于显示表格)
        self.dynamic_filter_content_widget = QWidget()
        dynamic_layout = QVBoxLayout(self.dynamic_filter_content_widget)
        dynamic_layout.setContentsMargins(0, 0, 0, 0)

        # 创建表格视图和模型
        self.block_table_view = QTableView()
        self.block_table_model = QStandardItemModel()
        self.block_table_view.setModel(self.block_table_model)
        # 设置表格的水平表头拉伸策略
        self.block_table_view.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        dynamic_layout.addWidget(self.block_table_view)

        # 为表格和日志区域设置拉伸因子，让表格占据更多空间
        filter_layout.addWidget(self.dynamic_filter_content_widget, 5)
        
        # 3. 按钮区
        btn_layout = QHBoxLayout()
        
        self.preview_btn = QPushButton("生成预览并可视化")
        self.preview_btn.setObjectName("PreviewBtn")
        self.preview_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.preview_btn.clicked.connect(self.start_preview_generation)
        btn_layout.addWidget(self.preview_btn, 1) # 1: stretch factor
        
        self.export_filtered_btn = QPushButton("直接导出筛选结果")
        self.export_filtered_btn.setObjectName("RunBtn") # 复用样式
        self.export_filtered_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.export_filtered_btn.clicked.connect(self.start_filter_export)
        btn_layout.addWidget(self.export_filtered_btn, 1) # 1: stretch factor
        
        filter_layout.addLayout(btn_layout)

        # 4. 日志区域
        log_group = QGroupBox("运行日志")
        log_group.setObjectName("FilterLogGroup") # 为其设置一个唯一的对象名称
        log_layout = QVBoxLayout()
        self.filter_progress = QProgressBar()
        self.filter_progress.setRange(0, 0)
        self.filter_progress.setTextVisible(False)
        self.filter_progress.hide()
        log_layout.addWidget(self.filter_progress)
        self.filter_log_area = QTextEdit()
        self.filter_log_area.setReadOnly(True)
        log_layout.addWidget(self.filter_log_area)
        log_group.setLayout(log_layout)
        filter_layout.addWidget(log_group, 2)

    def init_viz_tab(self, parent_widget):
        layout = QVBoxLayout(parent_widget)
        
        self.viz_splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # === Left Panel: Controls ===
        controls_widget = QWidget()
        controls_layout = QVBoxLayout(controls_widget)
        controls_layout.setContentsMargins(10, 10, 10, 10)
        
        # Layers Group
        layers_group = QGroupBox("图层控制")
        layers_layout = QVBoxLayout()
        self.show_links_cb = QCheckBox("显示路段 (Links)")
        self.show_links_cb.setChecked(True)
        self.show_nodes_cb = QCheckBox("显示节点 (Nodes)")
        self.show_nodes_cb.setChecked(True)
        layers_layout.addWidget(self.show_links_cb)
        layers_layout.addWidget(self.show_nodes_cb)
        layers_group.setLayout(layers_layout)
        controls_layout.addWidget(layers_group)
        
        # Link Style Group
        link_style_group = QGroupBox("路段样式")
        link_style_layout = QVBoxLayout()
        link_style_layout.addWidget(QLabel("着色属性:"))
        self.link_color_attr_combo = QComboBox()
        link_style_layout.addWidget(self.link_color_attr_combo)
        
        # Color picker button
        self.link_color_btn = QPushButton("选择基础颜色")
        self.link_color_btn.clicked.connect(lambda: self.pick_color('link'))
        # Set background color of button to current color
        self.link_color_btn.setStyleSheet(f"background-color: {self.viz_link_color}; color: white;")
        link_style_layout.addWidget(self.link_color_btn)
        
        link_style_group.setLayout(link_style_layout)
        controls_layout.addWidget(link_style_group)
        
        # Node Style Group
        node_style_group = QGroupBox("节点样式")
        node_style_layout = QVBoxLayout()
        node_style_layout.addWidget(QLabel("着色属性:"))
        self.node_color_attr_combo = QComboBox()
        node_style_layout.addWidget(self.node_color_attr_combo)
        
        self.node_color_btn = QPushButton("选择基础颜色")
        self.node_color_btn.clicked.connect(lambda: self.pick_color('node'))
        self.node_color_btn.setStyleSheet(f"background-color: {self.viz_node_color}; color: white;")
        node_style_layout.addWidget(self.node_color_btn)
        
        node_style_group.setLayout(node_style_layout)
        controls_layout.addWidget(node_style_group)
        
        controls_layout.addStretch()
        
        # Update Map Button
        self.update_map_btn = QPushButton("更新地图")
        self.update_map_btn.setObjectName("RunBtn")
        self.update_map_btn.clicked.connect(self.update_viz_map)
        controls_layout.addWidget(self.update_map_btn)
        
        # Export Preview Button
        self.export_preview_btn = QPushButton("导出当前预览")
        self.export_preview_btn.setObjectName("PreviewBtn")
        self.export_preview_btn.clicked.connect(self.start_preview_export)
        controls_layout.addWidget(self.export_preview_btn)
        
        diag_btn = QPushButton("诊断WebEngine环境")
        diag_btn.clicked.connect(self.show_webengine_diagnostics)
        controls_layout.addWidget(diag_btn)
        
        # === Right Panel: Map ===
        if WEB_ENGINE_AVAILABLE:
            self.web_view = QWebEngineView()
            self.web_view.setHtml("<html><body><h3 align='center'>请先生成预览数据</h3></body></html>")
            self.viz_splitter.addWidget(self.web_view)
        else:
            self.web_view = QLabel(self._webengine_status_text())
            self.web_view.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.web_view.setStyleSheet("background-color: #ecf0f1; color: #7f8c8d; font-size: 16px;")
            self.viz_splitter.addWidget(self.web_view)

        # Add to splitter
        self.viz_splitter.addWidget(controls_widget)
        # self.web_view added above
        self.viz_splitter.setStretchFactor(1, 4) # Give map more space
        
        layout.addWidget(self.viz_splitter)

    def _update_config_widgets_state(self, checked):
        """根据“格式转换”复选框的状态，启用/禁用相关控件。"""
        # 1. 启用/禁用映射文件输入
        self.mapping_files_group.setEnabled(checked)

        # 2. 启用/禁用“数据筛选”标签页
        self.tabs.setTabEnabled(1, checked)

        # 3. 更改主按钮的文本
        if checked:
            self.run_btn.setText("开始预处理")
        else:
            self.run_btn.setText("直接导出原始数据")

    def on_mode_changed(self, id):
        # id 1: online, 2: osm, 3: csv
        self.input_stack.setCurrentIndex(id - 1)

    def load_settings(self):
        if not os.path.exists(self.settings_file):
            return
        try:
            with open(self.settings_file, 'r') as f:
                settings = json.load(f)
                
                # 恢复目录路径
                self.last_osm_dir = settings.get('last_osm_dir', '.')
                self.last_config_dir = settings.get('last_config_dir', '.')

                # 恢复输入框内容
                self.city_input.setText(settings.get('online_input', ''))
                self.osm_file_input.setText(settings.get('osm_input', ''))
                self.link_file_input.setText(settings.get('csv_link_input', ''))
                self.node_file_input.setText(settings.get('csv_node_input', ''))
                self.node_header_input.setText(settings.get('node_header_input', ''))
                self.header_input.setText(settings.get('link_header_input', ''))
                self.attr_input.setText(settings.get('attr_input', ''))
                self.out_input.setText(settings.get('output_dir', os.path.join(os.getcwd(), "output")))

                # 恢复格式转换复选框的状态
                format_conversion_enabled = settings.get('format_conversion_enabled', True)
                self.format_conversion_checkbox.setChecked(format_conversion_enabled)
                self._update_config_widgets_state(format_conversion_enabled)
                # 恢复上次选择的模式
                last_mode_id = settings.get('last_mode_id', 1)
                if self.mode_bg.button(last_mode_id):
                    self.mode_bg.button(last_mode_id).setChecked(True)
                    self.on_mode_changed(last_mode_id)

        except (json.JSONDecodeError, IOError) as e:
            print(f"无法加载设置: {e}")

    def save_settings(self):
        settings = {
            'last_mode_id': self.mode_bg.checkedId(),
            'online_input': self.city_input.text(),
            'osm_input': self.osm_file_input.text(),
            'csv_link_input': self.link_file_input.text(),
            'csv_node_input': self.node_file_input.text(),
            'node_header_input': self.node_header_input.text(),
            'link_header_input': self.header_input.text(),
            'attr_input': self.attr_input.text(),
            'output_dir': self.out_input.text(),
            'format_conversion_enabled': self.format_conversion_checkbox.isChecked(),
            'last_osm_dir': self.last_osm_dir,
            'last_config_dir': self.last_config_dir
        }
        try:
            with open(self.settings_file, 'w') as f:
                json.dump(settings, f, indent=4)
        except IOError as e:
            print(f"无法保存设置: {e}")

    def init_defaults(self):
        resource_dir = os.path.join(os.getcwd(), "resources")
        if os.path.exists(resource_dir):
            files = os.listdir(resource_dir)
            for f in files:
                if "节点表头" in f:
                    self.node_header_input.setText(os.path.join(resource_dir, f))
                if "路网表头" in f:
                    self.header_input.setText(os.path.join(resource_dir, f))
                if "属性" in f:
                    self.attr_input.setText(os.path.join(resource_dir, f))

    def browse_file(self, line_edit, filter_str, dir_group):
        start_dir = "."
        if dir_group == 'osm':
            start_dir = self.last_osm_dir
        elif dir_group == 'config':
            start_dir = self.last_config_dir

        file, _ = QFileDialog.getOpenFileName(self, "选择文件", start_dir, filter_str)
        if file:
            line_edit.setText(file)
            if dir_group == 'osm':
                self.last_osm_dir = os.path.dirname(file)
            elif dir_group == 'config':
                self.last_config_dir = os.path.dirname(file)

    def browse_dir(self):
        start_dir = self.last_osm_dir # Use the same logic as OSM files
        directory = QFileDialog.getExistingDirectory(self, "选择输出目录", start_dir)
        if directory:
            self.out_input.setText(directory)
            self.last_osm_dir = directory

    def log(self, message):
        if self.current_log_widget:
            self.current_log_widget.append(message)
            # 自动滚动到底部
            scrollbar = self.current_log_widget.verticalScrollBar()
            scrollbar.setValue(scrollbar.maximum())

    def start_process(self):
        mode_id = self.mode_bg.checkedId()
        modes = {1: 'online', 2: 'osm', 3: 'csv'}
        mode = modes.get(mode_id)

        input_val = None
        if mode == 'online':
            input_val = self.city_input.text().strip()
            if not input_val:
                QMessageBox.warning(self, "输入错误", "请输入城市名称。")
                return
        elif mode == 'osm':
            input_val = self.osm_file_input.text().strip()
            if not input_val or not os.path.exists(input_val):
                QMessageBox.warning(self, "输入错误", "请为OSM数据处理模式选择一个有效的 .osm 或 .pbf 文件。")
                return
        elif mode == 'csv':
            link_file = self.link_file_input.text().strip()
            node_file = self.node_file_input.text().strip()
            if not link_file or not os.path.exists(link_file) or not node_file or not os.path.exists(node_file):
                QMessageBox.warning(self, "输入错误", "请为CSV数据处理模式选择有效的 Link 和 Node CSV 文件。")
                return
            input_val = {"link": link_file, "node": node_file}

        # 获取参数
        format_conversion_enabled = self.format_conversion_checkbox.isChecked()
        node_header_map = self.node_header_input.text().strip()
        header_map = self.header_input.text().strip()
        attr_map = self.attr_input.text().strip()
        output_dir = self.out_input.text().strip()

        # 如果启用了格式转换，则检查映射文件是否存在
        if format_conversion_enabled:
            if not all(os.path.exists(p) for p in [node_header_map, header_map, attr_map]):
                QMessageBox.warning(self, "输入错误", "格式转换已启用，请选择所有三个有效的映射文件。")
                return

        if not output_dir:
            QMessageBox.warning(self, "输入错误", "请指定一个有效的输出目录。")
            return

        if not os.path.isdir(output_dir):
            try:
                os.makedirs(output_dir)
            except OSError as e:
                QMessageBox.critical(self, "创建目录失败", f"无法创建输出目录: {output_dir}\n错误: {e}")
                return

        # 根据复选框状态决定要执行的任务
        task_type = 'preprocess' if format_conversion_enabled else 'export_raw'

        # 准备任务参数
        task_kwargs = {
            'processor': self.processor,
            'mode': mode,
            'input_val': input_val,
            'output_dir': output_dir,
            'node_header_map': node_header_map,
            'link_header_map': header_map,
            'attr_map': attr_map
        }

        self.run_btn.setEnabled(False)
        
        # 设置当前的日志目标并清空
        self.current_log_widget = self.preprocess_log_area
        self.current_progress_bar = self.preprocess_progress
        self.current_log_widget.clear()

        self.current_progress_bar.show()
        self.log(f"正在以 '{mode}' 模式启动...")

        # 启动工作线程
        self.worker = WorkerThread(task_type=task_type, **task_kwargs)
        self.worker.log_signal.connect(self.log)
        self.worker.finished_signal.connect(self.on_finished)
        self.worker.start()

    def on_finished(self, success, result):
        self.run_btn.setEnabled(True)
        self.export_filtered_btn.setEnabled(True) 
        self.preview_btn.setEnabled(True)
        self.export_preview_btn.setEnabled(True)

        if self.current_progress_bar:
            self.current_progress_bar.hide()

        if not success:
            QMessageBox.critical(self, "错误", f"任务失败: {result}")
            return

        # 根据完成的任务类型执行不同操作
        task_type = self.worker.task_type
        if task_type == 'preprocess':
            QMessageBox.information(self, "成功", "数据预处理完成！现在可以进行数据筛选或可视化。")
            self.update_filter_tab(self.processor.block_stats_df)
            self.tabs.setTabEnabled(1, True) 
            self.tabs.setTabEnabled(2, False) # 预处理完不能直接去可视化，要先预览
            self.tabs.setCurrentIndex(1)
        
        elif task_type == 'export_raw':
            QMessageBox.information(self, "成功", f"原始数据已成功导出。")
            
        elif task_type == 'filter_and_export':
            QMessageBox.information(self, "成功", f"筛选后的数据已成功导出。")

        elif task_type == 'preview':
            QMessageBox.information(self, "成功", "预览数据生成成功！正在跳转至可视化界面...")
            self.tabs.setTabEnabled(2, True)
            self.tabs.setCurrentIndex(2)
            # 初始化下拉框选项
            self._init_viz_combos()
            # 自动加载一次地图
            self.update_viz_map()

        elif task_type == 'export_preview':
            QMessageBox.information(self, "成功", "预览数据已成功导出。")

    def update_filter_tab(self, stats_df):
        """用预处理后的统计数据更新筛选标签页。"""
        print("更新筛选标签页UI...")
        
        # 1. 更新顶部的统计信息
        if stats_df is not None and not stats_df.empty:
            block_count = len(stats_df)
            total_links = self.processor.links_df.shape[0]
            total_nodes = self.processor.nodes_df.shape[0]
            self.block_count_label.setText(f"区块数量: {block_count}")
            self.link_count_label.setText(f"路段总数: {total_links}")
            self.node_count_label.setText(f"节点总数: {total_nodes}")
        else:
            self.block_count_label.setText("区块数量: 0")
            self.link_count_label.setText("路段总数: 0")
            self.node_count_label.setText("节点总数: 0")

        # 2. 更新下方的表格
        self.block_table_model.clear()
        # 清除旧的 widgets，防止内存泄漏
        for i in range(self.block_table_view.model().rowCount()):
            self.block_table_view.setIndexWidget(self.block_table_view.model().index(i, 0), None)

        if stats_df is not None and not stats_df.empty:
            headers = ["选择", "区块ID", "路段数", "路段占比", "节点数", "节点占比"]
            self.block_table_model.setHorizontalHeaderLabels(headers)

            for i, row in stats_df.iterrows():
                # 为所有列创建空的 QStandardItem
                placeholder_item = QStandardItem()
                block_id_item = QStandardItem(str(row["区块ID"]))
                link_count_item = QStandardItem(str(row["路段数"]))
                link_percent_item = QStandardItem(str(row["路段占比"])) 
                node_count_item = QStandardItem(str(row["节点数"]))
                node_percent_item = QStandardItem(str(row["节点占比"]))

                items = [placeholder_item, block_id_item, link_count_item, link_percent_item, node_count_item, node_percent_item]
                
                # 设置对齐和不可编辑
                for item in items[1:]: # 从第二列开始
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                    item.setEditable(False)
                placeholder_item.setEditable(False)

                self.block_table_model.appendRow(items)

                # --- 使用 setIndexWidget 来居中复选框 ---
                checkbox = QCheckBox()
                if i == 0:
                    checkbox.setChecked(True)
                
                container_widget = QWidget()
                layout = QHBoxLayout(container_widget)
                layout.addWidget(checkbox)
                layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
                layout.setContentsMargins(0, 0, 0, 0)
                
                index = self.block_table_model.index(i, 0)
                self.block_table_view.setIndexWidget(index, container_widget)
                # ----------------------------------------

            # 设置列宽
            self.block_table_view.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
            for i in range(1, self.block_table_model.columnCount()):
                self.block_table_view.horizontalHeader().setSectionResizeMode(i, QHeaderView.ResizeMode.Stretch)

    def _get_selected_blocks(self):
        selected_block_ids = []
        for i in range(self.block_table_model.rowCount()):
            index = self.block_table_model.index(i, 0)
            container_widget = self.block_table_view.indexWidget(index)
            if container_widget:
                checkbox = container_widget.layout().itemAt(0).widget()
                if checkbox and checkbox.isChecked():
                    block_id = int(self.block_table_model.item(i, 1).text())
                    selected_block_ids.append(block_id)
        return selected_block_ids

    def start_filter_export(self):
        if not self.processor or self.block_table_model.rowCount() == 0:
            QMessageBox.warning(self, "操作无效", "请先成功完成数据预处理步骤。")
            return

        selected_block_ids = self._get_selected_blocks()
        if not selected_block_ids:
            QMessageBox.warning(self, "选择无效", "请至少选择一个区块进行导出。")
            return

        if len(selected_block_ids) > 1:
            reply = QMessageBox.question(self, '确认操作',
                                       "您选择了多个区块，导出的路网可能不连续，是否继续导出？",
                                       QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                       QMessageBox.StandardButton.No)
            if reply == QMessageBox.StandardButton.No:
                return

        output_dir = self.out_input.text().strip()
        if not output_dir:
            QMessageBox.warning(self, "输入错误", "请指定一个有效的输出目录。")
            return

        task_kwargs = {
            'processor': self.processor,
            'output_dir': output_dir,
            'filter_criteria': selected_block_ids
        }

        self.export_filtered_btn.setEnabled(False)
        self.preview_btn.setEnabled(False)
        self.run_btn.setEnabled(False)
        
        self.current_log_widget = self.filter_log_area
        self.current_progress_bar = self.filter_progress
        self.current_log_widget.clear()

        self.current_progress_bar.show()
        self.log("正在启动筛选与导出任务...")

        self.worker = WorkerThread(task_type='filter_and_export', **task_kwargs)
        self.worker.log_signal.connect(self.log)
        self.worker.finished_signal.connect(self.on_finished)
        self.worker.start()

    def start_preview_generation(self):
        if not self.processor or self.block_table_model.rowCount() == 0:
            QMessageBox.warning(self, "操作无效", "请先成功完成数据预处理步骤。")
            return

        selected_block_ids = self._get_selected_blocks()
        if not selected_block_ids:
            QMessageBox.warning(self, "选择无效", "请至少选择一个区块进行预览。")
            return

        self.preview_btn.setEnabled(False)
        self.export_filtered_btn.setEnabled(False)
        self.run_btn.setEnabled(False)
        
        self.current_log_widget = self.filter_log_area
        self.current_progress_bar = self.filter_progress
        self.current_log_widget.clear()
        self.current_progress_bar.show()
        self.log("正在生成预览数据...")

        task_kwargs = {
            'processor': self.processor,
            'filter_criteria': selected_block_ids
        }

        self.worker = WorkerThread(task_type='preview', **task_kwargs)
        self.worker.log_signal.connect(self.log)
        self.worker.finished_signal.connect(self.on_finished)
        self.worker.start()

    def _init_viz_combos(self):
        """初始化可视化下拉框"""
        # 获取所有列
        if self.processor.preview_links_gdf is not None:
            link_cols = [c for c in self.processor.preview_links_gdf.columns if c != 'geometry']
            self.link_color_attr_combo.clear()
            self.link_color_attr_combo.addItems(link_cols)
            # 设置默认值
            default_link_attr = self.processor.get_chn_title_public('道路等级')
            if default_link_attr and default_link_attr in link_cols:
                self.link_color_attr_combo.setCurrentText(default_link_attr)
        
        if self.processor.preview_nodes_gdf is not None:
            node_cols = [c for c in self.processor.preview_nodes_gdf.columns if c != 'geometry']
            self.node_color_attr_combo.clear()
            self.node_color_attr_combo.addItems(node_cols)
             # 设置默认值
            default_node_attr = self.processor.get_chn_title_public('类型', 'node')
            if default_node_attr and default_node_attr in node_cols:
                self.node_color_attr_combo.setCurrentText(default_node_attr)

    def pick_color(self, target):
        color = QColorDialog.getColor()
        if color.isValid():
            hex_color = color.name()
            if target == 'link':
                self.viz_link_color = hex_color
                self.link_color_btn.setStyleSheet(f"background-color: {hex_color}; color: white;")
            elif target == 'node':
                self.viz_node_color = hex_color
                self.node_color_btn.setStyleSheet(f"background-color: {hex_color}; color: white;")

    def update_viz_map(self):
        if not WEB_ENGINE_AVAILABLE:
            if not self.recheck_webengine_and_upgrade_view():
                if isinstance(self.web_view, QLabel):
                    self.web_view.setText(self._webengine_status_text())
                return

        if self.processor.preview_links_gdf is None:
            return

        self.web_view.setHtml("<html><body><h3 align='center'>正在生成地图...</h3></body></html>")
        # 由于 folium 生成可能较慢，这里也可以放线程里，但为了简单先直接跑
        
        try:
            # 0. 获取数据并确保使用 WGS84
            links_gdf = self.processor.preview_links_gdf.copy()
            nodes_gdf = self.processor.preview_nodes_gdf.copy() if self.processor.preview_nodes_gdf is not None else None
            if links_gdf.crs is None:
                links_gdf.set_crs(epsg=4326, inplace=True)
            elif links_gdf.crs.to_epsg() != 4326:
                links_gdf = links_gdf.to_crs(epsg=4326)
            if nodes_gdf is not None:
                if nodes_gdf.crs is None:
                    nodes_gdf.set_crs(epsg=4326, inplace=True)
                elif nodes_gdf.crs.to_epsg() != 4326:
                    nodes_gdf = nodes_gdf.to_crs(epsg=4326)

            # 1. 创建基础地图
            # 计算中心点
            center_lat = 0
            center_lon = 0
            if not links_gdf.empty:
                bounds = links_gdf.total_bounds
                center_lon = (bounds[0] + bounds[2]) / 2
                center_lat = (bounds[1] + bounds[3]) / 2
            
            m = folium.Map(location=[center_lat, center_lon], zoom_start=13, tiles='CartoDB positron')

            # 2. 添加路段
            if self.show_links_cb.isChecked() and not links_gdf.empty:
                color_col = self.link_color_attr_combo.currentText()
                # 根据唯一值生成简单的分类配色
                unique_vals = [str(v) for v in links_gdf[color_col].unique()]
                base_palette = ['#1f77b4','#ff7f0e','#2ca02c','#d62728','#9467bd','#8c564b','#e377c2','#7f7f7f','#bcbd22','#17becf']
                color_map = {val: base_palette[i % len(base_palette)] for i, val in enumerate(sorted(unique_vals))}

                # 使用 GeoJSON 添加，支持 style_function
                folium.GeoJson(
                    links_gdf,
                    name="Links",
                    style_function=lambda feature: {
                        'color': color_map.get(str(feature['properties'].get(color_col)), self.viz_link_color),
                        'weight': 2,
                        'opacity': 0.7
                    },
                    tooltip=folium.GeoJsonTooltip(fields=[color_col], aliases=[f"{color_col}:"])
                ).add_to(m)

            # 3. 添加节点
            if self.show_nodes_cb.isChecked() and nodes_gdf is not None and not nodes_gdf.empty:
                color_col = self.node_color_attr_combo.currentText()
                for _, row in nodes_gdf.iterrows():
                    geom = row["geometry"]
                    val = str(row.get(color_col, ""))
                    if geom is None:
                        continue
                    gtype = getattr(geom, "geom_type", "")
                    if gtype == "Point":
                        lat = geom.y
                        lon = geom.x
                        folium.CircleMarker(
                            location=[lat, lon],
                            radius=3,
                            color=self.viz_node_color,
                            fill=True,
                            fill_color=self.viz_node_color,
                            tooltip=f"{color_col}: {val}"
                        ).add_to(m)
                    elif gtype == "MultiPoint":
                        for pt in geom.geoms:
                            lat = pt.y
                            lon = pt.x
                            folium.CircleMarker(
                                location=[lat, lon],
                                radius=3,
                                color=self.viz_node_color,
                                fill=True,
                                fill_color=self.viz_node_color,
                                tooltip=f"{color_col}: {val}"
                            ).add_to(m)

            folium.LayerControl().add_to(m)

            html = m.get_root().render()
            try:
                self.web_view.setHtml(html, baseUrl=QUrl("https://folium.local/"))
            except Exception:
                temp_file = os.path.join(tempfile.gettempdir(), "osm_preview.html")
                m.save(temp_file)
                self.web_view.setUrl(QUrl.fromLocalFile(temp_file))

        except Exception as e:
            self.web_view.setHtml(f"<html><body><h3 align='center' style='color:red'>生成地图失败: {str(e)}</h3></body></html>")
            print(e)
    
    def _webengine_status_text(self):
        lines = []
        lines.append("未检测到 PyQt6-WebEngine，或导入失败。")
        lines.append("请确认在当前Python环境已安装: pip install PyQt6 PyQt6-WebEngine")
        try:
            pyqt_ver = metadata.version("PyQt6")
        except Exception:
            pyqt_ver = "未安装"
        try:
            web_ver = metadata.version("PyQt6-WebEngine")
        except Exception:
            web_ver = "未安装"
        lines.append(f"PyQt6: {pyqt_ver}")
        lines.append(f"PyQt6-WebEngine: {web_ver}")
        lines.append(f"Python: {sys.version.split()[0]}")
        lines.append(f"解释器: {sys.executable}")
        if WEB_ENGINE_IMPORT_ERROR:
            lines.append(f"导入错误: {repr(WEB_ENGINE_IMPORT_ERROR)}")
        return "\n".join(lines)
    
    def recheck_webengine_and_upgrade_view(self):
        global WEB_ENGINE_AVAILABLE, WEB_ENGINE_IMPORT_ERROR, QWebEngineView
        if WEB_ENGINE_AVAILABLE:
            return True
        try:
            from PyQt6.QtWebEngineWidgets import QWebEngineView as _QEWV
            QWebEngineView = _QEWV
            WEB_ENGINE_AVAILABLE = True
            if isinstance(self.web_view, QLabel):
                new_view = QWebEngineView()
                idx = self.viz_splitter.indexOf(self.web_view)
                if idx != -1:
                    self.viz_splitter.replaceWidget(idx, new_view)
                self.web_view = new_view
            return True
        except Exception as e:
            WEB_ENGINE_IMPORT_ERROR = e
            WEB_ENGINE_AVAILABLE = False
            return False
    
    def show_webengine_diagnostics(self):
        msg = QMessageBox(self)
        msg.setWindowTitle("WebEngine 环境诊断")
        msg.setText(self._webengine_status_text())
        msg.setIcon(QMessageBox.Icon.Information)
        msg.exec()

    def start_preview_export(self):
        output_dir = self.out_input.text().strip()
        if not output_dir:
            QMessageBox.warning(self, "输入错误", "请指定一个有效的输出目录。")
            return
            
        self.export_preview_btn.setEnabled(False)
        self.log("正在导出预览数据...")
        
        task_kwargs = {
            'processor': self.processor,
            'output_dir': output_dir
        }
        
        self.worker = WorkerThread(task_type='export_preview', **task_kwargs)
        self.worker.log_signal.connect(self.log)
        self.worker.finished_signal.connect(self.on_finished)
        self.worker.start()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    app.aboutToQuit.connect(window.save_settings)
    window.show()
    sys.exit(app.exec())
