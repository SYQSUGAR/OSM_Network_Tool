from PyQt6.QtWidgets import QApplication
from PyQt6.QtGui import QPalette, QColor
from PyQt6.QtCore import Qt

def apply_theme(app: QApplication, theme_name='dark'):
    """
    应用主题
    :param app: QApplication 实例
    :param theme_name: 'dark' 或 'light'
    """
    app.setStyle("Fusion")
    
    if theme_name == 'dark':
        _apply_dark_theme(app)
    else:
        _apply_light_theme(app)

def _apply_dark_theme(app: QApplication):
    """
    强制应用深色主题
    """
    dark_palette = QPalette()
    
    # 基础颜色定义 (Material Design Dark 风格参考)
    color_window = QColor(53, 53, 53)
    color_window_text = QColor(255, 255, 255)
    color_base = QColor(42, 42, 42)  # 输入框背景等
    color_alternate_base = QColor(66, 66, 66)
    color_text = QColor(255, 255, 255)
    color_button = QColor(53, 53, 53)
    color_button_text = QColor(255, 255, 255)
    color_highlight = QColor(42, 130, 218) # 选中色 (蓝色)
    color_highlighted_text = QColor(255, 255, 255)
    color_link = QColor(42, 130, 218)

    dark_palette.setColor(QPalette.ColorRole.Window, color_window)
    dark_palette.setColor(QPalette.ColorRole.WindowText, color_window_text)
    dark_palette.setColor(QPalette.ColorRole.Base, color_base)
    dark_palette.setColor(QPalette.ColorRole.AlternateBase, color_alternate_base)
    dark_palette.setColor(QPalette.ColorRole.ToolTipBase, color_window_text)
    dark_palette.setColor(QPalette.ColorRole.ToolTipText, color_window)
    dark_palette.setColor(QPalette.ColorRole.Text, color_text)
    dark_palette.setColor(QPalette.ColorRole.Button, color_button)
    dark_palette.setColor(QPalette.ColorRole.ButtonText, color_button_text)
    dark_palette.setColor(QPalette.ColorRole.BrightText, Qt.GlobalColor.red)
    dark_palette.setColor(QPalette.ColorRole.Link, color_link)
    dark_palette.setColor(QPalette.ColorRole.Highlight, color_highlight)
    dark_palette.setColor(QPalette.ColorRole.HighlightedText, color_highlighted_text)
    
    # Disabled 状态
    color_disabled_text = QColor(127, 127, 127)
    dark_palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.WindowText, color_disabled_text)
    dark_palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Text, color_disabled_text)
    dark_palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.ButtonText, color_disabled_text)
    dark_palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Highlight, QColor(80, 80, 80))
    dark_palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.HighlightedText, color_disabled_text)

    app.setPalette(dark_palette)

    # 深色 QSS
    qss = """
    /* 全局字体与基础背景 */
    QWidget {
        background-color: #353535;
        color: #ffffff;
        font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
        font-size: 10pt;
    }
    
    /* 禁用状态文本颜色 */
    QWidget:disabled {
        color: #808080;
    }

    /* 主窗口与对话框 */
    QMainWindow, QDialog {
        background-color: #353535;
    }

    /* 文本输入框 */
    QLineEdit, QTextEdit, QPlainTextEdit, QSpinBox, QDoubleSpinBox {
        background-color: #2a2a2a;
        color: #ffffff;
        border: 1px solid #555555;
        border-radius: 4px;
        padding: 4px;
        selection-background-color: #2a82da;
        selection-color: #ffffff;
    }
    
    QLineEdit:focus, QTextEdit:focus, QPlainTextEdit:focus, QComboBox:focus {
        border: 1px solid #2a82da;
    }
    
    /* 只读/禁用输入框 */
    QLineEdit:read-only, QTextEdit:read-only {
        background-color: #252525;
        color: #aaaaaa;
    }

    /* 按钮 */
    QPushButton {
        background-color: #454545;
        color: #ffffff;
        border: 1px solid #666666;
        border-radius: 4px;
        padding: 6px 12px;
        min-height: 20px;
    }
    QPushButton:hover {
        background-color: #555555;
        border: 1px solid #777777;
    }
    QPushButton:pressed {
        background-color: #2a82da;
        border-color: #2a82da;
    }
    QPushButton:disabled {
        background-color: #2a2a2a;
        border: 1px solid #3a3a3a;
        color: #666666;
    }

    /* 工具按钮 */
    QToolButton {
        background-color: transparent;
        border: none;
        border-radius: 4px;
    }
    QToolButton:hover {
        background-color: #454545;
    }
    QToolButton:pressed {
        background-color: #2a2a2a;
    }

    /* 下拉框 */
    QComboBox {
        background-color: #2a2a2a;
        color: #ffffff;
        border: 1px solid #555555;
        border-radius: 4px;
        padding: 4px;
        padding-right: 20px; /* 箭头空间 */
    }
    QComboBox:disabled {
        background-color: #252525;
        color: #aaaaaa;
    }
    QComboBox::drop-down {
        subcontrol-origin: padding;
        subcontrol-position: top right;
        width: 20px;
        border-left-width: 0px;
        border-top-right-radius: 3px;
        border-bottom-right-radius: 3px;
    }
    QComboBox QAbstractItemView {
        background-color: #2a2a2a;
        color: #ffffff;
        selection-background-color: #2a82da;
        selection-color: #ffffff;
        border: 1px solid #555555;
    }

    /* 列表/表格视图 */
    QTableView, QTableWidget, QListView, QListWidget, QTreeView {
        background-color: #2a2a2a;
        color: #ffffff;
        border: 1px solid #555555;
        gridline-color: #444444;
        selection-background-color: #2a82da;
        selection-color: #ffffff;
        alternate-background-color: #353535;
    }
    
    QHeaderView::section {
        background-color: #454545;
        color: #ffffff;
        padding: 4px;
        border: 1px solid #555555;
        font-weight: bold;
    }
    
    QTableView QTableCornerButton::section {
        background-color: #454545;
        border: 1px solid #555555;
    }

    /* 滚动条 - 垂直 */
    QScrollBar:vertical {
        border: none;
        background: #2a2a2a;
        width: 10px;
        margin: 0px;
    }
    QScrollBar::handle:vertical {
        background: #555555;
        min-height: 20px;
        border-radius: 5px;
    }
    QScrollBar::handle:vertical:hover {
        background: #666666;
    }
    QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
        height: 0px;
    }
    
    /* 滚动条 - 水平 */
    QScrollBar:horizontal {
        border: none;
        background: #2a2a2a;
        height: 10px;
        margin: 0px;
    }
    QScrollBar::handle:horizontal {
        background: #555555;
        min-width: 20px;
        border-radius: 5px;
    }
    QScrollBar::handle:horizontal:hover {
        background: #666666;
    }
    QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
        width: 0px;
    }

    /* 选项卡 */
    QTabWidget::pane {
        border: 1px solid #555555;
        background: #353535;
        top: -1px; 
    }
    QTabBar::tab {
        background: #2a2a2a;
        color: #aaaaaa;
        border: 1px solid #555555;
        padding: 6px 12px;
        margin-right: 2px;
        border-top-left-radius: 4px;
        border-top-right-radius: 4px;
    }
    QTabBar::tab:selected {
        background: #454545; /* 选中更亮 */
        color: #ffffff;
        border-bottom-color: #454545; /* 连接感 */
    }
    QTabBar::tab:hover {
        background: #3a3a3a;
    }

    /* 分组框 */
    QGroupBox {
        border: 1px solid #555555;
        border-radius: 6px;
        margin-top: 10px; 
        padding-top: 15px;
    }
    QGroupBox::title {
        subcontrol-origin: margin;
        subcontrol-position: top left;
        padding: 0 5px;
        left: 10px;
        color: #2a82da; 
        font-weight: bold;
    }

    /* 菜单和菜单栏 */
    QMenuBar {
        background-color: #353535;
        color: #ffffff;
        border-bottom: 1px solid #555555;
    }
    QMenuBar::item {
        background: transparent;
        padding: 4px 8px;
    }
    QMenuBar::item:selected {
        background: #454545;
    }
    
    QMenu {
        background-color: #353535; 
        color: #ffffff;
        border: 1px solid #555555;
    }
    QMenu::item {
        padding: 6px 24px 6px 10px; /* Right padding for shortcuts */
        background-color: transparent;
    }
    QMenu::item:selected {
        background-color: #2a82da;
        color: #ffffff;
    }
    QMenu::separator {
        height: 1px;
        background: #555555;
        margin: 4px 0px;
    }

    /* 消息框 */
    QMessageBox {
        background-color: #353535;
    }
    QMessageBox QLabel {
        color: #ffffff;
    }

    /* 进度条 */
    QProgressBar {
        border: 1px solid #555555;
        border-radius: 4px;
        text-align: center;
        background-color: #2a2a2a;
        color: #ffffff;
    }
    QProgressBar::chunk {
        background-color: #2a82da;
    }
    
    /* CheckBox & RadioButton */
    QCheckBox, QRadioButton {
        color: #ffffff;
        spacing: 5px;
    }
    
    /* 分割线 */
    QSplitter::handle {
        background-color: #454545;
    }
    
    /* WebEngineView (如果有背景) */
    QWebEngineView {
        background-color: #2a2a2a;
    }
    """
    
    app.setStyleSheet(qss)

def _apply_light_theme(app: QApplication):
    """
    应用浅色主题 (标准 Qt 风格 + 微调)
    """
    light_palette = QPalette()
    
    # 基础颜色定义 (标准 Light)
    color_window = QColor(240, 240, 240)
    color_window_text = QColor(0, 0, 0)
    color_base = QColor(255, 255, 255)
    color_alternate_base = QColor(245, 245, 245)
    color_text = QColor(0, 0, 0)
    color_button = QColor(240, 240, 240)
    color_button_text = QColor(0, 0, 0)
    color_highlight = QColor(0, 120, 215)
    color_highlighted_text = QColor(255, 255, 255)
    color_link = QColor(0, 102, 204)

    light_palette.setColor(QPalette.ColorRole.Window, color_window)
    light_palette.setColor(QPalette.ColorRole.WindowText, color_window_text)
    light_palette.setColor(QPalette.ColorRole.Base, color_base)
    light_palette.setColor(QPalette.ColorRole.AlternateBase, color_alternate_base)
    light_palette.setColor(QPalette.ColorRole.ToolTipBase, QColor(255, 255, 220))
    light_palette.setColor(QPalette.ColorRole.ToolTipText, QColor(0, 0, 0))
    light_palette.setColor(QPalette.ColorRole.Text, color_text)
    light_palette.setColor(QPalette.ColorRole.Button, color_button)
    light_palette.setColor(QPalette.ColorRole.ButtonText, color_button_text)
    light_palette.setColor(QPalette.ColorRole.Link, color_link)
    light_palette.setColor(QPalette.ColorRole.Highlight, color_highlight)
    light_palette.setColor(QPalette.ColorRole.HighlightedText, color_highlighted_text)
    
    # Disabled
    color_disabled = QColor(160, 160, 160)
    light_palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.WindowText, color_disabled)
    light_palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Text, color_disabled)
    light_palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.ButtonText, color_disabled)

    app.setPalette(light_palette)

    # 浅色 QSS
    qss = """
    /* 全局字体 */
    QWidget {
        font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
        font-size: 10pt;
        color: #000000;
        background-color: #f0f0f0;
    }
    
    /* 禁用状态文本 */
    QWidget:disabled {
        color: #a0a0a0;
    }

    /* 主窗口与对话框 */
    QMainWindow, QDialog {
        background-color: #f0f0f0;
    }

    /* 文本输入框 */
    QLineEdit, QTextEdit, QPlainTextEdit, QSpinBox, QDoubleSpinBox {
        background-color: #ffffff;
        color: #000000;
        border: 1px solid #cccccc;
        border-radius: 4px;
        padding: 4px;
    }
    
    QLineEdit:focus, QTextEdit:focus, QPlainTextEdit:focus, QComboBox:focus {
        border: 1px solid #0078d7;
    }
    
    QLineEdit:read-only, QTextEdit:read-only {
        background-color: #e5e5e5;
        color: #555555;
    }

    /* 按钮 */
    QPushButton {
        background-color: #e1e1e1;
        color: #000000;
        border: 1px solid #adadad;
        border-radius: 4px;
        padding: 6px 12px;
        min-height: 20px;
    }
    QPushButton:hover {
        background-color: #e5f1fb;
        border: 1px solid #0078d7;
    }
    QPushButton:pressed {
        background-color: #cce4f7;
        border-color: #005499;
    }
    QPushButton:disabled {
        background-color: #f0f0f0;
        border: 1px solid #d9d9d9;
        color: #a0a0a0;
    }

    /* 工具按钮 */
    QToolButton {
        background-color: transparent;
        border: none;
        border-radius: 4px;
    }
    QToolButton:hover {
        background-color: #e5f1fb;
        border: 1px solid #a0a0a0;
    }

    /* 下拉框 */
    QComboBox {
        background-color: #ffffff;
        color: #000000;
        border: 1px solid #cccccc;
        border-radius: 4px;
        padding: 4px;
        padding-right: 20px;
    }
    QComboBox::drop-down {
        subcontrol-origin: padding;
        subcontrol-position: top right;
        width: 20px;
        border-left-width: 0px;
    }
    QComboBox QAbstractItemView {
        background-color: #ffffff;
        color: #000000;
        selection-background-color: #0078d7;
        selection-color: #ffffff;
        border: 1px solid #cccccc;
    }

    /* 列表/表格视图 */
    QTableView, QTableWidget, QListView, QListWidget, QTreeView {
        background-color: #ffffff;
        color: #000000;
        border: 1px solid #cccccc;
        gridline-color: #eeeeee;
        selection-background-color: #0078d7;
        selection-color: #ffffff;
        alternate-background-color: #f9f9f9;
    }
    
    QHeaderView::section {
        background-color: #e1e1e1;
        color: #000000;
        padding: 4px;
        border: 1px solid #cccccc;
        font-weight: bold;
    }

    /* 滚动条 */
    QScrollBar:vertical {
        border: none;
        background: #f0f0f0;
        width: 10px;
        margin: 0px;
    }
    QScrollBar::handle:vertical {
        background: #cdcdcd;
        min-height: 20px;
        border-radius: 5px;
    }
    QScrollBar::handle:vertical:hover {
        background: #a6a6a6;
    }
    
    QScrollBar:horizontal {
        border: none;
        background: #f0f0f0;
        height: 10px;
        margin: 0px;
    }
    QScrollBar::handle:horizontal {
        background: #cdcdcd;
        min-width: 20px;
        border-radius: 5px;
    }
    QScrollBar::handle:horizontal:hover {
        background: #a6a6a6;
    }

    /* 选项卡 */
    QTabWidget::pane {
        border: 1px solid #cccccc;
        background: #f0f0f0;
        top: -1px;
    }
    QTabBar::tab {
        background: #e1e1e1;
        color: #000000;
        border: 1px solid #cccccc;
        padding: 6px 12px;
        margin-right: 2px;
        border-top-left-radius: 4px;
        border-top-right-radius: 4px;
    }
    QTabBar::tab:selected {
        background: #ffffff;
        border-bottom-color: #ffffff;
    }
    QTabBar::tab:hover {
        background: #f2f2f2;
    }

    /* 分组框 */
    QGroupBox {
        border: 1px solid #cccccc;
        border-radius: 6px;
        margin-top: 10px; 
        padding-top: 15px;
    }
    QGroupBox::title {
        subcontrol-origin: margin;
        subcontrol-position: top left;
        padding: 0 5px;
        left: 10px;
        color: #0078d7;
        font-weight: bold;
    }

    /* 菜单 */
    QMenuBar {
        background-color: #f0f0f0;
        color: #000000;
        border-bottom: 1px solid #cccccc;
    }
    QMenuBar::item:selected {
        background: #cce4f7;
    }
    
    QMenu {
        background-color: #ffffff; 
        color: #000000;
        border: 1px solid #cccccc;
    }
    QMenu::item {
        padding: 6px 24px 6px 10px;
        background-color: transparent;
    }
    QMenu::item:selected {
        background-color: #0078d7;
        color: #ffffff;
    }
    
    /* 进度条 */
    QProgressBar {
        border: 1px solid #cccccc;
        border-radius: 4px;
        text-align: center;
        background-color: #ffffff;
        color: #000000;
    }
    QProgressBar::chunk {
        background-color: #0078d7;
    }
    
    /* 分割线 */
    QSplitter::handle {
        background-color: #cccccc;
    }
    """
    app.setStyleSheet(qss)
