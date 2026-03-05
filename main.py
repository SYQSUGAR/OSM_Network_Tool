import sys
import os
# 设置 WebEngine 标志：禁用GPU加速以避免崩溃，设置日志级别以减少干扰
os.environ.setdefault("QTWEBENGINE_CHROMIUM_FLAGS", "--disable-gpu --disable-gpu-compositing --disable-software-rasterizer --log-level=3")
os.environ.setdefault("QTWEBENGINE_DISABLE_SANDBOX", "1")

# 将项目根目录添加到 python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.gui import MainWindow
from PyQt6.QtWidgets import QApplication
from PyQt6.QtCore import Qt

def main():
    # ================= 核心修复 1：最稳定的禁用 GPU 方式 =================
    # 直接向 sys.argv 注入参数，确保 WebEngine 进程必定能读取到
    if "--disable-gpu" not in sys.argv:
        sys.argv.extend([
            "--disable-gpu", 
            "--disable-software-rasterizer", 
            "--disable-gpu-compositing",
            "--no-sandbox"
        ])
    # 强制 Qt 使用纯软件渲染
    os.environ["QT_OPENGL"] = "software"
    os.environ["QT_QUICK_BACKEND"] = "software"
    # =================================================================

    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
