import sys
import os
os.environ.setdefault("QTWEBENGINE_CHROMIUM_FLAGS", "--disable-gpu --disable-gpu-compositing --disable-software-rasterizer")
os.environ.setdefault("QTWEBENGINE_DISABLE_SANDBOX", "1")

# 将项目根目录添加到 python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.gui import MainWindow
from PyQt6.QtWidgets import QApplication
from PyQt6.QtCore import Qt

def main():
    QApplication.setAttribute(Qt.ApplicationAttribute.AA_UseSoftwareOpenGL, True)
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
