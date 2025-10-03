"""
环境设置脚本
"""

import os
import sys
import subprocess
from pathlib import Path

def setup_environment():
    """设置开发环境"""
    
    print("🚀 开始设置 DB Connector 开发环境...")
    
    # 检查Python版本
    python_version = sys.version_info
    if python_version.major < 3 or (python_version.major == 3 and python_version.minor < 8):
        print("❌ 需要 Python 3.8 或更高版本")
        sys.exit(1)
    
    print(f"✅ Python 版本: {python_version.major}.{python_version.minor}.{python_version.micro}")
    
    # 安装依赖
    print("
📦 安装依赖...")
    requirements_files = [
        'requirements.txt',
        'requirements-dev.txt'
    ]
    
    for req_file in requirements_files:
        if Path(req_file).exists():
            try:
                subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', req_file])
                print(f"✅ 已安装依赖: {req_file}")
            except subprocess.CalledProcessError as e:
                print(f"❌ 安装依赖失败 {req_file}: {e}")
                sys.exit(1)
    
    # 创建必要的目录
    print("
📁 创建目录结构...")
    directories = [
        'logs',
        'tests/integration',
        'examples/config'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✅ 目录已创建: {directory}")
    
    # 运行测试
    print("
🧪 运行测试...")
    try:
        subprocess.check_call([sys.executable, '-m', 'pytest', 'tests/', '-v'])
        print("✅ 所有测试通过!")
    except subprocess.CalledProcessError as e:
        print(f"❌ 测试失败: {e}")
        sys.exit(1)
    
    print("
🎉 环境设置完成!")
    print("
下一步:")
    print("1. 查看 examples/ 目录中的使用示例")
    print("2. 运行 'python examples/basic_usage.py' 测试基本功能")
    print("3. 查看 README.md 了解详细文档")

if __name__ == "__main__":
    setup_environment()
