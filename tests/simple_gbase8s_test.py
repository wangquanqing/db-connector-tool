#!/usr/bin/env python3
"""
GBase 8s JDBC快速连接测试

这个简单的测试脚本用于快速验证GBase 8s JDBC连接是否正常工作。
"""

import os

from sqlalchemy import create_engine, text

# 设置驱动路径
os.environ["CLASSPATH"] = (
    "D:/wangq/Documents/gbasedbtjdbc_3.6.3_3X2_1_377ee9.jar"
)


def quick_gbase8s_test():
    """快速测试GBase 8s JDBC连接"""

    # 修改为您的实际连接参数
    config = {
        "username": "cvicse",
        "password": "Cvicsejszx%402022",
        "host": "localhost",
        "port": "9088",
        "database": "db_station",
        "server": "gbase01"
    }

    # 创建连接URL
    url = (
        f"gbasedbt-sqli://{config['host']}:{config['port']}/{config['database']}:GBASEDBTSERVER={config['server']}"
        f"?user={config['username']}&password={config['password']}"
    )

    # try:
    print("尝试连接GBase 8s数据库...")
    engine = create_engine(url)
    with engine.connect() as conn:
        # 测试基本查询
        result = conn.execute(text("select * from sysusers"))
        test_value = result.fetchall()
        if test_value:
            print("✓ GBase 8s JDBC连接测试成功！ 返回版本信息: {}".format(test_value))
            return True
        else:
            print("✗ 连接测试返回意外结果")
            return False

    # except Exception as e:
    #     print(f"✗ 连接失败: {e}")
    #     print("\n可能的原因:")
    #     print("1. GBase 8s服务器未运行")
    #     print("2. 连接参数不正确") 
    #     print("3. GBase 8s JDBC驱动未在CLASSPATH中")
    #     print("4. 网络或权限问题")
    #     print("5. INFORMIXSERVER名称不正确")
    #     return False


if __name__ == "__main__":
    quick_gbase8s_test()