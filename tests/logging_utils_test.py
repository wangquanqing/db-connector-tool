"""
LogManager使用示例

展示如何使用LogManager类进行高级日志管理。
"""

import os
import time
from db_connector.utils.logging_utils import LogManager


def basic_usage_example():
    """基础用法示例"""
    print("=== 基础用法示例 ===")
    
    # 1. 快速设置日志系统
    log_manager = LogManager.quick_setup("my_app", "DEBUG")
    logger = log_manager.logger
    
    logger.debug("这是调试信息")
    logger.info("应用程序启动")
    logger.warning("这是一个警告")
    logger.error("发生了一个错误")
    print(logger.name, logger.level, logger.handlers)
    
    return log_manager


def advanced_usage_example():
    """高级用法示例"""
    print("\n=== 高级用法示例 ===")
    
    # 1. 创建LogManager实例
    log_manager = LogManager("advanced_app")
    
    # 2. 配置基础日志系统
    logger = log_manager.setup(
        level="INFO",
        log_to_console=True,
        log_to_file=True,
        max_file_size=5 * 1024 * 1024,  # 5MB
        backup_count=3
    )
    
    # 3. 添加额外的文件handler（调试日志）
    log_manager.add_file_handler(
        "debug.log",
        level="DEBUG",
        max_size=2 * 1024 * 1024,  # 2MB
        backup_count=2
    )
    
    # 4. 添加基于时间的轮转日志（每小时轮转）
    log_manager.add_file_handler(
        "hourly.log",
        when="H",  # 每小时轮转
        backup_count=24  # 保留24小时日志
    )
    
    # 5. 使用不同级别的日志
    logger.debug("调试信息 - 通常用于开发阶段")
    logger.info("信息日志 - 应用程序运行状态")
    logger.warning("警告信息 - 需要注意的情况")
    logger.error("错误信息 - 需要处理的问题")
    
    # 6. 获取日志系统信息
    loggers_info = log_manager.get_loggers_info()
    print(f"\n当前日志系统信息:")
    for name, info in loggers_info.items():
        if name.startswith("advanced_app"):
            print(f"  {name}: 级别={info['level']}, handlers={info['handlers']}")
    
    return log_manager


def dynamic_level_management():
    """动态级别管理示例"""
    print("\n=== 动态级别管理示例 ===")
    
    log_manager = LogManager("dynamic_app")
    logger = log_manager.setup(level="WARNING")
    
    logger.debug("这条消息不会显示（级别为WARNING）")
    logger.info("这条消息也不会显示")
    logger.warning("警告消息会显示")
    
    # 动态添加DEBUG级别的文件handler
    log_manager.add_file_handler("dynamic_debug.log", level="DEBUG")
    
    # 现在DEBUG级别的消息会记录到文件，但控制台仍然只显示WARNING及以上
    logger.debug("这条消息会记录到文件，但控制台不显示")
    logger.info("这条消息也会记录到文件")
    
    return log_manager


def error_handling_example():
    """错误处理示例"""
    print("\n=== 错误处理示例 ===")
    
    try:
        # 尝试使用无效的日志级别
        log_manager = LogManager("error_app")
        log_manager.setup(level="INVALID_LEVEL")  # 这会抛出ValueError
    except ValueError as e:
        print(f"捕获到预期错误: {e}")
    
    # 正确的用法
    log_manager = LogManager("error_app")
    logger = log_manager.setup(level="ERROR")
    
    # 模拟异常处理
    try:
        result = 1 / 0
    except Exception as e:
        logger.exception("除零错误发生")
    
    return log_manager


def performance_monitoring_example():
    """性能监控示例"""
    print("\n=== 性能监控示例 ===")
    
    log_manager = LogManager("performance_app")
    logger = log_manager.setup(level="INFO")
    
    # 添加性能监控日志
    log_manager.add_file_handler("performance.log", level="DEBUG")
    
    # 模拟性能监控
    start_time = time.time()
    
    # 模拟一些操作
    for i in range(5):
        logger.debug(f"操作 {i} 开始")
        time.sleep(0.1)  # 模拟处理时间
        logger.debug(f"操作 {i} 完成")
    
    end_time = time.time()
    duration = end_time - start_time
    
    logger.info(f"操作总耗时: {duration:.2f}秒")
    
    return log_manager


def cleanup_example():
    """清理操作示例"""
    print("\n=== 清理操作示例 ===")
    
    log_manager = LogManager("cleanup_app")
    logger = log_manager.setup(level="INFO")
    
    # 添加多个handler
    log_manager.add_file_handler("temp1.log")
    log_manager.add_file_handler("temp2.log")
    
    logger.info("添加了多个handler")
    
    # 获取handler数量
    initial_handlers = len(log_manager._handlers)
    print(f"清理前handler数量: {initial_handlers}")
    
    # 清理所有handler
    log_manager.cleanup()
    
    final_handlers = len(log_manager._handlers)
    print(f"清理后handler数量: {final_handlers}")
    
    return log_manager


def main():
    """主函数 - 运行所有示例"""
    print("LogManager使用示例程序")
    print("=" * 50)
    
    managers = []
    
    try:
        # 运行各个示例
        managers.append(basic_usage_example())
        managers.append(advanced_usage_example())
        managers.append(dynamic_level_management())
        managers.append(error_handling_example())
        managers.append(performance_monitoring_example())
        managers.append(cleanup_example())
        
        print("\n" + "=" * 50)
        print("所有示例执行完成！")
        
        # 显示创建的日志文件
        log_files = [f for f in os.listdir('.') if f.endswith('.log')]
        if log_files:
            print(f"\n生成的日志文件: {log_files}")
        
    except Exception as e:
        print(f"示例执行过程中发生错误: {e}")
    
    finally:
        # 可选：清理所有创建的LogManager
        for manager in managers:
            try:
                manager.cleanup()
            except:
                pass  # 忽略清理过程中的错误


if __name__ == "__main__":
    main()