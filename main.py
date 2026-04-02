#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志智能清洗与统计工具 - 程序入口 (Main)
负责调度所有模块、启动任务
"""

import sys
import os
import argparse
from datetime import datetime
from typing import Optional

# 确保能导入本地模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config_manager import get_config_manager
from logger_system import get_logger
from data_pipeline import get_data_pipeline, PipelineResult
from exporter import get_report_manager


class LogAnalyzerApp:
    """
    日志分析器应用程序类
    使用PascalCase命名法
    程序主入口，负责调度所有模块
    """
    
    def __init__(self):
        """
        初始化应用程序
        """
        self._config = get_config_manager()
        self._logger = get_logger()
        self._pipeline = get_data_pipeline()
        self._report_manager = get_report_manager()
        self._start_time: Optional[datetime] = None
    
    def run(self, input_dir: str = None) -> int:
        """
        运行日志分析任务
        
        参数:
            input_dir: 输入目录，默认使用配置中的路径
        
        返回:
            退出码 (0=成功, 1=失败)
        """
        self._start_time = datetime.now()
        
        try:
            # 打印程序信息
            self._print_banner()
            
            # 检查环境
            if not self._check_environment():
                return 1
            
            # 执行数据处理流水线
            result = self._pipeline.execute(input_dir)
            
            # 生成报告
            if result.success:
                output_files = self._report_manager.generate_reports(result)
                result.output_files = output_files
            
            # 打印结果摘要
            self._print_summary(result)
            
            return 0 if result.success else 1
            
        except KeyboardInterrupt:
            self._logger.info("\n程序被用户中断")
            return 130
        except Exception as e:
            self._logger.exception("程序执行失败")
            return 1
    
    def _print_banner(self) -> None:
        """
        打印程序横幅
        私有方法，使用snake_case命名法
        """
        banner = """
╔══════════════════════════════════════════════════════════════╗
║           日志智能清洗与统计工具 (Log Analyzer)               ║
║                    版本 1.0.0                                 ║
╚══════════════════════════════════════════════════════════════╝
        """
        print(banner)
        self._logger.info("程序启动")
    
    def _check_environment(self) -> bool:
        """
        检查运行环境
        
        返回:
            环境是否正常
        """
        self._logger.info("检查运行环境...")
        
        # 检查输入目录
        input_dir = self._config.get_input_dir()
        if not os.path.exists(input_dir):
            self._logger.error(f"输入目录不存在: {input_dir}")
            print(f"❌ 错误: 输入目录不存在: {input_dir}")
            print(f"   请创建该目录或将日志文件放入该目录")
            return False
        
        # 检查输出目录
        output_dir = self._config.get_output_dir()
        if not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir, exist_ok=True)
                self._logger.info(f"创建输出目录: {output_dir}")
            except Exception as e:
                self._logger.error(f"无法创建输出目录: {output_dir} - {str(e)}")
                print(f"❌ 错误: 无法创建输出目录: {output_dir}")
                return False
        
        # 检查配置文件
        if not os.path.exists("./settings.yaml"):
            self._logger.warning("配置文件不存在: ./settings.yaml")
            print("[!] 警告: 配置文件不存在，将使用默认配置")
        
        self._logger.info("环境检查通过")
        return True
    
    def _print_summary(self, result: PipelineResult) -> None:
        """
        打印结果摘要
        
        参数:
            result: 流水线处理结果
        """
        print("\n" + "=" * 60)
        print("处理结果摘要")
        print("=" * 60)
        
        if result.success:
            print(f"[OK] 执行状态: 成功")
        else:
            print(f"[FAIL] 执行状态: 失败")
        
        print(f"[TIME] 执行时间: {result.execution_time:.2f} 秒")
        print(f"[FILES] 输入文件数: {len(result.input_files)}")
        print(f"[TOTAL] 总行数: {result.total_lines}")
        print(f"[VALID] 有效行数: {result.valid_lines}")
        print(f"[ERROR] 异常数量: {result.exception_count}")
        
        if result.total_lines > 0:
            valid_rate = (result.valid_lines / result.total_lines) * 100
            print(f"[RATE] 有效率: {valid_rate:.2f}%")
        
        # 输出文件
        if result.output_files:
            print("\n[OUTPUT] 生成文件:")
            for file_type, file_path in result.output_files.items():
                print(f"   - {file_type}: {file_path}")
        
        # 错误信息
        if result.errors:
            print("\n[ERRORS] 错误信息:")
            for error in result.errors:
                print(f"   - {error}")
        
        print("=" * 60)


def parse_arguments() -> argparse.Namespace:
    """
    解析命令行参数
    
    返回:
        解析后的参数
    """
    parser = argparse.ArgumentParser(
        description="日志智能清洗与统计工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python main.py                    # 使用默认配置运行
  python main.py -i ./data/input    # 指定输入目录
  python main.py -v                 # 显示详细日志
        """
    )
    
    parser.add_argument(
        "-i", "--input",
        type=str,
        default=None,
        help="输入目录路径 (默认: ./data/input)"
    )
    
    parser.add_argument(
        "-c", "--config",
        type=str,
        default="./settings.yaml",
        help="配置文件路径 (默认: ./settings.yaml)"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="显示详细日志"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 1.0.0"
    )
    
    return parser.parse_args()


def main() -> int:
    """
    程序主入口
    
    返回:
        退出码
    """
    # 解析命令行参数
    args = parse_arguments()
    
    # 创建并运行应用程序
    app = LogAnalyzerApp()
    exit_code = app.run(args.input)
    
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
