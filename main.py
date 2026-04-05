#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志智能清洗与统计工具 - 程序入口 (Main)
负责调度所有模块、启动任务
"""

import sys
import os
from datetime import datetime
from typing import List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import get_config
from log_reader import get_log_reader, ReadResult
from data_processor import get_data_processor, ProcessResult
from file_writer import get_file_writer


class LogAnalyzer:
    """
    日志分析器类
    使用PascalCase命名法
    """
    
    def __init__(self):
        self._config = get_config()
        self._reader = get_log_reader()
        self._processor = get_data_processor()
        self._writer = get_file_writer()
    
    def run(self) -> int:
        start_time = datetime.now()
        
        try:
            self._print_banner()
            
            if not self._check_environment():
                return 1
            
            read_results = self._reader.read_directory()
            
            if not read_results:
                print("[ERROR] 没有找到日志文件")
                return 1
            
            all_lines = self._merge_lines(read_results)
            
            if not all_lines:
                print("[ERROR] 没有有效的日志行")
                return 1
            
            process_result = self._processor.process_lines(all_lines)
            
            statistics = self._processor.generate_statistics(process_result)
            
            input_files = [r.file_path for r in read_results if r.success]
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            output_files = self._write_outputs(
                statistics, 
                process_result.exception_logs, 
                input_files, 
                execution_time
            )
            
            self._print_summary(process_result, output_files, execution_time, input_files)
            
            return 0
            
        except KeyboardInterrupt:
            print("\n[INFO] 程序被用户中断")
            return 130
        except Exception as e:
            print(f"[ERROR] 程序执行失败: {str(e)}")
            return 1
    
    def _print_banner(self) -> None:
        print("=" * 60)
        print("       日志智能清洗与统计工具 (Log Analyzer)")
        print("                版本 1.0.0")
        print("=" * 60)
    
    def _check_environment(self) -> bool:
        input_dir = self._config.get_input_dir()
        if not os.path.exists(input_dir):
            print(f"[ERROR] 输入目录不存在: {input_dir}")
            print(f"        请创建该目录或将日志文件放入该目录")
            return False
        
        output_dir = self._config.get_output_dir()
        if not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir, exist_ok=True)
            except Exception as e:
                print(f"[ERROR] 无法创建输出目录: {output_dir}")
                return False
        
        return True
    
    def _merge_lines(self, read_results: List[ReadResult]) -> List[str]:
        all_lines = []
        for result in read_results:
            if result.success:
                all_lines.extend(result.lines)
        return all_lines
    
    def _write_outputs(self, statistics: dict, exception_logs: list, 
                       input_files: list, execution_time: float) -> dict:
        output_files = {}
        
        md_content = self._writer.generate_markdown_report(
            statistics, exception_logs, input_files, execution_time
        )
        md_path = self._writer.generate_output_filename(extension=".md")
        if self._writer.write_text(md_path, md_content):
            output_files["markdown"] = md_path
        
        json_data = self._writer.generate_json_statistics(
            statistics, exception_logs, input_files, execution_time
        )
        json_path = self._writer.generate_output_filename(extension=".json")
        if self._writer.write_json(json_path, json_data):
            output_files["json"] = json_path
        
        if exception_logs:
            exc_content = self._writer.generate_exception_archive(exception_logs)
            exc_path = self._writer.generate_output_filename(suffix="exceptions", extension=".log")
            if self._writer.write_text(exc_path, exc_content):
                output_files["exceptions"] = exc_path
        
        return output_files
    
    def _print_summary(self, result: ProcessResult, output_files: dict, 
                       execution_time: float, input_files: list) -> None:
        print("\n" + "=" * 60)
        print("处理结果摘要")
        print("=" * 60)
        
        print(f"[OK] 执行状态: 成功")
        print(f"[TIME] 执行时间: {execution_time:.2f} 秒")
        print(f"[FILES] 输入文件数: {len(input_files)}")
        print(f"[TOTAL] 总行数: {result.total_lines}")
        print(f"[VALID] 有效行数: {result.valid_lines}")
        print(f"[ERROR] 异常数量: {result.exception_lines}")
        
        if result.total_lines > 0:
            valid_rate = (result.valid_lines / result.total_lines) * 100
            print(f"[RATE] 有效率: {valid_rate:.2f}%")
        
        if output_files:
            print("\n[OUTPUT] 生成文件:")
            for file_type, file_path in output_files.items():
                print(f"   - {file_type}: {file_path}")
        
        print("=" * 60)


def main() -> int:
    analyzer = LogAnalyzer()
    return analyzer.run()


if __name__ == "__main__":
    sys.exit(main())
