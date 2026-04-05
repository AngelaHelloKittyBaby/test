import os
import sys
from typing import List

from config import get_config
from log_reader import get_log_reader
from data_processor import get_data_processor
from file_writer import get_file_writer


class LogAnalyzer:
    def __init__(self):
        self.config = get_config()
        self.log_reader = get_log_reader()
        self.data_processor = get_data_processor()
        self.file_writer = get_file_writer()

    def _check_environment(self):
        input_dir = self.config.get_input_dir()
        
        if not os.path.exists(input_dir):
            os.makedirs(input_dir, exist_ok=True)
            print(f"[警告] 输入目录不存在，已创建: {input_dir}")
            print("[提示] 请将日志文件放入 data/input/ 目录后重新运行")
            return False
        
        log_files = self.log_reader._get_log_files(input_dir)
        if not log_files:
            print(f"[警告] 输入目录中未找到 .log 文件: {input_dir}")
            print("[提示] 请将日志文件放入 data/input/ 目录后重新运行")
            return False
        
        return True

    def _merge_lines(self, read_results: List) -> List[str]:
        all_lines = []
        for result in read_results:
            all_lines.extend(result.lines)
        return all_lines

    def _write_outputs(self, statistics, error_logs, warning_logs):
        print("\n[输出] 正在生成报告文件...")
        
        json_path = self.file_writer.write_json_report(statistics)
        print(f"  [OK] JSON统计文件: {json_path}")
        
        md_path = self.file_writer.write_markdown_report(statistics)
        print(f"  [OK] Markdown分析报告: {md_path}")
        
        exception_path = self.file_writer.write_error_logs(error_logs, warning_logs)
        print(f"  [OK] 异常日志归档: {exception_path}")

    def _print_summary(self, statistics):
        summary = statistics['summary']
        
        print("\n" + "=" * 60)
        print("处理完成统计")
        print("=" * 60)
        print(f"  总行数: {summary['total_lines']:,}")
        print(f"  有效行数: {summary['valid_lines']:,}")
        print(f"  无效行数: {summary['invalid_lines']:,}")
        print(f"  错误率: {summary['error_rate']}%")
        print(f"  ERROR数量: {statistics['error_count']:,}")
        print(f"  WARNING数量: {statistics['warning_count']:,}")
        print("=" * 60)

    def run(self):
        print("=" * 60)
        print("本地日志智能清洗与统计工具")
        print("=" * 60)

        try:
            if not self._check_environment():
                return

            print("\n[读取] 正在扫描并读取日志文件...")
            read_results = self.log_reader.read_directory()
            
            total_files = len(read_results)
            total_lines = sum(r.total_lines for r in read_results)
            print(f"  [OK] 已处理 {total_files} 个文件，共 {total_lines:,} 行")

            print("\n[处理] 正在进行数据清洗与统计...")
            processing_results = []
            for result in read_results:
                proc_result = self.data_processor.process_lines(
                    result.lines,
                    source_file=result.file_name
                )
                processing_results.append(proc_result)

            merged_result = self.data_processor.merge_results(processing_results)
            statistics = self.data_processor.generate_statistics(merged_result)
            print(f"  [OK] 数据处理完成")

            self._write_outputs(
                statistics,
                merged_result.error_logs,
                merged_result.warning_logs
            )

            self._print_summary(statistics)
            print("\n[成功] 所有任务执行完成！")

        except Exception as e:
            print(f"\n[错误] 程序运行异常: {str(e)}")
            print("[提示] 程序已安全退出，数据未丢失")
            sys.exit(1)


def main():
    analyzer = LogAnalyzer()
    analyzer.run()


if __name__ == "__main__":
    main()
