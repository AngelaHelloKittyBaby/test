# -*- coding: utf-8 -*-
"""
日志读取模块 (LogReader)
负责读取日志文件、校验文件合法性、过滤只读保护文件
"""

import os
from typing import List, Dict, Any, Optional, Iterator
from dataclasses import dataclass
from pathlib import Path

from config_manager import get_config_manager
from logger_system import get_logger
from filter_plugins import get_filter_manager, FilterResult


@dataclass
class ReadResult:
    """
    读取结果数据类
    """
    success: bool
    file_path: str
    lines: List[str]
    total_lines: int
    valid_lines: int
    filtered_lines: int
    error_message: str = ""
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class LogReader:
    """
    日志读取器类
    使用PascalCase命名法
    负责安全地读取日志文件
    """
    
    def __init__(self):
        """
        初始化日志读取器
        """
        self._config = get_config_manager()
        self._logger = get_logger()
        self._filter_manager = get_filter_manager()
        self._readonly_files = set(self._config.get_readonly_files())
    
    def is_readonly_file(self, file_path: str) -> bool:
        """
        检查文件是否为只读保护文件
        
        参数:
            file_path: 文件路径
        
        返回:
            是否为只读文件
        """
        # 标准化路径进行比较
        normalized_path = os.path.normpath(file_path)
        for readonly in self._readonly_files:
            if os.path.normpath(readonly) == normalized_path:
                return True
        return False
    
    def is_valid_log_file(self, file_path: str) -> bool:
        """
        检查文件是否为有效的日志文件
        
        参数:
            file_path: 文件路径
        
        返回:
            是否有效
        """
        # 检查文件是否存在
        if not os.path.exists(file_path):
            return False
        
        # 检查是否为文件
        if not os.path.isfile(file_path):
            return False
        
        # 检查扩展名
        allowed_extensions = self._config.get_allowed_extensions()
        file_ext = os.path.splitext(file_path)[1].lower()
        if file_ext not in allowed_extensions:
            return False
        
        # 检查文件大小（避免读取过大的文件）
        max_size = 100 * 1024 * 1024  # 100MB
        try:
            file_size = os.path.getsize(file_path)
            if file_size > max_size:
                self._logger.warning(f"文件过大，跳过: {file_path} ({file_size} bytes)")
                return False
        except OSError:
            return False
        
        return True
    
    def read_file(self, file_path: str, apply_filters: bool = True) -> ReadResult:
        """
        读取单个日志文件
        
        参数:
            file_path: 文件路径（相对路径）
            apply_filters: 是否应用过滤器
        
        返回:
            ReadResult对象
        """
        self._logger.info(f"开始读取文件: {file_path}")
        
        # 校验文件
        if not self.is_valid_log_file(file_path):
            return ReadResult(
                success=False,
                file_path=file_path,
                lines=[],
                total_lines=0,
                valid_lines=0,
                filtered_lines=0,
                error_message="无效的文件或文件不存在"
            )
        
        # 检查只读保护
        is_readonly = self.is_readonly_file(file_path)
        if is_readonly:
            self._logger.info(f"文件为只读保护: {file_path}")
        
        try:
            encoding = self._config.get_encoding()
            encoding_errors = self._config.get_encoding_errors()
            
            lines = []
            total_count = 0
            filtered_count = 0
            
            with open(file_path, 'r', encoding=encoding, errors=encoding_errors) as f:
                for line in f:
                    total_count += 1
                    line = line.rstrip('\n\r')
                    
                    # 应用过滤器
                    if apply_filters:
                        filter_result = self._filter_manager.apply_filters(line)
                        if filter_result.should_filter:
                            filtered_count += 1
                            continue
                    
                    lines.append(line)
            
            valid_count = len(lines)
            
            self._logger.log_file_processed(file_path, total_count, valid_count)
            
            return ReadResult(
                success=True,
                file_path=file_path,
                lines=lines,
                total_lines=total_count,
                valid_lines=valid_count,
                filtered_lines=filtered_count,
                metadata={
                    "is_readonly": is_readonly,
                    "encoding": encoding,
                    "file_size": os.path.getsize(file_path)
                }
            )
            
        except PermissionError as e:
            self._logger.error(f"读取文件权限不足: {file_path} - {str(e)}")
            return ReadResult(
                success=False,
                file_path=file_path,
                lines=[],
                total_lines=0,
                valid_lines=0,
                filtered_lines=0,
                error_message=f"权限不足: {str(e)}"
            )
        except UnicodeDecodeError as e:
            self._logger.error(f"文件编码错误: {file_path} - {str(e)}")
            return ReadResult(
                success=False,
                file_path=file_path,
                lines=[],
                total_lines=0,
                valid_lines=0,
                filtered_lines=0,
                error_message=f"编码错误: {str(e)}"
            )
        except Exception as e:
            self._logger.error(f"读取文件失败: {file_path} - {str(e)}")
            return ReadResult(
                success=False,
                file_path=file_path,
                lines=[],
                total_lines=0,
                valid_lines=0,
                filtered_lines=0,
                error_message=f"读取失败: {str(e)}"
            )
    
    def read_directory(self, directory: str = None, apply_filters: bool = True) -> List[ReadResult]:
        """
        读取目录下的所有日志文件
        
        参数:
            directory: 目录路径（相对路径），默认使用配置中的input_dir
            apply_filters: 是否应用过滤器
        
        返回:
            ReadResult对象列表
        """
        if directory is None:
            directory = self._config.get_input_dir()
        
        self._logger.info(f"开始扫描目录: {directory}")
        
        results = []
        
        # 检查目录是否存在
        if not os.path.exists(directory):
            self._logger.error(f"输入目录不存在: {directory}")
            return results
        
        if not os.path.isdir(directory):
            self._logger.error(f"路径不是目录: {directory}")
            return results
        
        # 获取所有日志文件
        log_files = self._get_log_files(directory)
        
        if not log_files:
            self._logger.warning(f"目录中没有找到日志文件: {directory}")
            return results
        
        self._logger.info(f"找到 {len(log_files)} 个日志文件")
        
        # 读取每个文件
        for file_path in log_files:
            result = self.read_file(file_path, apply_filters)
            results.append(result)
        
        # 统计
        success_count = sum(1 for r in results if r.success)
        self._logger.info(f"成功读取 {success_count}/{len(results)} 个文件")
        
        return results
    
    def _get_log_files(self, directory: str) -> List[str]:
        """
        获取目录下的所有日志文件
        
        参数:
            directory: 目录路径
        
        返回:
            日志文件路径列表（相对路径）
        """
        log_files = []
        allowed_extensions = self._config.get_allowed_extensions()
        
        try:
            for entry in os.scandir(directory):
                if entry.is_file():
                    file_ext = os.path.splitext(entry.name)[1].lower()
                    if file_ext in allowed_extensions:
                        log_files.append(entry.path)
                elif entry.is_dir():
                    # 递归子目录
                    sub_files = self._get_log_files(entry.path)
                    log_files.extend(sub_files)
        except PermissionError as e:
            self._logger.error(f"访问目录权限不足: {directory} - {str(e)}")
        except Exception as e:
            self._logger.error(f"扫描目录失败: {directory} - {str(e)}")
        
        # 按文件名排序
        log_files.sort()
        return log_files
    
    def read_file_stream(self, file_path: str, chunk_size: int = 1000) -> Iterator[List[str]]:
        """
        流式读取大文件
        
        参数:
            file_path: 文件路径
            chunk_size: 每次读取的行数
        
        返回:
            行列表的迭代器
        """
        if not self.is_valid_log_file(file_path):
            self._logger.error(f"无效的文件: {file_path}")
            return
        
        try:
            encoding = self._config.get_encoding()
            encoding_errors = self._config.get_encoding_errors()
            
            chunk = []
            with open(file_path, 'r', encoding=encoding, errors=encoding_errors) as f:
                for line in f:
                    line = line.rstrip('\n\r')
                    
                    # 应用过滤器
                    filter_result = self._filter_manager.apply_filters(line)
                    if filter_result.should_filter:
                        continue
                    
                    chunk.append(line)
                    
                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []
                
                # 返回剩余的行
                if chunk:
                    yield chunk
                    
        except Exception as e:
            self._logger.error(f"流式读取文件失败: {file_path} - {str(e)}")
    
    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """
        获取文件信息
        
        参数:
            file_path: 文件路径
        
        返回:
            文件信息字典
        """
        try:
            stat = os.stat(file_path)
            return {
                "path": file_path,
                "name": os.path.basename(file_path),
                "size": stat.st_size,
                "modified_time": stat.st_mtime,
                "is_readonly": self.is_readonly_file(file_path),
                "is_valid": self.is_valid_log_file(file_path)
            }
        except Exception as e:
            self._logger.error(f"获取文件信息失败: {file_path} - {str(e)}")
            return {
                "path": file_path,
                "error": str(e)
            }


# 全局日志读取器实例
_reader_instance: Optional[LogReader] = None


def get_log_reader() -> LogReader:
    """
    获取全局日志读取器实例（单例模式）
    
    返回:
        LogReader实例
    """
    global _reader_instance
    if _reader_instance is None:
        _reader_instance = LogReader()
    return _reader_instance
