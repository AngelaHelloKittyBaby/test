# -*- coding: utf-8 -*-
"""
日志读取模块 (LogReader)
负责读取日志文件、校验文件合法性、过滤只读保护文件
"""

import os
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from config import get_config
from validator import LogValidator


@dataclass
class ReadResult:
    """读取结果数据类"""
    success: bool
    file_path: str
    file_name: str
    lines: List[str]
    total_lines: int
    valid_lines: int
    filtered_lines: int
    error_message: str = ""
    is_readonly: bool = False


class LogReader:
    """
    日志读取器类
    使用PascalCase命名法
    """
    
    def __init__(self):
        self._config = get_config()
        self._validator = LogValidator()
        self._readonly_files = set(self._config.get_readonly_files())
    
    def is_readonly_file(self, file_path: str) -> bool:
        normalized_path = os.path.normpath(file_path)
        for readonly in self._readonly_files:
            if os.path.normpath(readonly) == normalized_path:
                return True
        return False
    
    def is_valid_log_file(self, file_path: str) -> bool:
        if not os.path.exists(file_path):
            return False
        
        if not os.path.isfile(file_path):
            return False
        
        allowed_extensions = self._config.get_allowed_extensions()
        file_ext = os.path.splitext(file_path)[1].lower()
        if file_ext not in allowed_extensions:
            return False
        
        max_size = 100 * 1024 * 1024
        try:
            file_size = os.path.getsize(file_path)
            if file_size > max_size:
                return False
        except OSError:
            return False
        
        return True
    
    def read_file(self, file_path: str) -> ReadResult:
        is_readonly = self.is_readonly_file(file_path)
        
        if not self.is_valid_log_file(file_path):
            return ReadResult(
                success=False,
                file_path=file_path,
                file_name=os.path.basename(file_path),
                lines=[],
                total_lines=0,
                valid_lines=0,
                filtered_lines=0,
                error_message="无效的文件或文件不存在",
                is_readonly=is_readonly
            )
        
        try:
            encoding = self._config.get_encoding()
            
            lines = []
            total_count = 0
            filtered_count = 0
            
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                for line in f:
                    total_count += 1
                    line = line.rstrip('\n\r')
                    
                    if self._validator.should_skip(line):
                        filtered_count += 1
                        continue
                    
                    lines.append(line)
            
            valid_count = len(lines)
            
            return ReadResult(
                success=True,
                file_path=file_path,
                file_name=os.path.basename(file_path),
                lines=lines,
                total_lines=total_count,
                valid_lines=valid_count,
                filtered_lines=filtered_count,
                is_readonly=is_readonly
            )
            
        except PermissionError as e:
            return ReadResult(
                success=False,
                file_path=file_path,
                file_name=os.path.basename(file_path),
                lines=[],
                total_lines=0,
                valid_lines=0,
                filtered_lines=0,
                error_message=f"权限不足: {str(e)}",
                is_readonly=is_readonly
            )
        except Exception as e:
            return ReadResult(
                success=False,
                file_path=file_path,
                file_name=os.path.basename(file_path),
                lines=[],
                total_lines=0,
                valid_lines=0,
                filtered_lines=0,
                error_message=f"读取失败: {str(e)}",
                is_readonly=is_readonly
            )
    
    def read_directory(self, directory: str = None) -> List[ReadResult]:
        if directory is None:
            directory = self._config.get_input_dir()
        
        results = []
        
        if not os.path.exists(directory):
            return results
        
        if not os.path.isdir(directory):
            return results
        
        log_files = self._get_log_files(directory)
        
        for file_path in log_files:
            result = self.read_file(file_path)
            results.append(result)
        
        return results
    
    def _get_log_files(self, directory: str) -> List[str]:
        log_files = []
        allowed_extensions = self._config.get_allowed_extensions()
        
        try:
            for entry in os.scandir(directory):
                if entry.is_file():
                    file_ext = os.path.splitext(entry.name)[1].lower()
                    if file_ext in allowed_extensions:
                        log_files.append(entry.path)
                elif entry.is_dir():
                    sub_files = self._get_log_files(entry.path)
                    log_files.extend(sub_files)
        except (PermissionError, OSError):
            pass
        
        log_files.sort()
        return log_files


_reader_instance: Optional[LogReader] = None


def get_log_reader() -> LogReader:
    global _reader_instance
    if _reader_instance is None:
        _reader_instance = LogReader()
    return _reader_instance
