# -*- coding: utf-8 -*-
"""
文件写入模块 (FileWriter)
负责输出报告、JSON统计文件、异常日志归档
提供安全的文件写入功能
"""

import os
import json
import shutil
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
from datetime import datetime

from config_manager import get_config_manager
from logger_system import get_logger


class FileWriter:
    """
    文件写入器类
    使用PascalCase命名法
    负责所有文件的写入操作
    """
    
    def __init__(self):
        """
        初始化文件写入器
        """
        self._config = get_config_manager()
        self._logger = get_logger()
        self._readonly_files = set(self._config.get_readonly_files())
    
    def is_readonly(self, file_path: str) -> bool:
        """
        检查文件是否为只读保护文件
        
        参数:
            file_path: 文件路径
        
        返回:
            是否为只读文件
        """
        normalized_path = os.path.normpath(file_path)
        for readonly in self._readonly_files:
            if os.path.normpath(readonly) == normalized_path:
                return True
        return False
    
    def ensure_directory(self, file_path: str) -> bool:
        """
        确保文件所在目录存在
        
        参数:
            file_path: 文件路径
        
        返回:
            是否成功
        """
        directory = os.path.dirname(file_path)
        if not directory:
            return True
        
        try:
            os.makedirs(directory, exist_ok=True)
            return True
        except Exception as e:
            self._logger.error(f"创建目录失败: {directory} - {str(e)}")
            return False
    
    def write_text(self, file_path: str, content: str, encoding: str = "utf-8") -> bool:
        """
        写入文本文件
        
        参数:
            file_path: 文件路径（相对路径）
            content: 文本内容
            encoding: 编码格式
        
        返回:
            是否成功
        """
        # 检查只读保护
        if self.is_readonly(file_path):
            self._logger.error(f"无法写入只读文件: {file_path}")
            return False
        
        # 确保目录存在
        if not self.ensure_directory(file_path):
            return False
        
        try:
            with open(file_path, 'w', encoding=encoding) as f:
                f.write(content)
            
            self._logger.debug(f"文本文件已写入: {file_path}")
            return True
            
        except PermissionError as e:
            self._logger.error(f"写入文件权限不足: {file_path} - {str(e)}")
            return False
        except Exception as e:
            self._logger.error(f"写入文本文件失败: {file_path} - {str(e)}")
            return False
    
    def write_json(self, file_path: str, data: Dict[str, Any], 
                   encoding: str = "utf-8", indent: int = 2) -> bool:
        """
        写入JSON文件
        
        参数:
            file_path: 文件路径（相对路径）
            data: JSON数据
            encoding: 编码格式
            indent: 缩进空格数
        
        返回:
            是否成功
        """
        # 检查只读保护
        if self.is_readonly(file_path):
            self._logger.error(f"无法写入只读文件: {file_path}")
            return False
        
        # 确保目录存在
        if not self.ensure_directory(file_path):
            return False
        
        try:
            with open(file_path, 'w', encoding=encoding) as f:
                json.dump(data, f, ensure_ascii=False, indent=indent)
            
            self._logger.debug(f"JSON文件已写入: {file_path}")
            return True
            
        except PermissionError as e:
            self._logger.error(f"写入文件权限不足: {file_path} - {str(e)}")
            return False
        except Exception as e:
            self._logger.error(f"写入JSON文件失败: {file_path} - {str(e)}")
            return False
    
    def write_lines(self, file_path: str, lines: List[str], 
                    encoding: str = "utf-8", line_ending: str = "\n") -> bool:
        """
        写入多行文本文件
        
        参数:
            file_path: 文件路径（相对路径）
            lines: 行列表
            encoding: 编码格式
            line_ending: 行尾符
        
        返回:
            是否成功
        """
        # 检查只读保护
        if self.is_readonly(file_path):
            self._logger.error(f"无法写入只读文件: {file_path}")
            return False
        
        # 确保目录存在
        if not self.ensure_directory(file_path):
            return False
        
        try:
            with open(file_path, 'w', encoding=encoding) as f:
                for line in lines:
                    f.write(line + line_ending)
            
            self._logger.debug(f"行文本文件已写入: {file_path}")
            return True
            
        except PermissionError as e:
            self._logger.error(f"写入文件权限不足: {file_path} - {str(e)}")
            return False
        except Exception as e:
            self._logger.error(f"写入行文本文件失败: {file_path} - {str(e)}")
            return False
    
    def append_text(self, file_path: str, content: str, encoding: str = "utf-8") -> bool:
        """
        追加文本到文件
        
        参数:
            file_path: 文件路径（相对路径）
            content: 文本内容
            encoding: 编码格式
        
        返回:
            是否成功
        """
        # 检查只读保护
        if self.is_readonly(file_path):
            self._logger.error(f"无法追加只读文件: {file_path}")
            return False
        
        # 确保目录存在
        if not self.ensure_directory(file_path):
            return False
        
        try:
            with open(file_path, 'a', encoding=encoding) as f:
                f.write(content)
            
            self._logger.debug(f"文本已追加到文件: {file_path}")
            return True
            
        except PermissionError as e:
            self._logger.error(f"追加文件权限不足: {file_path} - {str(e)}")
            return False
        except Exception as e:
            self._logger.error(f"追加文本失败: {file_path} - {str(e)}")
            return False
    
    def copy_file(self, source_path: str, dest_path: str) -> bool:
        """
        复制文件
        
        参数:
            source_path: 源文件路径
            dest_path: 目标文件路径
        
        返回:
            是否成功
        """
        # 检查只读保护（目标文件）
        if self.is_readonly(dest_path):
            self._logger.error(f"无法复制到只读文件: {dest_path}")
            return False
        
        # 确保目录存在
        if not self.ensure_directory(dest_path):
            return False
        
        try:
            shutil.copy2(source_path, dest_path)
            self._logger.debug(f"文件已复制: {source_path} -> {dest_path}")
            return True
            
        except Exception as e:
            self._logger.error(f"复制文件失败: {str(e)}")
            return False
    
    def move_file(self, source_path: str, dest_path: str) -> bool:
        """
        移动文件
        
        参数:
            source_path: 源文件路径
            dest_path: 目标文件路径
        
        返回:
            是否成功
        """
        # 检查只读保护（源文件和目标文件）
        if self.is_readonly(source_path):
            self._logger.error(f"无法移动只读文件: {source_path}")
            return False
        
        if self.is_readonly(dest_path):
            self._logger.error(f"无法移动到只读文件: {dest_path}")
            return False
        
        # 确保目录存在
        if not self.ensure_directory(dest_path):
            return False
        
        try:
            shutil.move(source_path, dest_path)
            self._logger.debug(f"文件已移动: {source_path} -> {dest_path}")
            return True
            
        except Exception as e:
            self._logger.error(f"移动文件失败: {str(e)}")
            return False
    
    def delete_file(self, file_path: str) -> bool:
        """
        删除文件
        
        参数:
            file_path: 文件路径
        
        返回:
            是否成功
        """
        # 检查只读保护
        if self.is_readonly(file_path):
            self._logger.error(f"无法删除只读文件: {file_path}")
            return False
        
        try:
            os.remove(file_path)
            self._logger.debug(f"文件已删除: {file_path}")
            return True
            
        except FileNotFoundError:
            self._logger.warning(f"文件不存在，无法删除: {file_path}")
            return False
        except Exception as e:
            self._logger.error(f"删除文件失败: {file_path} - {str(e)}")
            return False
    
    def file_exists(self, file_path: str) -> bool:
        """
        检查文件是否存在
        
        参数:
            file_path: 文件路径
        
        返回:
            是否存在
        """
        return os.path.exists(file_path) and os.path.isfile(file_path)
    
    def get_file_size(self, file_path: str) -> int:
        """
        获取文件大小
        
        参数:
            file_path: 文件路径
        
        返回:
            文件大小（字节），失败返回-1
        """
        try:
            return os.path.getsize(file_path)
        except Exception:
            return -1
    
    def generate_output_filename(self, suffix: str = "", extension: str = ".txt") -> str:
        """
        生成输出文件名
        
        参数:
            suffix: 文件名后缀
            extension: 文件扩展名
        
        返回:
            生成的文件名（相对路径）
        """
        output_dir = self._config.get_output_dir()
        prefix = self._config.get_output_prefix()
        timestamp = datetime.now().strftime(self._config.get_timestamp_format())
        
        if suffix:
            filename = f"{prefix}{timestamp}_{suffix}{extension}"
        else:
            filename = f"{prefix}{timestamp}{extension}"
        
        return os.path.join(output_dir, filename)


# 全局文件写入器实例
_writer_instance = None


def get_file_writer() -> FileWriter:
    """
    获取全局文件写入器实例（单例模式）
    
    返回:
        FileWriter实例
    """
    global _writer_instance
    if _writer_instance is None:
        _writer_instance = FileWriter()
    return _writer_instance
