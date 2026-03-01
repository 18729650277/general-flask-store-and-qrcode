# -*- coding: utf-8 -*-
"""
通用Flask后端API核心模块
Copyright (c) 2026 ai摸鱼的程序员
License: Apache 2.0 (详见项目根目录 LICENSE 文件)
"""
from flask import Flask, redirect, render_template, request, jsonify, send_file, make_response, abort
from typing import List, Optional, Tuple, Any, Dict, Union
import logging
import secrets
import sqlite3
import hashlib
import os
import json
import time
import psutil
import PIL
from PIL import Image, ImageDraw
import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端
import matplotlib.pyplot as plt
import numpy as np
import re
import math
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import qrcode
import io
import ast
import operator
from functools import wraps
from datetime import datetime, timedelta
import threading
from contextlib import contextmanager
import atexit
import shutil
import sys
import signal

class Path:
    """路径工具类"""
    
    @staticmethod
    def file(relative_path: str) -> str:
        """获取文件绝对路径"""
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), relative_path)
    
    @staticmethod
    def dir(relative_path: str) -> str:
        """获取目录绝对路径"""
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), relative_path)
    
    @staticmethod
    def temp(prefix: str = "tmp", suffix: str = "") -> str:
        """生成临时文件路径"""
        filename = f"{prefix}_{secrets.token_hex(8)}{suffix}"
        return os.path.join(Path.dir('temp'), filename)

# 创建必要的目录
for dir_path in ['data', 'log', 'temp']:
    os.makedirs(Path.dir(dir_path), exist_ok=True)
# ==================== 自定义异常 ====================

class AppError(Exception):
    """应用自定义错误"""
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code

class ValidationError(AppError):
    """验证错误"""
    def __init__(self, message: str):
        super().__init__(message, 400)

class AuthenticationError(AppError):
    """认证错误"""
    def __init__(self, message: str = "需要认证"):
        super().__init__(message, 401)

class PermissionError(AppError):
    """权限错误"""
    def __init__(self, message: str = "权限不足"):
        super().__init__(message, 403)

class RateLimitError(AppError):
    """限流错误"""
    def __init__(self, message: str = "请求过于频繁"):
        super().__init__(message, 429)

# ==================== 日志配置 ====================

def setup_logger() -> logging.Logger:
    """配置日志系统"""
    # 日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
    
    # 文件处理器
    log_file = Path.file(f'log/app_{time.strftime("%Y%m%d_%H%M%S")}.log')
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    
    # 错误文件处理器
    error_log_file = Path.file(f'log/error_{time.strftime("%Y%m%d_%H%M%S")}.log')
    error_file_handler = logging.FileHandler(error_log_file, encoding='utf-8')
    error_file_handler.setFormatter(formatter)
    error_file_handler.setLevel(logging.ERROR)
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # 配置根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(error_file_handler)
    root_logger.addHandler(console_handler)
    
    return logging.getLogger(__name__)

logger = setup_logger()
START_RUN_TIME = time.time()

# ==================== 数据库操作类（安全版本） ====================

class DatabasePool:
    """数据库连接池"""
    _instances: Dict[str, sqlite3.Connection] = {}
    _lock = threading.Lock()
    _ref_count: Dict[str, int] = {}
    
    @classmethod
    def get_connection(cls, db_path: str) -> sqlite3.Connection:
        """获取或创建数据库连接"""
        with cls._lock:
            if db_path not in cls._instances:
                # 确保目录存在
                os.makedirs(os.path.dirname(db_path), exist_ok=True)
                
                conn = sqlite3.connect(db_path, check_same_thread=False, timeout=10)
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA foreign_keys=ON")
                
                cls._instances[db_path] = conn
                cls._ref_count[db_path] = 0
            
            cls._ref_count[db_path] += 1
            return cls._instances[db_path]
    
    @classmethod
    def close_connection(cls, db_path: str):
        """关闭数据库连接"""
        with cls._lock:
            if db_path in cls._ref_count:
                cls._ref_count[db_path] -= 1
                if cls._ref_count[db_path] <= 0 and db_path in cls._instances:
                    try:
                        cls._instances[db_path].close()
                    except Exception as e:
                        logger.error(f"关闭数据库连接失败 {db_path}: {e}")
                    finally:
                        del cls._instances[db_path]
                        del cls._ref_count[db_path]
    
    @classmethod
    def close_all(cls):
        """关闭所有连接"""
        with cls._lock:
            for db_path in list(cls._instances.keys()):
                try:
                    cls._instances[db_path].close()
                except Exception as e:
                    logger.error(f"关闭数据库连接失败 {db_path}: {e}")
            cls._instances.clear()
            cls._ref_count.clear()

class Data:
    """安全的数据库操作类"""
    
    # 表名验证正则
    TABLE_NAME_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
    KEY_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9_\-\.]{1,255}$')
    
    def __init__(self, path: str, table: str) -> None:
        """初始化"""
        self.path = os.path.normpath(path)
        self.table = self._validate_table_name(table)
        self._local = threading.local()
        logger.info(f"初始化数据库: {self.path}, 表: {self.table}")
        
    def _validate_table_name(self, table: str) -> str:
        """验证表名"""
        if not self.TABLE_NAME_PATTERN.match(table):
            raise ValidationError(f"无效的表名: {table}")
        return table
    
    def _validate_key(self, key: str) -> str:
        """验证键名"""
        if not key or not isinstance(key, str):
            raise ValidationError("键必须是字符串")
        if not self.KEY_NAME_PATTERN.match(key):
            raise ValidationError(f"无效的键名: {key}")
        return key
    
    @contextmanager
    def _get_cursor(self):
        """获取数据库游标的上下文管理器"""
        conn = None
        try:
            conn = DatabasePool.get_connection(self.path)
            cursor = conn.cursor()
            yield cursor
            conn.commit()
        except sqlite3.Error as e:
            if conn:
                conn.rollback()
            logger.error(f"数据库操作错误: {e}")
            raise AppError(f"数据库操作失败: {e}", 500)
        finally:
            if conn:
                DatabasePool.close_connection(self.path)
    
    def _ensure_table(self, cursor):
        """确保表存在"""
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {self.table} "
            "(key TEXT PRIMARY KEY, value TEXT, created_at REAL, updated_at REAL)"
        )
        # 创建索引
        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.table}_updated ON {self.table}(updated_at)"
        )
    
    def exists(self, key: str) -> bool:
        """检查键是否存在"""
        key = self._validate_key(key)
        try:
            with self._get_cursor() as cursor:
                self._ensure_table(cursor)
                cursor.execute(f"SELECT 1 FROM {self.table} WHERE key=?", (key,))
                return cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"检查键时出错: {e}")
            return False
    
    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """获取值"""
        key = self._validate_key(key)
        try:
            with self._get_cursor() as cursor:
                self._ensure_table(cursor)
                cursor.execute(f"SELECT value FROM {self.table} WHERE key=?", (key,))
                result = cursor.fetchone()
                return result[0] if result else default
        except sqlite3.Error as e:
            logger.error(f"获取值时出错: {e}")
            return default
    
    def set(self, key: str, value: str) -> bool:
        """设置值"""
        key = self._validate_key(key)
        now = time.time()
        try:
            with self._get_cursor() as cursor:
                self._ensure_table(cursor)
                cursor.execute(
                    f"INSERT OR REPLACE INTO {self.table} (key, value, created_at, updated_at) "
                    f"VALUES (?, ?, COALESCE((SELECT created_at FROM {self.table} WHERE key=?), ?), ?)",
                    (key, str(value), key, now, now)
                )
                logger.info(f"数据库 {self.path} 表 {self.table} 键 {key} 设置成功")
                return True
        except sqlite3.Error as e:
            logger.error(f"设置值时出错: {e}")
            return False
    
    def items(self) -> List[Tuple[str, str]]:
        """获取所有键值对"""
        try:
            with self._get_cursor() as cursor:
                self._ensure_table(cursor)
                cursor.execute(f"SELECT key, value FROM {self.table} ORDER BY updated_at DESC")
                return cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"获取所有键值对时出错: {e}")
            return []
    
    def delete(self, key: str) -> bool:
        """删除键值对"""
        key = self._validate_key(key)
        try:
            with self._get_cursor() as cursor:
                self._ensure_table(cursor)
                cursor.execute(f"DELETE FROM {self.table} WHERE key=?", (key,))
                logger.info(f"数据库 {self.path} 表 {self.table} 键 {key} 删除成功")
                return cursor.rowcount > 0
        except sqlite3.Error as e:
            logger.error(f"删除键值对时出错: {e}")
            return False
    
    def clear_expired(self, max_age: int = 86400) -> int:
        """清理过期数据（默认24小时）"""
        try:
            with self._get_cursor() as cursor:
                self._ensure_table(cursor)
                cutoff = time.time() - max_age
                cursor.execute(
                    f"DELETE FROM {self.table} WHERE updated_at < ?",
                    (cutoff,)
                )
                deleted = cursor.rowcount
                if deleted > 0:
                    logger.info(f"清理了 {deleted} 条过期数据")
                return deleted
        except sqlite3.Error as e:
            logger.error(f"清理过期数据时出错: {e}")
            return 0
    
    def __getitem__(self, key: str) -> Optional[str]:
        return self.get(key)
    
    def __setitem__(self, key: str, value: str) -> None:
        self.set(key, value)
    
    def __contains__(self, key: str) -> bool:
        return self.exists(key)

# ==================== 工具类 ====================
class Password:
    """密码处理类"""
    
    @staticmethod
    def hash(password: str) -> str:
        """哈希密码"""
        if not password or len(password) < 6:
            raise ValidationError("密码长度不能少于6位")
        
        salt = secrets.token_hex(16)
        # 使用多次哈希增加安全性
        hash_value = password
        for _ in range(1000):  # 1000次迭代
            hash_value = hashlib.sha256((hash_value + salt).encode()).hexdigest()
        return f"{hash_value}:{salt}"
    
    @staticmethod
    def verify(password: str, hashed_password: str) -> bool:
        """验证密码"""
        try:
            hash_value, salt = hashed_password.split(":")
            # 验证时使用相同的迭代次数
            test_hash = password
            for _ in range(1000):
                test_hash = hashlib.sha256((test_hash + salt).encode()).hexdigest()
            return hash_value == test_hash
        except Exception as e:
            logger.error(f"密码验证失败: {e}")
            return False
    
    @staticmethod
    def generate_strong() -> str:
        """生成强密码"""
        import string
        alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
        while True:
            password = ''.join(secrets.choice(alphabet) for _ in range(12))
            # 确保包含至少一个大写字母、小写字母、数字和特殊字符
            if (any(c.islower() for c in password) and
                any(c.isupper() for c in password) and
                any(c.isdigit() for c in password) and
                any(c in "!@#$%^&*" for c in password)):
                return password

class APIKey:
    """API密钥管理类"""
    
    @staticmethod
    def add(access_for: List[str] = None, length_per_seg: int = 8, seg_num: int = 4) -> str:
        """生成API密钥"""
        if not access_for:
            access_for = ["Any"]
        
        # 验证权限列表
        valid_access = ["Any", "store", "draw", "calc", "qr_code"]
        for access in access_for:
            if access not in valid_access:
                raise ValidationError(f"无效的权限: {access}")
        
        key = '-'.join(secrets.token_hex(length_per_seg // 2) for _ in range(seg_num))
        key_data = {
            "valid": True,
            "access_for": access_for,
            "created_at": time.time(),
            "last_used": None,
            "usage_count": 0
        }
        
        data_files['api_key']['object'].set(key, json.dumps(key_data))
        logger.info(f"API密钥 {key[:8]}... 生成成功，权限: {access_for}")
        return key
    
    @staticmethod
    def valid(api_key: str, required_access: List[str]) -> bool:
        """验证API密钥"""
        if not api_key:
            return False
        
        try:
            value = data_files['api_key']['object'].get(api_key)
            if not value:
                return False
            
            data = json.loads(value)
            
            # 检查是否有效
            if not data.get('valid', False):
                return False
            
            # 更新使用统计
            data['last_used'] = time.time()
            data['usage_count'] = data.get('usage_count', 0) + 1
            data_files['api_key']['object'].set(api_key, json.dumps(data))
            
            # 检查权限
            allowed = data.get('access_for', [])
            
            # "Any" 权限允许所有操作
            if "Any" in allowed:
                return True
            
            return all(access in allowed for access in required_access)
            
        except Exception as e:
            logger.error(f"API密钥验证失败: {e}")
            return False
    
    @staticmethod
    def set_invalid(api_key: str) -> bool:
        """设置密钥为无效"""
        try:
            value = data_files['api_key']['object'].get(api_key)
            if value:
                data = json.loads(value)
                data['valid'] = False
                data_files['api_key']['object'].set(api_key, json.dumps(data))
                logger.info(f"API密钥 {api_key[:8]}... 已禁用")
                return True
            return False
        except Exception as e:
            logger.error(f"禁用API密钥失败: {e}")
            return False
    
    @staticmethod
    def list_keys() -> List[Dict]:
        """列出所有API密钥"""
        items = data_files['api_key']['object'].items()
        result = []
        for key, value in items:
            try:
                data = json.loads(value)
                result.append({
                    'key': key[:8] + '...' + key[-4:] if len(key) > 12 else key,
                    'full_key': key,
                    'valid': data.get('valid', False),
                    'access_for': data.get('access_for', []),
                    'created_at': data.get('created_at'),
                    'last_used': data.get('last_used'),
                    'usage_count': data.get('usage_count', 0)
                })
            except Exception:
                pass
        return result

# ==================== 安全数学求值器 ====================
class SafeMathEvaluator:
    """安全的数学表达式求值器"""
    
    # 允许的数学函数
    ALLOWED_FUNCTIONS = {
        'sin': np.sin,
        'cos': np.cos,
        'tan': np.tan,
        'arcsin': np.arcsin,
        'arccos': np.arccos,
        'arctan': np.arctan,
        'sinh': np.sinh,
        'cosh': np.cosh,
        'tanh': np.tanh,
        'sqrt': np.sqrt,
        'log': np.log,
        'log10': np.log10,
        'log2': np.log2,
        'exp': np.exp,
        'abs': abs,
        'floor': math.floor,
        'ceil': math.ceil,
        'round': round,
        'max': max,
        'min': min,
        'csc': lambda x: 1/np.sin(x),
        'sec': lambda x: 1/np.cos(x),
        'cot': lambda x: 1/np.tan(x),
        'arccsc': lambda x: np.arcsin(1/x),
        'arcsec': lambda x: np.arccos(1/x),
        'arccot': lambda x: np.pi/2 - np.arctan(x),
    }
    
    # 允许的常量
    ALLOWED_CONSTANTS = {
        'pi': math.pi,
        'e': math.e,
        'inf': float('inf'),
        'nan': float('nan'),
        'π': math.pi,
        'τ': 2 * math.pi,
    }
    
    # 允许的操作符
    OPERATORS = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.Pow: operator.pow,
        ast.USub: operator.neg,
        ast.Mod: operator.mod,
    }
    
    @classmethod
    def _latex_to_python(cls, expr: str) -> str:
        """将 LaTeX 语法转换为 Python 语法"""
        try:
            result = expr
            
            # URL解码：将%20转换为空格
            result = result.replace('%20', ' ')
            
            # 修复连续运算符：将多个连续运算符简化为单个
            result = re.sub(r'\+\+\+', '+', result)  # +++ -> +
            result = re.sub(r'\+\-\+', '-', result)  # +-+ -> -
            result = re.sub(r'\+\s*\+', '+', result)  # + + -> +
            result = re.sub(r'\-\s*\-', '-', result)  # - - -> -
            result = re.sub(r'\+\s*\-', '-', result)  # + - -> -
            result = re.sub(r'\-\s*\+', '-', result)  # - + -> -
            
            # 基本LaTeX符号替换
            latex_to_python = {
                'π': 'pi',           # 圆周率
                '∞': 'inf',          # 无穷大
                '√': 'sqrt',         # 平方根
                '×': '*',            # 乘法
                '÷': '/',            # 除法
                '≤': '<=',           # 小于等于
                '≥': '>=',           # 大于等于
                '≠': '!=',           # 不等于
                '^': '**',           # 幂运算
                '·': '*',            # 点乘
                '°': '*pi/180',      # 度转弧度
                '±': '±',            # 正负号
                '∓': '∓',            # 负正号
                '≈': '≈',            # 约等于
                '≡': '≡',            # 恒等于
                '∝': '∝',            # 正比于
                '∇': '∇',            # 梯度
                '∂': '∂',            # 偏微分
                '∫': '∫',            # 积分
                '∑': '∑',            # 求和
                '∏': '∏',            # 求积
                '∩': '∩',            # 交集
                '∪': '∪',            # 并集
                '∈': '∈',            # 属于
                '∉': '∉',            # 不属于
                '⊂': '⊂',            # 子集
                '⊃': '⊃',            # 超集
                '∧': '∧',            # 逻辑与
                '∨': '∨',            # 逻辑或
                '¬': '¬',            # 逻辑非
                '∀': '∀',            # 任意
                '∃': '∃',            # 存在
                '∄': '∄',            # 不存在
                '∴': '∴',            # 所以
                '∵': '∵',            # 因为
                '∶': ':',            # 比例
                '∶∶': '::',          # 双比例
            }
            
            # 替换LaTeX符号
            for latex, python in latex_to_python.items():
                result = result.replace(latex, python)
            
            # 处理常见的LaTeX函数
            # 替换 \sin, \cos, \tan 等
            latex_functions = {
                r'\\sin': 'sin',
                r'\\cos': 'cos', 
                r'\\tan': 'tan',
                r'\\log': 'log',
                r'\\ln': 'log',
                r'\\exp': 'exp',
                r'\\sqrt': 'sqrt',
                r'\\abs': 'abs',
                r'\\pi': 'pi',
                r'\\times': '*',           # 乘法
                r'\\div': '/',            # 除法
                r'\\cdot': '*',           # 点乘
                r'\\ast': '*',            # 星号乘
                r'\\pm': '±',             # 正负号
                r'\\mp': '∓',             # 负正号
                r'\\approx': '≈',         # 约等于
                r'\\equiv': '≡',          # 恒等于
                r'\\propto': '∝',         # 正比于
                r'\\nabla': '∇',          # 梯度
                r'\\partial': '∂',        # 偏微分
                r'\\int': '∫',           # 积分
                r'\\sum': '∑',           # 求和
                r'\\prod': '∏',          # 求积
                r'\\cap': '∩',           # 交集
                r'\\cup': '∪',           # 并集
                r'\\in': '∈',            # 属于
                r'\\notin': '∉',         # 不属于
                r'\\subset': '⊂',        # 子集
                r'\\supset': '⊃',        # 超集
                r'\\land': '∧',          # 逻辑与
                r'\\lor': '∨',           # 逻辑或
                r'\\neg': '¬',           # 逻辑非
                r'\\forall': '∀',        # 任意
                r'\\exists': '∃',        # 存在
                r'\\nexists': '∄',       # 不存在
                r'\\therefore': '∴',      # 所以
                r'\\because': '∵',       # 因为
                r'\\colon': ':',         # 比例
                r'\\left': '',           # 左括号（移除）
                r'\\right': '',          # 右括号（移除）
            }
            
            for latex_func, python_func in latex_functions.items():
                result = re.sub(latex_func, python_func, result)
            
            # 处理分数格式 a/b
            result = re.sub(r'\\frac\{([^}]+)\}\{([^}]+)\}', r'(\1)/(\2)', result)
            
            # 修复分数后的变量乘法：将 (a)/(b)x 转换为 (a)/(b)*x
            result = re.sub(r'\)/(\d+)([a-zA-Z])', r')/\1*\2', result)
            
            # 处理上标格式 x^{2} -> x**2
            result = re.sub(r'\^\{([^}]+)\}', r'**\1', result)
            
            # 处理上标格式 x^2 -> x**2
            result = re.sub(r'\^(\d+)', r'**\1', result)
            
            # 处理函数上标格式 \sin^2 -> sin**2
            result = re.sub(r'(sin|cos|tan|log|ln|exp|sqrt|abs)\^\{([^}]+)\}', r'\1**\2', result)
            result = re.sub(r'(sin|cos|tan|log|ln|exp|sqrt|abs)\^(\d+)', r'\1**\2', result)
            
            # 处理函数调用格式 √x -> sqrt(x)
            result = re.sub(r'sqrt(\w+)', r'sqrt(\1)', result)
            
            # 修复括号匹配问题：确保所有括号正确闭合
            # 移除多余的转义字符
            result = result.replace('\\', '')
            
            # 清理多余的空格
            result = re.sub(r'\s+', ' ', result).strip()
            
            return result
            
        except Exception:
            # 如果转换失败，返回原始表达式，让后续的解析器处理
            return expr
    
    @classmethod
    def evaluate(cls, expr: str, x: np.ndarray) -> np.ndarray:
        """安全求值数学表达式"""
        
        # 转换 LaTeX 到 Python 语法
        expr = cls._latex_to_python(expr)
        
        # 输入验证
        if len(expr) > get_config('MAX_EXPR_LENGTH', 200):
            raise ValidationError(f"表达式过长，最大长度{get_config('MAX_EXPR_LENGTH', 200)}")
        
        # 检查危险模式
        dangerous_patterns = [
            (r'__\w+__', "双下划线方法"),
            (r'import\s+', "import语句"),
            (r'exec\s*\(', "exec调用"),
            (r'eval\s*\(', "eval调用"),
            (r'open\s*\(', "文件操作"),
            (r'os\.', "os模块"),
            (r'sys\.', "sys模块"),
            (r'subprocess', "subprocess模块"),
            (r'globals\(\)', "globals访问"),
            (r'locals\(\)', "locals访问"),
            (r'__builtins__', "内置模块访问"),
            (r'\[\s*\]', "列表推导可能危险"),
            (r'\{\s*\}', "字典推导可能危险"),
        ]
        
        for pattern, desc in dangerous_patterns:
            if re.search(pattern, expr):
                raise ValidationError(f"表达式包含不安全的语法: {desc}")
        
        try:
            # 解析AST
            tree = ast.parse(expr, mode='eval')
            
            # 验证AST
            cls._validate_node(tree.body)
            
            # 创建安全环境
            safe_dict = {
                'x': x,
                **cls.ALLOWED_FUNCTIONS,
                **cls.ALLOWED_CONSTANTS,
            }
            
            # 使用限制的命名空间编译执行
            code = compile(tree, '<string>', 'eval')
            
            # 使用受限的globals和locals
            result = eval(code, {"__builtins__": {}}, safe_dict)
            
            # 验证结果
            if isinstance(result, np.ndarray):
                if result.size > get_config('MAX_POINTS', 10000):
                    raise ValidationError(f"结果点数过多: {result.size}")
                if not np.all(np.isfinite(result)):
                    raise ValidationError("结果包含无效值(inf或nan)")
            
            return result
            
        except SyntaxError as e:
            raise ValidationError(f"语法错误: {e}")
        except ValidationError:
            raise
        except Exception as e:
            raise ValidationError(f"表达式求值失败: {e}")
    
    @classmethod
    def _validate_node(cls, node):
        """递归验证AST节点"""
        
        if isinstance(node, ast.Constant):
            # Python 3.8+ 使用 Constant
            if isinstance(node.value, (int, float)):
                if abs(node.value) > get_config('MAX_CALC_VALUE', 1000000):
                    raise ValidationError(f"数值过大: {node.value}")
            return
        
        elif isinstance(node, ast.Num):  # 兼容旧版本
            if abs(node.n) > get_config('MAX_CALC_VALUE', 1000000):
                raise ValidationError(f"数值过大: {node.n}")
            return
        
        elif isinstance(node, ast.Name):
            if node.id not in cls.ALLOWED_CONSTANTS and node.id != 'x':
                raise ValidationError(f"不允许的变量: {node.id}")
        
        elif isinstance(node, ast.BinOp):
            cls._validate_node(node.left)
            cls._validate_node(node.right)
            if type(node.op) not in cls.OPERATORS:
                raise ValidationError(f"不允许的操作符: {type(node.op).__name__}")
        
        elif isinstance(node, ast.UnaryOp):
            cls._validate_node(node.operand)
            if type(node.op) not in cls.OPERATORS:
                raise ValidationError(f"不允许的操作符: {type(node.op).__name__}")
        
        elif isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name):
                raise ValidationError("只允许直接函数调用")
            if node.func.id not in cls.ALLOWED_FUNCTIONS:
                raise ValidationError(f"不允许的函数: {node.func.id}")
            
            # 验证参数个数
            func = cls.ALLOWED_FUNCTIONS[node.func.id]
            try:
                # 先尝试获取 Python 函数的参数个数
                if hasattr(func, '__code__'):
                    arg_count = func.__code__.co_argcount
                elif hasattr(func, 'nin'):
                    # NumPy ufunc 对象使用 nin 属性
                    arg_count = func.nin
                else:
                    # 默认允许 1-2 个参数
                    arg_count = 2
                
                if len(node.args) > arg_count:
                    raise ValidationError(f"函数 {node.func.id} 参数过多")
            except Exception:
                # 如果获取参数个数失败，仍然继续，让后续的 eval 处理
                pass
            
            for arg in node.args:
                cls._validate_node(arg)
        
        elif isinstance(node, ast.Expression):
            cls._validate_node(node.body)
        
        elif isinstance(node, ast.List):
            raise ValidationError("列表操作不被允许")
        
        elif isinstance(node, ast.Dict):
            raise ValidationError("字典操作不被允许")
        
        else:
            raise ValidationError(f"不支持的语法: {type(node).__name__}")

    @classmethod
    def _get_smart_x_range(cls, expr: str) -> np.ndarray:
        """根据表达式智能选择x值范围"""
        # 默认范围
        x_min, x_max, points = -10, 10, 1000
        
        # 根据表达式类型调整范围
        expr_lower = expr.lower()
        
        # 对数函数：避免负数和零
        if any(log_func in expr_lower for log_func in ['log(', 'ln(', 'log10(', 'log2(']):
            x_min, x_max = 0.1, 10  # 避免log(0)和负数
        
        # 平方根函数：避免负数
        elif 'sqrt(' in expr_lower:
            x_min, x_max = 0, 10
        
        # 反三角函数：限制在合理范围内
        elif any(arc_func in expr_lower for arc_func in ['arcsin(', 'arccos(', 'arctan(']):
            x_min, x_max = -1, 1
        
        # 正切函数：避免奇点
        elif 'tan(' in expr_lower and 'arctan(' not in expr_lower:
            x_min, x_max = -5, 5  # 避免π/2的倍数
        
        # 包含除法的表达式：避免除零
        elif '/' in expr:
            x_min, x_max = -9, 9  # 避免x=0
        
        return np.linspace(x_min, x_max, points)

# ==================== 限流器 ====================

class RateLimiter:
    """请求限流器"""
    
    def __init__(self, max_requests: int = None, window: int = 60):
        self.max_requests = max_requests if max_requests is not None else 100
        self.window = window
        self.requests: Dict[str, List[float]] = {}
        self._lock = threading.RLock()
        self._cleanup_interval = 300  # 5分钟清理一次
        self._last_cleanup = time.time()
    
    def _cleanup(self):
        """清理过期记录"""
        now = time.time()
        if now - self._last_cleanup < self._cleanup_interval:
            return
        
        with self._lock:
            for key in list(self.requests.keys()):
                self.requests[key] = [t for t in self.requests[key] if now - t < self.window]
                if not self.requests[key]:
                    del self.requests[key]
            self._last_cleanup = now
    
    def is_allowed(self, key: str) -> bool:
        """检查是否允许请求"""
        self._cleanup()
        
        with self._lock:
            now = time.time()
            
            if key not in self.requests:
                self.requests[key] = []
            
            # 清理过期请求
            self.requests[key] = [t for t in self.requests[key] if now - t < self.window]
            
            if len(self.requests[key]) >= self.max_requests:
                return False
            
            self.requests[key].append(now)
            return True
    
    def get_remaining(self, key: str) -> int:
        """获取剩余请求数"""
        self._cleanup()
        
        with self._lock:
            if key not in self.requests:
                return self.max_requests
            
            now = time.time()
            self.requests[key] = [t for t in self.requests[key] if now - t < self.window]
            return max(0, self.max_requests - len(self.requests[key]))

rate_limiter = RateLimiter()

# ==================== 线程池管理器 ====================

class ThreadPoolManager:
    """线程池管理器"""
    
    _instance = None
    _lock = threading.RLock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._executor = None
                cls._instance._shutdown = False
                cls._instance._futures = []
            return cls._instance
    
    @property
    def executor(self):
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=get_config('MAX_WORKERS', 4),
                thread_name_prefix="app_pool"
            )
        return self._executor
    
    def submit(self, func, *args, **kwargs):
        """提交任务"""
        if self._shutdown:
            raise RuntimeError("线程池已关闭")
        
        future = self.executor.submit(func, *args, **kwargs)
        with self._lock:
            self._futures.append(future)
            # 清理已完成的future
            self._futures = [f for f in self._futures if not f.done()]
        return future
    
    def shutdown(self, wait: bool = True):
        """关闭线程池"""
        with self._lock:
            if not self._shutdown:
                self._shutdown = True
                # 取消所有未完成的任务
                for future in self._futures:
                    if not future.done():
                        future.cancel()
                self.executor.shutdown(wait=wait)
                self._futures.clear()

thread_pool = ThreadPoolManager()

# ==================== 缓存管理器 ====================

class CacheManager:
    """简单的内存缓存"""
    
    def __init__(self, max_size: int = 100, default_ttl: int = 300):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._lock = threading.RLock()
    
    def get(self, key: str):
        """获取缓存"""
        with self._lock:
            if key in self._cache:
                value, expiry = self._cache[key]
                if expiry > time.time():
                    return value
                else:
                    del self._cache[key]
        return None
    
    def set(self, key: str, value: Any, ttl: int = None):
        """设置缓存"""
        if ttl is None:
            ttl = self.default_ttl
        
        with self._lock:
            # 清理过期缓存
            self._cleanup()
            
            # 如果缓存已满，删除最旧的
            if len(self._cache) >= self.max_size:
                oldest_key = min(self._cache.keys(), 
                               key=lambda k: self._cache[k][1])
                del self._cache[oldest_key]
            
            self._cache[key] = (value, time.time() + ttl)
    
    def _cleanup(self):
        """清理过期缓存"""
        now = time.time()
        expired = [k for k, (_, exp) in self._cache.items() if exp <= now]
        for k in expired:
            del self._cache[k]
    
    def clear(self):
        """清空缓存"""
        with self._lock:
            self._cache.clear()

cache_manager = CacheManager()

# ==================== 认证装饰器 ====================

def admin_required(f):
    """管理员认证装饰器"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            # 从cookie获取会话
            session_token = request.cookies.get('session_token')
            if not session_token:
                raise AuthenticationError("需要管理员登录")
            
            # 验证会话
            session_data = data_files['session']['object'].get(session_token)
            if not session_data:
                raise AuthenticationError("会话无效")
            
            session = json.loads(session_data)
            
            # 检查会话是否过期
            if time.time() - session.get('created_at', 0) > get_config('SESSION_TIMEOUT', 3600):
                data_files['session']['object'].delete(session_token)
                raise AuthenticationError("会话已过期")
            
            # 检查IP是否匹配（可选，可以根据需要启用）
            # if session.get('ip') != request.remote_addr:
            #     raise AuthenticationError("IP地址不匹配")
            
            # 将用户信息添加到请求上下文
            request.user = session.get('username')
            request.session = session
            
        except AuthenticationError as e:
            # 重定向到登录页面
            return redirect('/login')
        except Exception as e:
            logger.error(f"会话验证错误: {e}")
            # 重定向到登录页面
            return redirect('/login')
        
        return f(*args, **kwargs)
    return decorated_function

def api_key_required(required_access: List[str]):
    """API密钥认证装饰器，同时支持会话认证（管理员登录）"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # 首先尝试 API 密钥认证
            api_key = request.args.get('api_key') or request.headers.get('X-API-Key')
            
            if APIKey.valid(api_key, required_access):
                return f(*args, **kwargs)
            
            # 如果 API 密钥认证失败，尝试会话认证（管理员登录）
            try:
                session_token = request.cookies.get('session_token')
                if not session_token:
                    raise AuthenticationError("需要认证")
                
                session_data = data_files['session']['object'].get(session_token)
                if not session_data:
                    raise AuthenticationError("会话无效")
                
                session = json.loads(session_data)
                
                if time.time() - session.get('created_at', 0) > get_config('SESSION_TIMEOUT', 3600):
                    data_files['session']['object'].delete(session_token)
                    raise AuthenticationError("会话已过期")
                
                # 管理员会话认证成功
                return f(*args, **kwargs)
            except AuthenticationError:
                # 两种认证方式都失败
                logger.warning(f"IP {request.remote_addr} 认证失败")
                return jsonify({'error': '无效的认证'}), 403
        return decorated_function
    return decorator

def rate_limit(f):
    """限流装饰器"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # 对于API密钥认证的请求，使用更宽松的限流
        api_key = request.args.get('api_key') or request.headers.get('X-API-Key')
        if api_key:
            key = f"api:{api_key}"
            limit = get_config('RATE_LIMIT', 100) * 5  # API密钥用户有5倍额度
        else:
            key = f"ip:{request.remote_addr}:{request.path}"
            limit = get_config('RATE_LIMIT', 100)
        
        # 临时创建限流器
        limiter = RateLimiter(max_requests=limit)
        if not limiter.is_allowed(key):
            remaining = limiter.get_remaining(key)
            logger.warning(f"请求过于频繁: {key}, 剩余: {remaining}")
            
            response = jsonify({'error': '请求过于频繁'})
            response.headers['X-RateLimit-Limit'] = str(limit)
            response.headers['X-RateLimit-Remaining'] = str(remaining)
            return response, 429
        
        return f(*args, **kwargs)
    return decorated_function

# ==================== 初始化数据库 ====================

# 数据库配置
data_files = {
    'config': {
        "path": Path.file('data/config.db'),
        "object": None,
        "init_data": {
            "port": "5000",
            "host": "0.0.0.0",
            "admin_user": "admin",
            "admin_password": Password.hash("Admin@123456"),  # 使用强密码
            "setup_complete": "false",
            
        }
    },
    'store': {
        "path": Path.file('data/store.db'),
        "object": None,
        "init_data": None
    },
    'api_key': {
        "path": Path.file('data/api_key.db'),
        "object": None,
        "init_data": None
    },
    'session': {
        "path": Path.file('data/session.db'),
        "object": None,
        "init_data": None
    }
}

# 初始化数据库对象
for name, config in data_files.items():
    try:
        config["object"] = Data(config["path"], name)
        if config.get("init_data"):
            for key, value in config["init_data"].items():
                config["object"].set(key, str(value))
        logger.info(f"数据库初始化成功: {name}")
    except Exception as e:
        logger.error(f"数据库初始化失败 {name}: {e}")
        sys.exit(1)

# ==================== 配置管理函数 ====================

def get_config(key: str, default=None):
    """从数据库获取配置值"""
    if not data_files.get('config') or not data_files['config'].get('object'):
        return default
    
    value = data_files['config']['object'].get(key)
    if value is None:
        return default
    
    # 根据值的类型进行转换
    if value.lower() in ('true', 'false'):
        return value.lower() == 'true'
    elif value.isdigit():
        return int(value)
    elif value.replace('.', '').isdigit():
        return float(value)
    else:
        return value

def set_config(key: str, value):
    """设置配置值到数据库"""
    if not data_files.get('config') or not data_files['config'].get('object'):
        return False
    
    # 转换为字符串存储
    if isinstance(value, bool):
        value_str = 'True' if value else 'False'
    elif isinstance(value, (int, float)):
        value_str = str(value)
    else:
        value_str = str(value)
    
    return data_files['config']['object'].set(key, value_str)

def ensure_default_config():
    """确保默认配置存在"""
    if not data_files.get('config') or not data_files['config'].get('object'):
        return
    
    default_config = {
        # 服务器配置
        'HOST': '0.0.0.0',
        'PORT': '5000',
        
        # 安全配置
        'COOKIE_SECURE': 'False',
        'SESSION_TIMEOUT': '3600',
        'RATE_LIMIT': '100',
        'MAX_CONTENT_LENGTH': '16777216',  # 16MB
        
        # 计算配置
        'CALC_TIMEOUT': '5',
        'MAX_WORKERS': '4',
        'MAX_CALC_VALUE': '1000000',
        
        # 绘图配置
        'MAX_IMAGE_SIZE': '2000',
        'MAX_POINTS': '10000',
        'MAX_EXPR_LENGTH': '200',
        
        # 二维码配置
        'MAX_QR_CONTENT': '1000'
    }
    
    config_db = data_files['config']['object']
    for key, value in default_config.items():
        if not config_db.exists(key):
            config_db.set(key, value)

# 确保默认配置存在
ensure_default_config()

# ==================== 检查默认密码 ====================

def check_default_password():
    """检查是否使用了默认密码"""
    stored_password = data_files['config']['object'].get('admin_password')
    setup_complete = data_files['config']['object'].get('setup_complete')
    
    if setup_complete != "true":
        logger.warning("=" * 60)
        logger.warning("首次运行检测")
        logger.warning("=" * 60)
        logger.warning(f"管理员用户名: {data_files['config']['object'].get('admin_user')}")
        logger.warning(f"管理员密码: Admin@123456")
        logger.warning("请及时修改默认密码！")
        logger.warning("=" * 60)
        
        # 标记设置已完成
        data_files['config']['object'].set('setup_complete', 'true')

# ==================== Flask应用初始化 ====================

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = get_config('MAX_CONTENT_LENGTH', 16777216)
app.config['JSON_AS_ASCII'] = False
app.config['JSON_SORT_KEYS'] = False

# ==================== 请求钩子 ====================

@app.before_request
def before_request():
    """请求前处理"""
    # 设置请求开始时间
    request.start_time = time.time()
    
    # 检查Content-Type
    if request.method == 'POST':
        if not request.is_json and not request.form and not request.files:
            if request.content_length and request.content_length > 0:
                return jsonify({'error': '不支持的Content-Type'}), 415
    
    # 检查请求大小
    if request.content_length and request.content_length > get_config('MAX_CONTENT_LENGTH', 16777216):
        return jsonify({'error': '请求过大'}), 413

@app.after_request
def after_request(response):
    """请求后处理"""
    # 添加安全头
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    
    # 添加处理时间
    if hasattr(request, 'start_time'):
        duration = time.time() - request.start_time
        response.headers['X-Processing-Time'] = str(round(duration * 1000, 2))
    
    return response

# ==================== 路由：用户认证 ====================

@app.route("/user/<method>", methods=['POST'])
@rate_limit
def user_auth(method: str):
    """用户认证接口"""
    ip = request.remote_addr
    
    try:
        if method == 'login':
            username = request.form.get('username') or request.json.get('username')
            password = request.form.get('password') or request.json.get('password')
            
            if not username or not password:
                return jsonify({'error': '请提供用户名和密码'}), 400
            
            # 验证用户名密码
            stored_username = data_files['config']['object'].get('admin_user')
            stored_password = data_files['config']['object'].get('admin_password')
            
            if username == stored_username and Password.verify(password, stored_password):
                # 创建会话
                session_token = secrets.token_urlsafe(32)
                session_data = {
                    'id': session_token,
                    'ip': ip,
                    'created_at': time.time(),
                    'last_activity': time.time(),
                    'username': username,
                    'user_agent': request.headers.get('User-Agent', '')
                }
                
                data_files['session']['object'].set(session_token, json.dumps(session_data))
                
                response = make_response(jsonify({
                    'success': True,
                    'message': '登录成功'
                }))
                response.set_cookie(
                    'session_token',
                    session_token,
                    max_age=get_config('SESSION_TIMEOUT', 3600),
                    httponly=True,
                    secure=get_config('COOKIE_SECURE', False),
                    samesite='Lax',
                    path='/'
                )
                
                logger.info(f"IP {ip} 登录成功")
                return response
            
            logger.warning(f"IP {ip} 登录失败")
            return jsonify({'success': False, 'error': '用户名或密码错误'}), 401
        
        elif method == 'logout':
            session_token = request.cookies.get('session_token')
            if session_token:
                data_files['session']['object'].delete(session_token)
            
            response = make_response(jsonify({'success': True}))
            response.set_cookie('session_token', '', max_age=0, path='/')
            
            logger.info(f"IP {ip} 登出")
            return response
        
        elif method == 'status':
            """检查登录状态"""
            session_token = request.cookies.get('session_token')
            if not session_token:
                return jsonify({'authenticated': False})
            
            session_data = data_files['session']['object'].get(session_token)
            if not session_data:
                return jsonify({'authenticated': False})
            
            try:
                session = json.loads(session_data)
                if time.time() - session.get('created_at', 0) <= get_config('SESSION_TIMEOUT', 3600):
                    # 更新最后活动时间
                    session['last_activity'] = time.time()
                    data_files['session']['object'].set(session_token, json.dumps(session))
                    
                    return jsonify({
                        'authenticated': True,
                        'username': session.get('username'),
                        'created_at': session.get('created_at')
                    })
            except Exception:
                pass
            
            return jsonify({'authenticated': False})
        
        else:
            return jsonify({'error': '无效的方法'}), 400
            
    except Exception as e:
        logger.error(f"认证处理错误: {e}")
        return jsonify({'error': '处理失败'}), 500

# ==================== 路由：管理界面 ====================

@app.route('/')
@admin_required
def index():
    """首页重定向"""
    return redirect('/ui')

@app.route('/login')
def login_page():
    """登录页面"""
    return render_template('login.html')

@app.route('/ui')
@admin_required
def ui():
    """管理界面"""
    return render_template('index.html')

# ==================== 路由：管理员API ====================

@app.route('/admin/api_keys', methods=['GET', 'POST', 'DELETE'])
@admin_required
def admin_api_keys():
    """API密钥管理"""
    method = request.args.get('_method') or request.method
    
    try:
        if method == 'GET' or method == 'items':
            """获取所有密钥"""
            return jsonify(APIKey.list_keys())
        
        elif method == 'POST' or method == 'add':
            """添加新密钥"""
            access_for = request.args.get('access_for', 'Any')
            if access_for:
                access_for = [a.strip() for a in access_for.split(',')]
            else:
                access_for = ['Any']
            
            key = APIKey.add(access_for)
            return jsonify({
                'success': True,
                'key': key,
                'message': 'API密钥生成成功'
            })
        
        elif method == 'DELETE' or method == 'set_invalid':
            """禁用密钥"""
            api_key = request.args.get('api_key')
            if not api_key:
                return jsonify({'error': '缺少api_key参数'}), 400
            
            success = APIKey.set_invalid(api_key)
            return jsonify({
                'success': success,
                'message': '密钥已禁用' if success else '密钥不存在'
            })
        
        else:
            return jsonify({'error': '无效的方法'}), 400
            
    except ValidationError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"API密钥管理错误: {e}")
        return jsonify({'error': '操作失败'}), 500

@app.route('/admin/server_status')
@admin_required
def server_status():
    """服务器状态"""
    try:
        # 获取系统信息
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        # 获取进程信息
        process = psutil.Process()
        process_memory = process.memory_info()
        
        # 获取数据库大小
        db_sizes = {}
        for name, config in data_files.items():
            if os.path.exists(config["path"]):
                db_sizes[name] = os.path.getsize(config["path"])
        
        uptime = time.time() - START_RUN_TIME
        uptime_str = str(timedelta(seconds=int(uptime)))
        
        return jsonify({
            'status': 'running',
            'uptime': uptime,
            'uptime_str': uptime_str,
            'start_time': START_RUN_TIME,
            'system': {
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_used': memory.used,
                'memory_total': memory.total,
                'disk_percent': disk.percent,
                'disk_used': disk.used,
                'disk_total': disk.total,
            },
            'process': {
                'cpu_percent': process.cpu_percent(),
                'memory_rss': process_memory.rss,
                'memory_vms': process_memory.vms,
                'threads': process.num_threads(),
            },
            'database': db_sizes,
            'config': {
                'max_workers': get_config('MAX_WORKERS', 4),
                'rate_limit': get_config('RATE_LIMIT', 100),
                'session_timeout': get_config('SESSION_TIMEOUT', 3600),
            }
        })
    except Exception as e:
        logger.error(f"获取服务器状态失败: {e}")
        return jsonify({'error': '获取状态失败'}), 500

@app.route('/admin/change_password', methods=['POST'])
@admin_required
def change_password():
    """修改管理员密码"""
    try:
        old_password = request.json.get('old_password')
        new_password = request.json.get('new_password')
        
        if not old_password or not new_password:
            return jsonify({'error': '请提供旧密码和新密码'}), 400
        
        # 验证旧密码
        stored_password = data_files['config']['object'].get('admin_password')
        if not Password.verify(old_password, stored_password):
            return jsonify({'error': '旧密码错误'}), 401
        
        # 更新密码
        new_hash = Password.hash(new_password)
        data_files['config']['object'].set('admin_password', new_hash)
        
        # 使所有现有会话失效（可选）
        if request.json.get('invalidate_all_sessions', False):
            sessions = data_files['session']['object'].items()
            for key, _ in sessions:
                data_files['session']['object'].delete(key)
        
        logger.info(f"管理员密码已修改，IP: {request.remote_addr}")
        return jsonify({'success': True, 'message': '密码修改成功'})
        
    except ValidationError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"修改密码失败: {e}")
        return jsonify({'error': '修改失败'}), 500

@app.route('/admin/config', methods=['GET', 'POST'])
@admin_required
def admin_config():
    """配置管理"""
    try:
        config = data_files['config']['object']
        
        if request.method == 'GET':
            """获取所有配置"""
            items = config.items()
            result = {}
            for key, value in items:
                try:
                    result[key] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    result[key] = value
            return jsonify(result)
        
        elif request.method == 'POST':
            """设置配置"""
            data = request.json or {}
            if not data:
                return jsonify({'error': '请提供配置数据'}), 400
            
            for key, value in data.items():
                if isinstance(value, (dict, list)):
                    config.set(key, json.dumps(value, ensure_ascii=False))
                else:
                    config.set(key, str(value))
            
            logger.info(f"配置已更新，IP: {request.remote_addr}")
            return jsonify({'success': True, 'message': '配置更新成功'})
    
    except Exception as e:
        logger.error(f"配置管理错误: {e}")
        return jsonify({'error': '操作失败'}), 500

# ==================== 路由：存储API ====================

@app.route('/api/store', methods=['GET', 'POST', 'DELETE'])
@rate_limit
@api_key_required(['store'])
def api_store():
    """键值存储API"""
    method = request.args.get('_method') or request.method
    key = request.args.get('key')
    
    store = data_files['store']['object']
    
    try:
        if method == 'GET':
            """获取值"""
            if not key:
                return jsonify({'error': '缺少key参数'}), 400
            
            value = store.get(key)
            if value is None:
                return jsonify({'error': '键不存在'}), 404
            
            # 尝试解析JSON
            try:
                return jsonify({'value': json.loads(value)})
            except (json.JSONDecodeError, TypeError):
                return jsonify({'value': value})
        
        elif method == 'POST':
            """设置值"""
            value = request.args.get('value')
            if not key or value is None:
                # 尝试从JSON body获取
                data = request.get_json()
                if data:
                    key = data.get('key', key)
                    value = data.get('value', value)
            
            if not key or value is None:
                return jsonify({'error': '缺少key或value参数'}), 400
            
            # 如果是字典或列表，转换为JSON
            if isinstance(value, (dict, list)):
                value = json.dumps(value, ensure_ascii=False)
            
            success = store.set(key, str(value))
            return jsonify({
                'success': success,
                'key': key,
                'message': '存储成功' if success else '存储失败'
            })
        
        elif method == 'DELETE':
            """删除值"""
            if not key:
                return jsonify({'error': '缺少key参数'}), 400
            
            success = store.delete(key)
            return jsonify({
                'success': success,
                'message': '删除成功' if success else '键不存在'
            })
        
        elif method == 'ITEMS':
            """获取所有键值对"""
            items = store.items()
            result = []
            for k, v in items:
                try:
                    result.append({'key': k, 'value': json.loads(v)})
                except (json.JSONDecodeError, TypeError):
                    result.append({'key': k, 'value': v})
            return jsonify(result)
        
        elif method == 'CLEAR_EXPIRED':
            """清理过期数据"""
            max_age = int(request.args.get('max_age', 86400))
            deleted = store.clear_expired(max_age)
            return jsonify({'deleted': deleted})
        
        else:
            return jsonify({'error': '无效的方法'}), 400
            
    except Exception as e:
        logger.error(f"存储API错误: {e}")
        return jsonify({'error': '操作失败'}), 500

# ==================== 绘图辅助函数 ====================

def handle_draw_commands(commands: List[Tuple[str, Tuple]], width: int, height: int):
    """处理绘图命令"""
    # 验证点数
    max_points = get_config('MAX_POINTS', 10000)
    if len(commands) > max_points:
        return jsonify({'error': f'点数过多，最大允许{max_points}'}), 400
    
    try:
        # 创建图像
        image = Image.new('RGB', (width, height), color='white')
        draw = ImageDraw.Draw(image)
        
        valid_commands = 0
        for cmd_type, args in commands:
            try:
                if cmd_type == 'line':
                    x1, y1, x2, y2 = map(int, args)
                    # 验证坐标范围
                    if not (0 <= x1 < width and 0 <= y1 < height and 
                           0 <= x2 < width and 0 <= y2 < height):
                        logger.warning(f"坐标超出范围: ({x1},{y1})-({x2},{y2})")
                        continue
                    draw.line([(x1, y1), (x2, y2)], fill='black', width=2)
                    valid_commands += 1
                
                elif cmd_type == 'point':
                    x, y = map(int, args)
                    if not (0 <= x < width and 0 <= y < height):
                        logger.warning(f"坐标超出范围: ({x},{y})")
                        continue
                    # 绘制一个小的圆点
                    draw.ellipse([(x-2, y-2), (x+2, y+2)], fill='black', outline='black')
                    valid_commands += 1
                    
            except ValueError as e:
                logger.warning(f"无效的坐标参数: {e}")
                continue
            except Exception as e:
                logger.error(f"绘图命令错误: {e}")
                continue
        
        # 检查是否有有效的命令被执行
        if valid_commands == 0:
            return jsonify({'error': '所有绘图命令都无法执行'}), 400
        
        # 返回图像
        img_io = io.BytesIO()
        image.save(img_io, 'PNG', optimize=True)
        img_io.seek(0)
        
        response = send_file(img_io, mimetype='image/png')
        response.headers['Cache-Control'] = 'public, max-age=300'
        return response
        
    except Exception as e:
        logger.error(f"绘图处理错误: {e}")
        return jsonify({'error': '图像生成失败'}), 500

def handle_math_commands(commands: List[str], width: int, height: int):
    """处理数学函数绘图"""
    try:
        # 创建图形
        dpi = 100
        fig_width = max(4, width / dpi)
        fig_height = max(3, height / dpi)
        
        # 使用非交互式后端
        plt.switch_backend('Agg')
        fig, ax = plt.subplots(figsize=(fig_width, fig_height), dpi=dpi)
        
        colors = plt.cm.tab10(np.linspace(0, 1, len(commands)))
        
        valid_expressions = 0
        for i, expr in enumerate(commands):
            try:
                # 智能选择x值范围
                x = SafeMathEvaluator._get_smart_x_range(expr)
                
                # 安全求值
                y = SafeMathEvaluator.evaluate(expr, x)
                
                # 过滤无效值
                valid_mask = np.isfinite(y)
                if not np.any(valid_mask):
                    logger.warning(f"表达式生成无效值: {expr}")
                    continue
                
                # 只绘制有效部分
                x_valid = x[valid_mask]
                y_valid = y[valid_mask]
                
                # 绘制
                ax.plot(x_valid, y_valid, linewidth=2, color=colors[i], label=f'f(x)={expr}')
                valid_expressions += 1
                
            except ValidationError as e:
                logger.warning(f"数学表达式验证错误: {expr}, {e}")
                # 继续处理其他表达式，而不是立即返回错误
                continue
            except SyntaxError as e:
                logger.warning(f"数学表达式语法错误: {expr}, {e}")
                # 继续处理其他表达式，而不是立即返回错误
                continue
            except Exception as e:
                logger.warning(f"数学表达式错误: {expr}, {e}")
                # 继续处理其他表达式，而不是立即返回错误
                continue
        
        # 检查是否有有效的表达式被绘制
        if valid_expressions == 0:
            # 提供具体的修复建议
            suggestions = [
                '使用标准数学函数如 sin(x), cos(x), log(x), sqrt(x)',
                '确保括号匹配正确',
                '避免使用复杂的LaTeX格式，使用Python风格的表达式',
                '检查表达式中的变量名是否正确',
                '在分数和变量之间添加乘法符号，如 (pi)/(6)*x 而不是 (pi)/(6)x'
            ]
            
            # 如果表达式包含复杂的LaTeX格式，提供简化版本建议
            if any(cmd in ' '.join(commands) for cmd in ['\\frac', '\\left', '\\right', '\\sqrt']):
                suggestions.append('建议使用简化版本: sin(pi/6*x)**2 + cos(2*pi/5*x) - sqrt(3)/2*sin(pi/4*x+pi/3) + cos(pi/8*x)**2/(1+sin(pi/12*x))')
            
            return jsonify({
                'error': '所有表达式都无法正确绘制',
                'details': '请检查表达式语法和数学函数的使用',
                'suggestions': suggestions,
                'supported_functions': ['sin', 'cos', 'tan', 'log', 'ln', 'exp', 'sqrt', 'abs', 'pi', 'e'],
                'example': 'sin(x)**2 + cos(2*x) - sqrt(3)/2*sin(x+pi/3)'
            }), 400
        
        ax.set_xlabel('x', fontsize=12)
        ax.set_ylabel('y', fontsize=12)
        ax.set_title('数学函数绘图', fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.axhline(y=0, color='k', linewidth=0.5, alpha=0.5)
        ax.axvline(x=0, color='k', linewidth=0.5, alpha=0.5)
        
        # 设置显示范围
        ax.set_xlim(-10, 10)
        
        # 添加图例
        if len(commands) > 1:
            ax.legend(loc='best', fontsize=10)
        
        # 保存图像
        img_io = io.BytesIO()
        plt.savefig(img_io, format='png', bbox_inches='tight', pad_inches=0.1, dpi=dpi)
        plt.close(fig)
        img_io.seek(0)
        
        response = send_file(img_io, mimetype='image/png')
        response.headers['Cache-Control'] = 'public, max-age=300'
        return response
        
    except Exception as e:
        logger.error(f"数学绘图错误: {e}")
        return jsonify({'error': f'绘图失败: {e}'}), 500

# ==================== 绘图命令解析类 ====================

class DrawCommand:
    """绘图命令解析器"""
    
    COMMAND_PATTERNS = {
        'line': re.compile(r'^line\((\d+),(\d+),(\d+),(\d+)\)$'),
        'point': re.compile(r'^point\((\d+),(\d+)\)$'),
        'math': re.compile(r'^math\((.+)\)$')
    }
    
    @classmethod
    def parse(cls, command: str):
        """解析绘图命令"""
        command = command.strip()
        if not command:
            return None, None
        
        for cmd_type, pattern in cls.COMMAND_PATTERNS.items():
            match = pattern.match(command)
            if match:
                return cmd_type, match.groups()
        return None, None

# ==================== 路由：绘图API ====================

@app.route('/api/draw', methods=['GET'])
@rate_limit
@api_key_required(['draw'])
def api_draw():
    """绘图API"""
    commands_str = request.args.get('object', '')
    if not commands_str:
        return jsonify({'error': '缺少object参数'}), 400
    
    commands = commands_str.split(';')
    width = min(int(request.args.get('width', 500)), get_config('MAX_IMAGE_SIZE', 2000))
    height = min(int(request.args.get('height', 500)), get_config('MAX_IMAGE_SIZE', 2000))
    
    # 验证图像尺寸
    if width <= 0 or height <= 0:
        return jsonify({'error': '图像尺寸必须为正数'}), 400
    
    # 解析命令并分类
    draw_commands = []
    math_commands = []
    invalid_commands = []
    
    for cmd in commands:
        if not cmd.strip():
            continue
            
        # 修复URL编码问题：将空格转换回加号
        cmd = cmd.replace(' ', '+')
            
        cmd_type, args = DrawCommand.parse(cmd)
        if cmd_type == 'math':
            math_commands.append(args[0])
        elif cmd_type in ('line', 'point'):
            draw_commands.append((cmd_type, args))
        else:
            invalid_commands.append(cmd)
    
    # 检查无效命令
    if invalid_commands:
        return jsonify({'error': f'无效的绘图命令: {", ".join(invalid_commands[:3])}'}), 400
    
    # 不能同时包含draw和math命令
    if draw_commands and math_commands:
        return jsonify({'error': '不能同时包含绘图和数学命令'}), 400
    
    # 检查是否有命令
    if not draw_commands and not math_commands:
        return jsonify({'error': '没有有效的绘图命令'}), 400
    
    try:
        # 处理绘图命令
        if draw_commands:
            # 检查缓存
            cache_key = f"draw:{hash(frozenset(draw_commands))}:{width}:{height}"
            cached = cache_manager.get(cache_key)
            if cached:
                response = send_file(io.BytesIO(cached), mimetype='image/png')
                response.headers['X-Cache'] = 'HIT'
                return response
            
            result = handle_draw_commands(draw_commands, width, height)
            
            # 缓存结果
            if isinstance(result, tuple) and result[1] == 200:
                # 成功响应，缓存图像数据
                pass  # 实际缓存需要获取图像数据
            
            return result
        
        # 处理数学命令
        if math_commands:
            # 检查缓存
            cache_key = f"math:{hash(frozenset(math_commands))}:{width}:{height}"
            cached = cache_manager.get(cache_key)
            if cached:
                response = send_file(io.BytesIO(cached), mimetype='image/png')
                response.headers['X-Cache'] = 'HIT'
                return response
            
            return handle_math_commands(math_commands, width, height)
            
    except Exception as e:
        logger.error(f"绘图API错误: {e}")
        return jsonify({'error': '绘图处理失败'}), 500

# ==================== 路由：计算API ====================

@app.route('/api/calc/<method>', methods=['GET'])
@rate_limit
@api_key_required(['calc'])
def api_calc(method: str):
    """计算API"""
    try:
        timeout = min(
            float(request.args.get('timeout', get_config('CALC_TIMEOUT', 5))),
            10  # 限制最大超时时间
        )
        
        # 检查缓存
        cache_key = f"calc:{method}:{request.query_string.decode()}"
        cached = cache_manager.get(cache_key)
        if cached is not None:
            return jsonify({'result': cached, 'cached': True})
        
        if method == 'pow':
            x = float(request.args.get('x'))
            y = float(request.args.get('y'))
            
            # 验证输入范围
            if abs(x) > get_config('MAX_CALC_VALUE', 1000000) or abs(y) > get_config('MAX_CALC_VALUE', 1000000):
                return jsonify({'error': f'数值过大，最大允许{get_config("MAX_CALC_VALUE", 1000000)}'}), 400
            
            # 使用线程池执行计算
            future = thread_pool.submit(math.pow, x, y)
            try:
                result = future.result(timeout=timeout)
                cache_manager.set(cache_key, result, ttl=60)  # 缓存1分钟
                return jsonify({'result': result})
            except TimeoutError:
                future.cancel()
                return jsonify({'error': '计算超时'}), 408
        
        elif method == 'sqrt':
            x = float(request.args.get('x'))
            if x < 0:
                return jsonify({'error': '不能对负数开平方'}), 400
            if x > get_config('MAX_CALC_VALUE', 1000000) ** 2:
                return jsonify({'error': '数值过大'}), 400
            
            result = math.sqrt(x)
            cache_manager.set(cache_key, result, ttl=60)
            return jsonify({'result': result})
        
        elif method == 'log':
            a = float(request.args.get('a', 10))  # 底数，默认为10
            b = float(request.args.get('b'))  # 真数
            
            if a <= 0 or a == 1 or b <= 0:
                return jsonify({'error': '无效的对数参数'}), 400
            if a > get_config('MAX_CALC_VALUE', 1000000) or b > get_config('MAX_CALC_VALUE', 1000000):
                return jsonify({'error': '数值过大'}), 400
            
            future = thread_pool.submit(math.log, b, a)
            try:
                result = future.result(timeout=timeout)
                cache_manager.set(cache_key, result, ttl=60)
                return jsonify({'result': result})
            except TimeoutError:
                future.cancel()
                return jsonify({'error': '计算超时'}), 408
        
        elif method == 'sin':
            x = float(request.args.get('x'))
            result = math.sin(x)
            cache_manager.set(cache_key, result, ttl=60)
            return jsonify({'result': result})
        
        elif method == 'cos':
            x = float(request.args.get('x'))
            result = math.cos(x)
            cache_manager.set(cache_key, result, ttl=60)
            return jsonify({'result': result})
        
        elif method == 'tan':
            x = float(request.args.get('x'))
            # 检查无效值
            if abs(math.cos(x)) < 1e-10:
                return jsonify({'error': 'tan函数在这一点未定义'}), 400
            result = math.tan(x)
            cache_manager.set(cache_key, result, ttl=60)
            return jsonify({'result': result})
        
        else:
            return jsonify({'error': '无效的计算方法'}), 400
            
    except ValueError as e:
        return jsonify({'error': f'无效的输入参数: {e}'}), 400
    except OverflowError:
        return jsonify({'error': '计算结果溢出'}), 400
    except Exception as e:
        logger.error(f"计算错误: {e}")
        return jsonify({'error': '计算失败'}), 500

# ==================== 路由：二维码API ====================

@app.route('/api/qr_code', methods=['GET'])
@rate_limit
@api_key_required(['qr_code'])
def api_qr_code():
    """二维码生成API"""
    content = request.args.get('content')
    
    if not content:
        return jsonify({'error': '缺少content参数'}), 400
    
    # 限制内容长度
    if len(content) > get_config('MAX_QR_CONTENT', 1000):
        return jsonify({'error': f'内容过长，最大允许{get_config("MAX_QR_CONTENT", 1000)}字符'}), 400
    
    # 检查缓存
    cache_key = f"qr:{hash(content)}"
    cached = cache_manager.get(cache_key)
    if cached:
        response = send_file(io.BytesIO(cached), mimetype='image/png')
        response.headers['X-Cache'] = 'HIT'
        return response
    
    try:
        # 获取参数
        box_size = min(int(request.args.get('box_size', 10)), 20)
        border = min(int(request.args.get('border', 4)), 10)
        error_correction = request.args.get('error_correction', 'L')
        
        # 纠错级别映射
        ec_map = {
            'L': qrcode.constants.ERROR_CORRECT_L,
            'M': qrcode.constants.ERROR_CORRECT_M,
            'Q': qrcode.constants.ERROR_CORRECT_Q,
            'H': qrcode.constants.ERROR_CORRECT_H
        }
        
        # 创建二维码
        qr = qrcode.QRCode(
            version=None,
            error_correction=ec_map.get(error_correction, qrcode.constants.ERROR_CORRECT_L),
            box_size=box_size,
            border=border,
        )
        qr.add_data(content)
        qr.make(fit=True)
        
        # 生成图像
        img = qr.make_image(fill_color="black", back_color="white")
        
        # 调整大小
        if request.args.get('size'):
            size = min(int(request.args.get('size')), 1000)
            img = img.resize((size, size), Image.Resampling.LANCZOS)
        
        # 返回图像
        img_io = io.BytesIO()
        img.save(img_io, 'PNG', optimize=True)
        img_io.seek(0)
        
        # 缓存图像数据
        cache_manager.set(cache_key, img_io.getvalue(), ttl=3600)  # 缓存1小时
        
        response = send_file(img_io, mimetype='image/png')
        response.headers['Cache-Control'] = 'public, max-age=3600'
        response.headers['X-Cache'] = 'MISS'
        return response
        
    except Exception as e:
        logger.error(f"二维码生成错误: {e}")
        return jsonify({'error': '二维码生成失败'}), 500

# ==================== 路由：批量操作API ====================

@app.route('/api/batch', methods=['POST'])
@rate_limit
@api_key_required(['store', 'calc'])
def api_batch():
    """批量操作API"""
    try:
        data = request.get_json()
        if not data or 'operations' not in data:
            return jsonify({'error': '缺少operations参数'}), 400
        
        operations = data['operations']
        if not isinstance(operations, list):
            return jsonify({'error': 'operations必须是数组'}), 400
        
        if len(operations) > 50:
            return jsonify({'error': '批量操作不能超过50个'}), 400
        
        results = []
        store = data_files['store']['object']
        
        for i, op in enumerate(operations):
            try:
                op_type = op.get('type')
                if op_type == 'store_get':
                    key = op.get('key')
                    if not key:
                        results.append({'index': i, 'error': '缺少key'})
                    else:
                        value = store.get(key)
                        results.append({'index': i, 'result': value})
                
                elif op_type == 'store_set':
                    key = op.get('key')
                    value = op.get('value')
                    if not key or value is None:
                        results.append({'index': i, 'error': '缺少key或value'})
                    else:
                        success = store.set(key, str(value))
                        results.append({'index': i, 'success': success})
                
                elif op_type == 'calc':
                    method = op.get('method')
                    params = op.get('params', {})
                    
                    if method == 'pow':
                        x = float(params.get('x', 0))
                        y = float(params.get('y', 0))
                        results.append({'index': i, 'result': math.pow(x, y)})
                    elif method == 'sqrt':
                        x = float(params.get('x', 0))
                        results.append({'index': i, 'result': math.sqrt(x)})
                    else:
                        results.append({'index': i, 'error': f'未知计算方法: {method}'})
                
                else:
                    results.append({'index': i, 'error': f'未知操作类型: {op_type}'})
                    
            except Exception as e:
                results.append({'index': i, 'error': str(e)})
        
        return jsonify({'results': results})
        
    except Exception as e:
        logger.error(f"批量操作错误: {e}")
        return jsonify({'error': '批量操作失败'}), 500

# ==================== 错误处理器 ====================

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': '接口不存在'}), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({'error': '方法不允许'}), 405

@app.errorhandler(413)
def too_large(error):
    return jsonify({'error': '请求过大'}), 413

@app.errorhandler(429)
def too_many_requests(error):
    return jsonify({'error': '请求过于频繁'}), 429

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"内部错误: {error}")
    return jsonify({'error': '服务器内部错误'}), 500

# ==================== 健康检查 ====================

@app.route('/health')
@admin_required
def health_check():
    """健康检查接口"""
    status = {
        'status': 'healthy',
        'timestamp': time.time(),
        'uptime': time.time() - START_RUN_TIME,
        'version': '1.0.0'
    }
    
    # 检查数据库连接
    db_status = {}
    all_db_ok = True
    
    for name, config in data_files.items():
        try:
            if config["object"]:
                # 简单查询测试
                config["object"].exists('health_check')
                db_status[name] = 'ok'
        except Exception as e:
            db_status[name] = 'error'
            all_db_ok = False
            logger.error(f"数据库健康检查失败 {name}: {e}")
    
    status['database'] = db_status
    
    # 检查磁盘空间
    try:
        disk = psutil.disk_usage(Path.dir('data'))
        if disk.free < 100 * 1024 * 1024:  # 小于100MB
            status['disk_warning'] = '磁盘空间不足'
            status['disk_free'] = disk.free
    except:
        pass
    
    if not all_db_ok:
        status['status'] = 'degraded'
    
    return jsonify(status)

# ==================== 清理函数 ====================

def cleanup_resources():
    """清理资源"""
    logger.info("开始清理资源...")
    
    try:
        # 关闭所有数据库连接
        DatabasePool.close_all()
        logger.info("数据库连接已关闭")
        
        # 关闭线程池
        thread_pool.shutdown(wait=True)
        logger.info("线程池已关闭")
        
        # 清理临时文件
        if os.path.exists(Path.dir('temp')):
            shutil.rmtree(Path.dir('temp'))
            os.makedirs(Path.dir('temp'), exist_ok=True)
            logger.info("临时文件已清理")
        
        # 清理过期的会话
        try:
            session_store = data_files['session']['object']
            deleted = session_store.clear_expired(get_config('SESSION_TIMEOUT', 3600))
            logger.info(f"已清理 {deleted} 个过期会话")
        except Exception as e:
            logger.error(f"清理会话失败: {e}")
        
        logger.info("资源清理完成")
        
    except Exception as e:
        logger.error(f"清理资源时出错: {e}")

def signal_handler(signum, frame):
    """信号处理函数"""
    logger.info(f"接收到信号 {signum}，准备关闭...")
    cleanup_resources()
    sys.exit(0)

# ==================== 启动函数 ====================

def main():
    """主函数"""
    try:
        # 注册信号处理
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # 注册清理函数
        atexit.register(cleanup_resources)
        
        # 检查默认密码
        check_default_password()
        
        # 清理过期会话
        try:
            session_store = data_files['session']['object']
            session_store.clear_expired(get_config('SESSION_TIMEOUT', 3600))
        except Exception as e:
            logger.error(f"清理会话失败: {e}")
        
        # 获取配置
        port = int(data_files['config']['object'].get('port', '5000'))
        host = data_files['config']['object'].get('host', '0.0.0.0')
        
        # 验证端口
        if port < 1024 or port > 65535:
            logger.warning(f"端口 {port} 无效，使用默认端口 5000")
            port = 5000
        
        logger.info("=" * 50)
        logger.info(f"服务启动在 {host}:{port}")
        logger.info(f"数据目录: {Path.dir('data')}")
        logger.info(f"日志目录: {Path.dir('log')}")
        logger.info(f"临时目录: {Path.dir('temp')}")
        logger.info("=" * 50)
        
        # 输出API文档
        logger.info("可用API接口:")
        logger.info("  - GET  /health                 健康检查")
        logger.info("  - POST /user/login             用户登录")
        logger.info("  - POST /user/logout            用户登出")
        logger.info("  - POST /user/status            登录状态")
        logger.info("  - GET  /api/store              存储API")
        logger.info("  - GET  /api/draw               绘图API")
        logger.info("  - GET  /api/calc/<method>      计算API")
        logger.info("  - GET  /api/qr_code            二维码API")
        logger.info("  - POST /api/batch              批量操作API")
        logger.info("=" * 50)
        
        # 运行应用
        debug = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
        app.run(
            debug=debug,
            port=port,
            host=host,
            threaded=True,
            use_reloader=False  # 生产环境禁用重载器
        )
        
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在关闭...")
        cleanup_resources()
        logger.info("服务已关闭")
    except Exception as e:
        logger.error(f"启动失败: {e}")
        cleanup_resources()
        sys.exit(1)

if __name__ == '__main__':
    main()