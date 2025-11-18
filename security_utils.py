"""
Security utilities module
Provides logging, validation, and security functions
"""

import logging
import time
import hashlib
import html
from functools import wraps
from pathlib import Path
from typing import Optional, Callable, Any
from threading import Lock
import streamlit as st

# ============================================================================
# КОНСТАНТЫ БЕЗОПАСНОСТИ
# ============================================================================

MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB
MAX_FILES_COUNT = 10
MAX_ROWS_PER_FILE = 100000
MAX_SESSION_DF_SIZE = 1000

ALLOWED_FILE_EXTENSIONS = ['.xlsx', '.csv', '.xls']
ALLOWED_IMAGE_EXTENSIONS = ['.png', '.jpg', '.jpeg', '.eps']

# Whitelisted domains for external requests
ALLOWED_API_DOMAINS = ['api.hh.ru']

# ============================================================================
# ЛОГИРОВАНИЕ
# ============================================================================

def setup_logging(log_dir: str = 'logs', log_level: int = logging.INFO) -> logging.Logger:
    """
    Настройка системы логирования с ротацией файлов

    Args:
        log_dir: Директория для файлов логов
        log_level: Уровень логирования (INFO, DEBUG, WARNING, ERROR)

    Returns:
        Настроенный logger объект
    """
    from logging.handlers import RotatingFileHandler
    import sys

    # Создаем директорию для логов
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)

    # Создаем logger
    logger = logging.getLogger('vr_multitool')
    logger.setLevel(log_level)

    # Избегаем дублирования handlers
    if logger.handlers:
        return logger

    # Форматтер
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - '
        '%(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler с ротацией (5 файлов по 10 MB)
    file_handler = RotatingFileHandler(
        log_path / 'app.log',
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Security events handler
    security_handler = RotatingFileHandler(
        log_path / 'security.log',
        maxBytes=10*1024*1024,
        backupCount=10,
        encoding='utf-8'
    )
    security_handler.setLevel(logging.WARNING)
    security_handler.setFormatter(formatter)
    logger.addHandler(security_handler)

    return logger


# Глобальный logger
logger = setup_logging()


def log_security_event(event_type: str, details: str, severity: str = 'WARNING') -> None:
    """
    Логирование security событий

    Args:
        event_type: Тип события (file_upload, api_call, validation_failed, etc)
        details: Детали события
        severity: Уровень (INFO, WARNING, ERROR, CRITICAL)
    """
    log_message = f"[SECURITY] {event_type}: {details}"

    if severity == 'CRITICAL':
        logger.critical(log_message)
    elif severity == 'ERROR':
        logger.error(log_message)
    elif severity == 'WARNING':
        logger.warning(log_message)
    else:
        logger.info(log_message)


# ============================================================================
# ВАЛИДАЦИЯ ФАЙЛОВ
# ============================================================================

def validate_file_size(file_size: int, max_size: int = MAX_FILE_SIZE) -> tuple[bool, str]:
    """
    Валидация размера файла

    Args:
        file_size: Размер файла в байтах
        max_size: Максимальный разрешенный размер

    Returns:
        (valid, error_message)
    """
    if file_size > max_size:
        max_mb = max_size / 1024 / 1024
        actual_mb = file_size / 1024 / 1024
        error = f"Файл слишком большой ({actual_mb:.1f} MB). Максимум {max_mb:.0f} MB"
        log_security_event('file_size_exceeded', error, 'WARNING')
        return False, error
    return True, ""


def validate_file_extension(filename: str, allowed_extensions: list[str]) -> tuple[bool, str]:
    """
    Валидация расширения файла

    Args:
        filename: Имя файла
        allowed_extensions: Список разрешенных расширений (с точкой)

    Returns:
        (valid, error_message)
    """
    file_path = Path(filename)
    ext = file_path.suffix.lower()

    if ext not in allowed_extensions:
        error = f"Недопустимое расширение {ext}. Разрешены: {', '.join(allowed_extensions)}"
        log_security_event('invalid_file_extension', f"{filename}: {error}", 'WARNING')
        return False, error
    return True, ""


def validate_safe_path(filepath: str, base_dir: Optional[Path] = None) -> tuple[bool, str]:
    """
    Проверка пути на path traversal атаки

    Args:
        filepath: Путь к файлу
        base_dir: Базовая директория (если None, используется текущая)

    Returns:
        (valid, error_message)
    """
    if base_dir is None:
        base_dir = Path(__file__).parent

    # Проверка на опасные символы
    if '..' in filepath or filepath.startswith('/') or filepath.startswith('\\'):
        error = f"Недопустимый путь: {filepath}"
        log_security_event('path_traversal_attempt', error, 'ERROR')
        return False, error

    try:
        # Проверка что путь внутри базовой директории
        full_path = (base_dir / filepath).resolve()
        if not full_path.is_relative_to(base_dir.resolve()):
            error = f"Путь вне разрешенной директории: {filepath}"
            log_security_event('path_traversal_attempt', error, 'ERROR')
            return False, error
    except (ValueError, OSError) as e:
        error = f"Некорректный путь: {filepath}"
        log_security_event('invalid_path', f"{error}: {e}", 'WARNING')
        return False, error

    return True, ""


# ============================================================================
# ВАЛИДАЦИЯ ДАННЫХ
# ============================================================================

def validate_city_name(name: str, max_length: int = 100) -> tuple[bool, str]:
    """
    Валидация названия города

    Args:
        name: Название города
        max_length: Максимальная длина

    Returns:
        (valid, error_message)
    """
    import re

    if not name or len(name) < 2:
        return False, "Название слишком короткое (минимум 2 символа)"

    if len(name) > max_length:
        return False, f"Название слишком длинное (максимум {max_length} символов)"

    # Только буквы, пробелы, дефисы, точки, скобки
    if not re.match(r'^[А-Яа-яЁёA-Za-z\s\-\.\(\)]+$', name):
        return False, "Недопустимые символы в названии"

    return True, ""


def sanitize_html(text: str) -> str:
    """
    Санитизация HTML для защиты от XSS

    Args:
        text: Входной текст

    Returns:
        Безопасный текст с экранированным HTML
    """
    return html.escape(str(text))


def sanitize_csv_content(df) -> Any:
    """
    Защита от CSV injection (формулы Excel)

    Args:
        df: Pandas DataFrame

    Returns:
        Sanitized DataFrame
    """
    import pandas as pd

    for col in df.columns:
        if df[col].dtype == 'object':
            # Удаляем формулы Excel (=, +, -, @)
            df[col] = df[col].astype(str).str.replace(
                r'^[=+\-@]', '', regex=True
            )

    return df


# ============================================================================
# RATE LIMITING
# ============================================================================

class RateLimiter:
    """
    Rate limiter для защиты от чрезмерных запросов к API
    """

    def __init__(self, max_calls: int, period: int):
        """
        Args:
            max_calls: Максимальное количество вызовов
            period: Период в секундах
        """
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = Lock()

    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            with self.lock:
                now = time.time()

                # Удаляем старые вызовы
                self.calls = [c for c in self.calls if c > now - self.period]

                # Проверяем лимит
                if len(self.calls) >= self.max_calls:
                    sleep_time = self.calls[0] + self.period - now
                    log_security_event(
                        'rate_limit_exceeded',
                        f"Функция {func.__name__} превысила лимит ({self.max_calls} вызовов за {self.period}с)",
                        'WARNING'
                    )
                    time.sleep(sleep_time)

                self.calls.append(now)

            return func(*args, **kwargs)

        return wrapper


# ============================================================================
# ДЕКОРАТОРЫ МОНИТОРИНГА
# ============================================================================

def monitor_performance(func: Callable) -> Callable:
    """
    Декоратор для мониторинга производительности функций
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        func_name = func.__name__

        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time

            if execution_time > 1.0:  # Логируем только медленные функции
                logger.info(f"Функция {func_name} выполнена за {execution_time:.2f}с")

            return result

        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(
                f"Ошибка в {func_name} после {execution_time:.2f}с: {e}",
                exc_info=True
            )
            raise

    return wrapper


# ============================================================================
# УТИЛИТЫ
# ============================================================================

def get_file_hash(file_content: bytes) -> str:
    """
    Вычисление SHA256 хеша файла

    Args:
        file_content: Содержимое файла в байтах

    Returns:
        SHA256 хеш в hex формате
    """
    return hashlib.sha256(file_content).hexdigest()


def mask_sensitive_data(data: str, visible_chars: int = 4) -> str:
    """
    Маскирование чувствительных данных для логирования

    Args:
        data: Исходные данные
        visible_chars: Количество видимых символов в начале

    Returns:
        Замаскированная строка
    """
    if len(data) <= visible_chars:
        return '*' * len(data)
    return data[:visible_chars] + '*' * (len(data) - visible_chars)


# ============================================================================
# SESSION STATE MANAGEMENT
# ============================================================================

# Лимиты для session state
MAX_SESSION_LIST_SIZE = 10000  # Максимальный размер списка в session_state
MAX_SESSION_KEYS = 100  # Максимальное количество ключей в session_state
MAX_SESSION_STRING_LENGTH = 100000  # Максимальная длина строки


def safe_session_append(key: str, value: Any, max_size: int = MAX_SESSION_LIST_SIZE) -> bool:
    """
    Безопасное добавление элемента в список в session_state с проверкой лимита

    Args:
        key: Ключ в session_state
        value: Значение для добавления
        max_size: Максимальный размер списка

    Returns:
        True если успешно, False если достигнут лимит
    """
    try:
        # Инициализация если не существует
        if key not in st.session_state:
            st.session_state[key] = []

        # Проверка типа
        if not isinstance(st.session_state[key], list):
            logger.error(f"session_state[{key}] не является списком")
            return False

        # Проверка лимита
        if len(st.session_state[key]) >= max_size:
            logger.warning(f"Достигнут лимит размера списка для {key}: {max_size}")
            log_security_event('session_limit_reached', f"Key: {key}, Size: {len(st.session_state[key])}", 'WARNING')
            return False

        # Добавление элемента
        st.session_state[key].append(value)
        logger.debug(f"Добавлен элемент в session_state[{key}], текущий размер: {len(st.session_state[key])}")
        return True

    except Exception as e:
        logger.error(f"Ошибка при добавлении в session_state[{key}]: {e}", exc_info=True)
        return False


def cleanup_session_state(preserve_keys: list = None) -> int:
    """
    Очистка session_state для предотвращения утечек памяти

    Args:
        preserve_keys: Список ключей, которые нужно сохранить

    Returns:
        Количество удаленных ключей
    """
    if preserve_keys is None:
        preserve_keys = []

    keys_to_delete = []

    try:
        # Поиск ключей для удаления
        for key in st.session_state.keys():
            if key not in preserve_keys:
                keys_to_delete.append(key)

        # Удаление ключей
        deleted_count = 0
        for key in keys_to_delete:
            try:
                del st.session_state[key]
                deleted_count += 1
            except Exception as e:
                logger.error(f"Ошибка при удалении session_state[{key}]: {e}")

        if deleted_count > 0:
            logger.info(f"Очищено {deleted_count} ключей из session_state")
            log_security_event('session_cleanup', f"Удалено ключей: {deleted_count}", 'INFO')

        return deleted_count

    except Exception as e:
        logger.error(f"Ошибка при очистке session_state: {e}", exc_info=True)
        return 0


def check_session_state_limits() -> dict:
    """
    Проверка лимитов session_state и возврат статистики

    Returns:
        Словарь со статистикой: {
            'total_keys': int,
            'oversized_lists': list,
            'warnings': list
        }
    """
    stats = {
        'total_keys': 0,
        'oversized_lists': [],
        'warnings': []
    }

    try:
        stats['total_keys'] = len(st.session_state.keys())

        # Проверка количества ключей
        if stats['total_keys'] > MAX_SESSION_KEYS:
            warning = f"Превышено количество ключей в session_state: {stats['total_keys']} > {MAX_SESSION_KEYS}"
            stats['warnings'].append(warning)
            logger.warning(warning)
            log_security_event('session_keys_exceeded', warning, 'WARNING')

        # Проверка размеров списков
        for key, value in st.session_state.items():
            if isinstance(value, list) and len(value) > MAX_SESSION_LIST_SIZE:
                oversized_info = f"{key}: {len(value)} элементов"
                stats['oversized_lists'].append(oversized_info)
                logger.warning(f"Превышен размер списка в session_state: {oversized_info}")

        # Проверка размеров строк
        for key, value in st.session_state.items():
            if isinstance(value, str) and len(value) > MAX_SESSION_STRING_LENGTH:
                warning = f"Превышена длина строки для {key}: {len(value)} > {MAX_SESSION_STRING_LENGTH}"
                stats['warnings'].append(warning)
                logger.warning(warning)

    except Exception as e:
        logger.error(f"Ошибка при проверке session_state: {e}", exc_info=True)

    return stats


def validate_dataframe_size(df, max_rows: int = MAX_ROWS_PER_FILE,
                           max_columns: int = 100) -> tuple[bool, str]:
    """
    Валидация размера DataFrame для предотвращения DoS

    Args:
        df: DataFrame для проверки
        max_rows: Максимальное количество строк
        max_columns: Максимальное количество столбцов

    Returns:
        (is_valid, error_message)
    """
    try:
        if df is None:
            return False, "DataFrame пустой"

        rows, cols = df.shape

        # Проверка количества строк
        if rows > max_rows:
            error = f"Слишком много строк: {rows} > {max_rows}"
            logger.warning(f"DataFrame validation failed: {error}")
            log_security_event('dataframe_too_large', error, 'WARNING')
            return False, error

        # Проверка количества столбцов
        if cols > max_columns:
            error = f"Слишком много столбцов: {cols} > {max_columns}"
            logger.warning(f"DataFrame validation failed: {error}")
            log_security_event('dataframe_too_wide', error, 'WARNING')
            return False, error

        # Проверка общего размера
        total_cells = rows * cols
        max_cells = max_rows * max_columns
        if total_cells > max_cells:
            error = f"DataFrame слишком большой: {total_cells} ячеек > {max_cells}"
            logger.warning(f"DataFrame validation failed: {error}")
            log_security_event('dataframe_excessive_cells', error, 'WARNING')
            return False, error

        logger.debug(f"DataFrame validation passed: {rows} rows, {cols} columns")
        return True, ""

    except Exception as e:
        error = f"Ошибка валидации DataFrame: {e}"
        logger.error(error, exc_info=True)
        return False, error
