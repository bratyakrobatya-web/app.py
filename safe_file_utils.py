"""
Safe file operations module
Защита от Path Traversal атак
"""

from pathlib import Path
from typing import Optional, Union
from PIL import Image
import pandas as pd
from security_utils import logger, log_security_event, validate_safe_path

# Определяем базовые директории проекта
BASE_DIR = Path(__file__).parent
ASSETS_DIR = BASE_DIR / "assets"
DATA_DIR = BASE_DIR / "data"
STATIC_DIR = BASE_DIR / "static"

# Создаем директории если не существуют
ASSETS_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)


def safe_open_image(filename: str, search_in_base: bool = True) -> Optional[Image.Image]:
    """
    Безопасное открытие изображения с защитой от Path Traversal

    Args:
        filename: Имя файла (без пути или относительный путь)
        search_in_base: Искать в корневой директории если не найдено в assets

    Returns:
        PIL Image объект или None если файл не найден/ошибка
    """
    # Сначала ищем в assets/
    file_path = ASSETS_DIR / filename

    # Если не найдено и разрешено искать в корне
    if not file_path.exists() and search_in_base:
        file_path = BASE_DIR / filename

    # Валидация пути
    valid, error = validate_safe_path(str(file_path.name), BASE_DIR)
    if not valid:
        logger.error(f"Попытка Path Traversal: {filename}")
        log_security_event('path_traversal_blocked', f"Файл: {filename}", 'ERROR')
        return None

    # Проверка существования
    if not file_path.exists():
        logger.warning(f"Файл изображения не найден: {filename}")
        return None

    # Проверка что это действительно изображение
    allowed_extensions = ['.png', '.jpg', '.jpeg', '.eps', '.gif', '.bmp']
    if file_path.suffix.lower() not in allowed_extensions:
        logger.error(f"Недопустимое расширение изображения: {file_path.suffix}")
        log_security_event('invalid_image_extension', str(file_path), 'WARNING')
        return None

    try:
        logger.debug(f"Открытие изображения: {file_path}")
        image = Image.open(file_path)
        return image
    except Exception as e:
        logger.error(f"Ошибка открытия изображения {file_path}: {e}")
        return None


def safe_read_csv(
    filename: str,
    sep: str = ',',
    encoding: str = 'utf-8',
    **kwargs
) -> Optional[pd.DataFrame]:
    """
    Безопасное чтение CSV файла с защитой от Path Traversal

    Args:
        filename: Имя файла (без пути или относительный путь)
        sep: Разделитель (по умолчанию ',')
        encoding: Кодировка (по умолчанию 'utf-8')
        **kwargs: Дополнительные параметры для pd.read_csv

    Returns:
        DataFrame или None в случае ошибки
    """
    # Ищем в data/
    file_path = DATA_DIR / filename

    # Если не найдено, ищем в корне
    if not file_path.exists():
        file_path = BASE_DIR / filename

    # Валидация пути
    valid, error = validate_safe_path(str(file_path.name), BASE_DIR)
    if not valid:
        logger.error(f"Попытка Path Traversal при чтении CSV: {filename}")
        log_security_event('path_traversal_blocked', f"CSV файл: {filename}", 'ERROR')
        return None

    # Проверка существования
    if not file_path.exists():
        logger.error(f"CSV файл не найден: {filename}")
        return None

    # Проверка расширения
    if file_path.suffix.lower() not in ['.csv', '.tsv', '.txt']:
        logger.error(f"Недопустимое расширение для CSV: {file_path.suffix}")
        return None

    try:
        logger.info(f"Чтение CSV файла: {file_path}")
        df = pd.read_csv(file_path, sep=sep, encoding=encoding, **kwargs)
        logger.debug(f"CSV загружен успешно: {len(df)} строк, {len(df.columns)} колонок")
        return df
    except FileNotFoundError:
        logger.error(f"CSV файл не найден: {file_path}")
        return None
    except pd.errors.EmptyDataError:
        logger.warning(f"CSV файл пустой: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Ошибка чтения CSV {file_path}: {e}", exc_info=True)
        return None


def safe_read_file(
    filename: str,
    encoding: str = 'utf-8',
    mode: str = 'r'
) -> Optional[str]:
    """
    Безопасное чтение текстового файла с защитой от Path Traversal

    Args:
        filename: Имя файла (без пути или относительный путь)
        encoding: Кодировка (по умолчанию 'utf-8')
        mode: Режим открытия (по умолчанию 'r')

    Returns:
        Содержимое файла или None в случае ошибки
    """
    # Ищем в корневой директории
    file_path = BASE_DIR / filename

    # Валидация пути
    valid, error = validate_safe_path(str(file_path.name), BASE_DIR)
    if not valid:
        logger.error(f"Попытка Path Traversal при чтении файла: {filename}")
        log_security_event('path_traversal_blocked', f"Файл: {filename}", 'ERROR')
        return None

    # Проверка существования
    if not file_path.exists():
        logger.error(f"Файл не найден: {filename}")
        return None

    try:
        logger.debug(f"Чтение файла: {file_path}")
        with open(file_path, mode, encoding=encoding) as f:
            content = f.read()
        logger.debug(f"Файл прочитан: {len(content)} байт")
        return content
    except Exception as e:
        logger.error(f"Ошибка чтения файла {file_path}: {e}", exc_info=True)
        return None


def get_asset_path(filename: str) -> Optional[Path]:
    """
    Получить безопасный путь к файлу в assets/

    Args:
        filename: Имя файла

    Returns:
        Path объект или None если файл небезопасен
    """
    valid, error = validate_safe_path(filename, ASSETS_DIR)
    if not valid:
        return None

    return ASSETS_DIR / filename


def get_data_path(filename: str) -> Optional[Path]:
    """
    Получить безопасный путь к файлу в data/

    Args:
        filename: Имя файла

    Returns:
        Path объект или None если файл небезопасен
    """
    valid, error = validate_safe_path(filename, DATA_DIR)
    if not valid:
        return None

    return DATA_DIR / filename
