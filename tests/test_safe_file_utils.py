"""
Unit тесты для safe_file_utils.py
"""
import pytest
import sys
import os
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from io import StringIO
from PIL import Image

# Добавляем родительскую директорию в путь для импорта
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from safe_file_utils import (
    safe_open_image,
    safe_read_csv,
    safe_read_file,
    get_asset_path,
    get_data_path
)


class TestSafeOpenImage:
    """Тесты для функции safe_open_image"""

    def test_path_traversal_protection(self):
        """Проверка защиты от path traversal"""
        # Попытка использовать ../
        result = safe_open_image("../../etc/passwd")
        assert result is None

    def test_nonexistent_file(self):
        """Проверка обработки несуществующего файла"""
        result = safe_open_image("nonexistent_image_12345.png")
        assert result is None

    @patch('safe_file_utils.Image.open')
    @patch('security_utils.validate_safe_path')
    @patch('pathlib.Path.exists')
    def test_valid_image_opens(self, mock_exists, mock_validate, mock_image_open):
        """Проверка открытия валидного изображения"""
        # Мокаем существование файла
        mock_exists.return_value = True

        # Мокаем валидацию пути - возвращаем (True, None)
        mock_validate.return_value = (True, None)

        # Мокаем Image.open
        mock_img = Mock(spec=Image.Image)
        mock_image_open.return_value = mock_img

        result = safe_open_image("test.png", search_in_base=False)
        # Результат должен быть объектом Image
        assert result is not None

    def test_empty_filename(self):
        """Проверка обработки пустого имени файла"""
        result = safe_open_image("")
        assert result is None

    @patch('security_utils.validate_safe_path')
    def test_invalid_path_rejected(self, mock_validate):
        """Проверка отклонения невалидного пути"""
        mock_validate.return_value = (False, "Path traversal detected")
        result = safe_open_image("../../../test.png", search_in_base=False)
        assert result is None


class TestSafeReadCsv:
    """Тесты для функции safe_read_csv"""

    @patch('security_utils.validate_safe_path')
    @patch('pandas.read_csv')
    @patch('pathlib.Path.exists')
    def test_valid_csv_reads(self, mock_exists, mock_read_csv, mock_validate):
        """Проверка чтения валидного CSV"""
        mock_exists.return_value = True
        mock_validate.return_value = (True, None)
        mock_df = pd.DataFrame({'col1': [1, 2, 3]})
        mock_read_csv.return_value = mock_df

        result = safe_read_csv("test.csv")
        # Проверяем что функция возвращает DataFrame
        assert result is not None
        assert isinstance(result, pd.DataFrame)

    def test_path_traversal_protection(self):
        """Проверка защиты от path traversal"""
        result = safe_read_csv("../../etc/passwd")
        assert result is None

    def test_nonexistent_file(self):
        """Проверка обработки несуществующего файла"""
        result = safe_read_csv("nonexistent_file_12345.csv")
        assert result is None

    @patch('security_utils.validate_safe_path')
    def test_invalid_path_rejected(self, mock_validate):
        """Проверка отклонения невалидного пути"""
        mock_validate.return_value = (False, "Path traversal detected")
        result = safe_read_csv("../../../test.csv")
        assert result is None

    @patch('pandas.read_csv')
    @patch('security_utils.validate_safe_path')
    @patch('pathlib.Path.exists')
    def test_encoding_parameter(self, mock_exists, mock_validate, mock_read_csv):
        """Проверка передачи параметра encoding"""
        mock_exists.return_value = True
        mock_validate.return_value = (True, None)
        mock_read_csv.return_value = pd.DataFrame()

        safe_read_csv("test.csv", encoding='utf-8')
        # Проверяем что encoding был передан
        assert mock_read_csv.called
        call_kwargs = mock_read_csv.call_args[1]
        assert 'encoding' in call_kwargs


class TestSafeReadFile:
    """Тесты для функции safe_read_file"""

    @patch('security_utils.validate_safe_path')
    @patch('pathlib.Path.exists')
    @patch('builtins.open', create=True)
    def test_valid_file_reads(self, mock_open, mock_exists, mock_validate):
        """Проверка чтения валидного файла"""
        mock_validate.return_value = (True, None)
        mock_exists.return_value = True
        mock_open.return_value.__enter__.return_value.read.return_value = "test content"

        result = safe_read_file("test.txt")
        assert result is not None
        assert isinstance(result, str)
        assert result == "test content"

    def test_path_traversal_protection(self):
        """Проверка защиты от path traversal"""
        result = safe_read_file("../../etc/passwd")
        assert result is None

    def test_nonexistent_file(self):
        """Проверка обработки несуществующего файла"""
        result = safe_read_file("nonexistent_file_12345.txt")
        assert result is None

    @patch('security_utils.validate_safe_path')
    def test_invalid_path_rejected(self, mock_validate):
        """Проверка отклонения невалидного пути"""
        mock_validate.return_value = (False, "Path traversal detected")
        result = safe_read_file("../../../test.txt")
        assert result is None

    @patch('security_utils.validate_safe_path')
    @patch('pathlib.Path.exists')
    @patch('builtins.open', create=True)
    def test_encoding_parameter(self, mock_open, mock_exists, mock_validate):
        """Проверка передачи параметра encoding"""
        mock_validate.return_value = (True, None)
        mock_exists.return_value = True
        mock_open.return_value.__enter__.return_value.read.return_value = "test"

        result = safe_read_file("test.txt", encoding='cp1251')
        # Проверяем что encoding был передан при вызове open
        assert mock_open.called
        call_kwargs = mock_open.call_args[1]
        assert call_kwargs['encoding'] == 'cp1251'


class TestGetAssetPath:
    """Тесты для функции get_asset_path"""

    @patch('security_utils.validate_safe_path')
    def test_valid_asset_path(self, mock_validate):
        """Проверка получения пути к валидному asset"""
        mock_validate.return_value = (True, None)

        result = get_asset_path("logo.png")
        assert result is not None
        assert isinstance(result, Path)
        assert "assets" in str(result)

    def test_path_traversal_protection(self):
        """Проверка защиты от path traversal"""
        result = get_asset_path("../../etc/passwd")
        assert result is None

    def test_nonexistent_asset(self):
        """Проверка обработки несуществующего asset"""
        result = get_asset_path("nonexistent_asset_12345.png")
        assert result is None

    @patch('security_utils.validate_safe_path')
    def test_invalid_path_rejected(self, mock_validate):
        """Проверка отклонения невалидного пути"""
        mock_validate.return_value = (False, "Path traversal detected")
        result = get_asset_path("../../../test.png")
        assert result is None

    def test_empty_filename(self):
        """Проверка обработки пустого имени файла"""
        result = get_asset_path("")
        assert result is None


class TestGetDataPath:
    """Тесты для функции get_data_path"""

    @patch('security_utils.validate_safe_path')
    def test_valid_data_path(self, mock_validate):
        """Проверка получения пути к валидным данным"""
        mock_validate.return_value = (True, None)

        result = get_data_path("data.csv")
        assert result is not None
        assert isinstance(result, Path)
        assert "data" in str(result)

    def test_path_traversal_protection(self):
        """Проверка защиты от path traversal"""
        result = get_data_path("../../etc/passwd")
        assert result is None

    def test_nonexistent_data(self):
        """Проверка обработки несуществующих данных"""
        result = get_data_path("nonexistent_data_12345.csv")
        assert result is None

    @patch('security_utils.validate_safe_path')
    def test_invalid_path_rejected(self, mock_validate):
        """Проверка отклонения невалидного пути"""
        mock_validate.return_value = (False, "Path traversal detected")
        result = get_data_path("../../../test.csv")
        assert result is None

    def test_empty_filename(self):
        """Проверка обработки пустого имени файла"""
        result = get_data_path("")
        assert result is None


# Интеграционные тесты
class TestIntegration:
    """Интеграционные тесты для всех функций"""

    def test_all_functions_handle_none_input(self):
        """Проверка обработки None всеми функциями"""
        assert safe_open_image(None) is None
        assert safe_read_csv(None) is None
        assert safe_read_file(None) is None
        assert get_asset_path(None) is None
        assert get_data_path(None) is None

    def test_all_functions_handle_path_traversal(self):
        """Проверка защиты от path traversal во всех функциях"""
        dangerous_paths = [
            "../../etc/passwd",
            "../../../windows/system32",
            "..\\..\\..\\test.txt",
            "%2e%2e/test"
        ]

        for path in dangerous_paths:
            assert safe_open_image(path) is None
            assert safe_read_csv(path) is None
            assert safe_read_file(path) is None
            assert get_asset_path(path) is None
            assert get_data_path(path) is None


# Запуск тестов
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
