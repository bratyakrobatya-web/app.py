"""
Unit тесты для security_utils.py
"""
import pytest
import sys
import os
import pandas as pd
from unittest.mock import Mock, patch

# Добавляем родительскую директорию в путь для импорта
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from security_utils import (
    sanitize_html,
    sanitize_user_input,
    sanitize_csv_content,
    safe_session_append,
    check_session_state_limits,
    validate_dataframe_size,
    handle_production_error,
    MAX_SESSION_LIST_SIZE,
    MAX_SESSION_KEYS
)


class TestSanitizeHtml:
    """Тесты для функции sanitize_html"""

    def test_escape_basic_html(self):
        """Проверка экранирования основных HTML тегов"""
        assert sanitize_html("<script>alert('xss')</script>") == "&lt;script&gt;alert('xss')&lt;/script&gt;"
        assert sanitize_html("<div>test</div>") == "&lt;div&gt;test&lt;/div&gt;"

    def test_escape_special_chars(self):
        """Проверка экранирования специальных символов"""
        assert "&lt;" in sanitize_html("<tag>")
        assert "&gt;" in sanitize_html("<tag>")
        assert "&amp;" in sanitize_html("a & b")

    def test_empty_string(self):
        """Проверка обработки пустой строки"""
        assert sanitize_html("") == ""

    def test_normal_text(self):
        """Проверка обычного текста без HTML"""
        assert sanitize_html("Hello World") == "Hello World"


class TestSanitizeUserInput:
    """Тесты для функции sanitize_user_input"""

    def test_max_length_truncation(self):
        """Проверка обрезки по максимальной длине"""
        long_text = "a" * 2000
        result = sanitize_user_input(long_text, max_length=100)
        assert len(result) == 100

    def test_null_byte_removal(self):
        """Проверка удаления null bytes"""
        text = "test\x00data"
        result = sanitize_user_input(text)
        assert "\x00" not in result

    def test_path_traversal_prevention(self):
        """Проверка защиты от path traversal"""
        assert "../" not in sanitize_user_input("../../etc/passwd")
        assert "..\\" not in sanitize_user_input("..\\..\\windows")

    def test_html_escaping_by_default(self):
        """Проверка экранирования HTML по умолчанию"""
        result = sanitize_user_input("<script>alert('xss')</script>")
        assert "&lt;script&gt;" in result

    def test_allow_html_option(self):
        """Проверка разрешения HTML при allow_html=True"""
        result = sanitize_user_input("<b>test</b>", allow_html=True)
        assert "<b>" in result

    def test_control_chars_removal(self):
        """Проверка удаления управляющих символов"""
        text = "test\x01\x02\x03data"
        result = sanitize_user_input(text)
        assert "\x01" not in result
        assert "\x02" not in result

    def test_preserve_newlines_tabs(self):
        """Проверка сохранения переносов строк и табов"""
        text = "line1\nline2\ttab"
        result = sanitize_user_input(text)
        assert "\n" in result
        assert "\t" in result

    def test_empty_input(self):
        """Проверка обработки пустого ввода"""
        assert sanitize_user_input("") == ""
        assert sanitize_user_input(None) == ""


class TestSanitizeCsvContent:
    """Тесты для функции sanitize_csv_content"""

    def test_remove_excel_formulas(self):
        """Проверка удаления формул Excel"""
        df = pd.DataFrame({
            'col1': ['=SUM(A1:A10)', '+1+2', '-10', '@test', 'normal']
        })
        result = sanitize_csv_content(df)
        # Формулы должны быть удалены
        assert not result['col1'].str.startswith('=').any()
        assert not result['col1'].str.startswith('+').any()
        assert not result['col1'].str.startswith('-').any()
        assert not result['col1'].str.startswith('@').any()

    def test_preserve_normal_data(self):
        """Проверка сохранения нормальных данных"""
        df = pd.DataFrame({
            'col1': ['test', 'data', '123']
        })
        result = sanitize_csv_content(df)
        assert result['col1'].tolist() == ['test', 'data', '123']

    def test_numeric_columns_unchanged(self):
        """Проверка что числовые столбцы не изменяются"""
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': [1.5, 2.5, 3.5]
        })
        result = sanitize_csv_content(df)
        assert result['col1'].dtype in ['int64', 'int32']
        assert result['col2'].dtype in ['float64', 'float32']


class TestSafeSessionAppend:
    """Тесты для функции safe_session_append"""

    @patch('security_utils.st')
    def test_initialize_new_list(self, mock_st):
        """Проверка инициализации нового списка"""
        mock_st.session_state = {}
        result = safe_session_append('test_key', 'value')
        assert result is True
        assert 'test_key' in mock_st.session_state
        assert mock_st.session_state['test_key'] == ['value']

    @patch('security_utils.st')
    def test_append_to_existing_list(self, mock_st):
        """Проверка добавления в существующий список"""
        mock_st.session_state = {'test_key': ['val1']}
        result = safe_session_append('test_key', 'val2')
        assert result is True
        assert mock_st.session_state['test_key'] == ['val1', 'val2']

    @patch('security_utils.st')
    def test_limit_enforcement(self, mock_st):
        """Проверка соблюдения лимита размера"""
        mock_st.session_state = {'test_key': ['val'] * 100}
        result = safe_session_append('test_key', 'new_val', max_size=100)
        assert result is False
        assert len(mock_st.session_state['test_key']) == 100

    @patch('security_utils.st')
    def test_non_list_key(self, mock_st):
        """Проверка обработки не-списка"""
        mock_st.session_state = {'test_key': 'not_a_list'}
        result = safe_session_append('test_key', 'value')
        assert result is False


class TestCheckSessionStateLimits:
    """Тесты для функции check_session_state_limits"""

    @patch('security_utils.st')
    def test_normal_state(self, mock_st):
        """Проверка нормального состояния"""
        mock_st.session_state = Mock()
        mock_st.session_state.keys.return_value = ['key1', 'key2']
        mock_st.session_state.items.return_value = [('key1', [1, 2]), ('key2', 'test')]

        stats = check_session_state_limits()
        assert stats['total_keys'] == 2
        assert len(stats['warnings']) == 0
        assert len(stats['oversized_lists']) == 0

    @patch('security_utils.st')
    def test_exceeds_key_limit(self, mock_st):
        """Проверка превышения лимита ключей"""
        mock_st.session_state = Mock()
        keys = [f'key{i}' for i in range(MAX_SESSION_KEYS + 10)]
        mock_st.session_state.keys.return_value = keys
        mock_st.session_state.items.return_value = [(k, 'val') for k in keys]

        stats = check_session_state_limits()
        assert stats['total_keys'] > MAX_SESSION_KEYS
        assert len(stats['warnings']) > 0

    @patch('security_utils.st')
    def test_oversized_list(self, mock_st):
        """Проверка определения слишком большого списка"""
        mock_st.session_state = Mock()
        big_list = ['item'] * (MAX_SESSION_LIST_SIZE + 100)
        mock_st.session_state.keys.return_value = ['big_key']
        mock_st.session_state.items.return_value = [('big_key', big_list)]

        stats = check_session_state_limits()
        assert len(stats['oversized_lists']) > 0


class TestValidateDataframeSize:
    """Тесты для функции validate_dataframe_size"""

    def test_valid_dataframe(self):
        """Проверка валидного DataFrame"""
        df = pd.DataFrame({'col1': [1, 2, 3], 'col2': [4, 5, 6]})
        is_valid, error = validate_dataframe_size(df)
        assert is_valid is True
        assert error == ""

    def test_too_many_rows(self):
        """Проверка слишком большого количества строк"""
        df = pd.DataFrame({'col1': range(200000)})
        is_valid, error = validate_dataframe_size(df, max_rows=100000)
        assert is_valid is False
        assert "строк" in error.lower()

    def test_too_many_columns(self):
        """Проверка слишком большого количества столбцов"""
        data = {f'col{i}': [1] for i in range(150)}
        df = pd.DataFrame(data)
        is_valid, error = validate_dataframe_size(df, max_columns=100)
        assert is_valid is False
        assert "столбцов" in error.lower()

    def test_empty_dataframe(self):
        """Проверка пустого DataFrame"""
        df = pd.DataFrame()
        is_valid, error = validate_dataframe_size(df)
        assert is_valid is True


class TestHandleProductionError:
    """Тесты для функции handle_production_error"""

    @patch.dict(os.environ, {'STREAMLIT_ENV': 'production'})
    def test_production_mode_hides_details(self):
        """Проверка скрытия деталей в production"""
        error = ValueError("sensitive error message")
        result = handle_production_error(error, "User friendly message", "test_context")

        assert "User friendly message" in result
        assert "ValueError" not in result
        assert "sensitive" not in result
        assert "поддержку" in result or "support" in result.lower()

    @patch.dict(os.environ, {'STREAMLIT_ENV': 'development'})
    def test_development_mode_shows_details(self):
        """Проверка отображения деталей в development"""
        error = ValueError("debug error message")
        result = handle_production_error(error, "User message", "test_context")

        assert "User message" in result
        assert "ValueError" in result
        assert "debug error message" in result

    def test_logging_occurs(self):
        """Проверка что логирование происходит"""
        with patch('security_utils.logger') as mock_logger:
            error = Exception("test error")
            handle_production_error(error, "message", "context")

            # Проверяем что logger.error был вызван
            assert mock_logger.error.called


# Запуск тестов
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
