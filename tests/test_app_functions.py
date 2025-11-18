"""
Unit тесты для основных функций app.py
"""
import pytest
import sys
import os
import pandas as pd
from unittest.mock import Mock, patch, MagicMock

# Добавляем родительскую директорию в путь для импорта
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Импортируем функции из app.py
# Примечание: Некоторые функции требуют Streamlit context, поэтому тестируем только независимые


class TestCachedFunctions:
    """Тесты для кэшированных функций"""

    def test_get_russian_cities_cached_returns_list(self):
        """Проверка что get_russian_cities_cached возвращает список"""
        # Мокаем hh_areas
        mock_hh_areas = {
            'Москва': {'root_parent_id': '113', 'id': '1'},
            'London': {'root_parent_id': '5', 'id': '2'},
            'Санкт-Петербург': {'root_parent_id': '113', 'id': '78'}
        }

        # Импортируем функцию
        from app import get_russian_cities_cached

        # Вызываем функцию
        result = get_russian_cities_cached(mock_hh_areas)

        # Проверки
        assert isinstance(result, list)
        assert 'Москва' in result
        assert 'Санкт-Петербург' in result
        assert 'London' not in result  # Не российский город

    def test_get_russian_cities_cached_filters_correctly(self):
        """Проверка корректной фильтрации российских городов"""
        mock_hh_areas = {
            'Город1': {'root_parent_id': '113'},
            'Город2': {'root_parent_id': '113'},
            'Город3': {'root_parent_id': '999'},  # Не Россия
            'Город4': {'root_parent_id': '113'}
        }

        from app import get_russian_cities_cached
        result = get_russian_cities_cached(mock_hh_areas)

        assert len(result) == 3  # Только российские
        assert 'Город3' not in result

    def test_prepare_city_options_returns_tuple(self):
        """Проверка что prepare_city_options возвращает кортеж"""
        from app import prepare_city_options

        candidates = (('Москва', 95.5), ('Московский', 85.0))
        current_value = 'Москва'
        current_match = 95.5
        city_name = 'Москва'

        options, candidates_dict = prepare_city_options(
            candidates, current_value, current_match, city_name
        )

        assert isinstance(options, tuple)
        assert isinstance(candidates_dict, dict)
        assert len(options) > 0
        assert '❌ Нет совпадения' in options

    def test_prepare_city_options_adds_no_match_option(self):
        """Проверка добавления опции 'Нет совпадения'"""
        from app import prepare_city_options

        candidates = (('Москва', 95.5),)
        options, _ = prepare_city_options(candidates, 'Москва', 95.5, 'Москва')

        assert options[0] == '❌ Нет совпадения'

    def test_prepare_city_options_sorts_by_percentage(self):
        """Проверка сортировки по проценту совпадения"""
        from app import prepare_city_options

        candidates = (
            ('Город1', 70.0),
            ('Город2', 95.0),
            ('Город3', 85.0)
        )

        options, _ = prepare_city_options(candidates, '', 0, 'test')

        # Пропускаем первый элемент (❌ Нет совпадения)
        # Второй должен быть с наивысшим процентом
        assert '95.0%' in options[1]

    def test_prepare_city_options_creates_correct_dict(self):
        """Проверка создания словаря для O(1) поиска"""
        from app import prepare_city_options

        candidates = (('Москва', 95.0), ('Московский', 85.0))
        _, candidates_dict = prepare_city_options(candidates, '', 0, 'test')

        assert 'Москва' in candidates_dict
        assert 'Московский' in candidates_dict
        assert candidates_dict['Москва'] >= 0  # Индекс должен быть неотрицательным


class TestApplyManualSelections:
    """Тесты для функции apply_manual_selections_cached"""

    def test_apply_no_match_selection(self):
        """Проверка применения 'Нет совпадения'"""
        from app import apply_manual_selections_cached

        df = pd.DataFrame({
            'row_id': [1, 2],
            'Исходное название': ['Москва', 'Питер'],
            'Итоговое гео': ['Москва', 'Санкт-Петербург'],
            'ID HH': [1, 78],
            'Регион': ['Москва', 'Санкт-Петербург'],
            'Совпадение %': [100.0, 95.0],
            'Изменение': ['Нет', 'Да'],
            'Статус': ['✅ Найдено', '✅ Найдено']
        })

        hh_areas = {
            'Москва': {'id': 1, 'parent': 'Москва'},
            'Санкт-Петербург': {'id': 78, 'parent': 'Санкт-Петербург'}
        }

        manual_selections = {1: '❌ Нет совпадения'}

        result = apply_manual_selections_cached(df, manual_selections, hh_areas)

        # Проверяем что для row_id=1 установлены правильные значения
        row = result[result['row_id'] == 1].iloc[0]
        assert pd.isna(row['Итоговое гео']) or row['Итоговое гео'] is None
        assert pd.isna(row['ID HH']) or row['ID HH'] is None
        assert row['Совпадение %'] == 0
        assert row['Статус'] == '❌ Не найдено'

    def test_apply_city_change(self):
        """Проверка применения изменения города"""
        from app import apply_manual_selections_cached

        df = pd.DataFrame({
            'row_id': [1],
            'Исходное название': ['Москва'],
            'Итоговое гео': ['Москва'],
            'ID HH': [1],
            'Регион': ['Москва'],
            'Совпадение %': [100.0],
            'Изменение': ['Нет'],
            'Статус': ['✅ Найдено']
        })

        hh_areas = {
            'Москва': {'id': 1, 'parent': 'Москва'},
            'Санкт-Петербург': {'id': 78, 'parent': 'Санкт-Петербург'}
        }

        manual_selections = {1: 'Санкт-Петербург'}

        result = apply_manual_selections_cached(df, manual_selections, hh_areas)

        row = result[result['row_id'] == 1].iloc[0]
        assert row['Итоговое гео'] == 'Санкт-Петербург'
        assert row['ID HH'] == 78
        assert row['Регион'] == 'Санкт-Петербург'
        assert row['Изменение'] == 'Да'

    def test_empty_manual_selections(self):
        """Проверка обработки пустых manual_selections"""
        from app import apply_manual_selections_cached

        df = pd.DataFrame({
            'row_id': [1],
            'Исходное название': ['Москва'],
            'Итоговое гео': ['Москва'],
            'ID HH': [1],
            'Регион': ['Москва'],
            'Совпадение %': [100.0],
            'Изменение': ['Нет'],
            'Статус': ['✅ Найдено']
        })

        hh_areas = {}
        manual_selections = {}

        result = apply_manual_selections_cached(df, manual_selections, hh_areas)

        # DataFrame должен остаться без изменений
        pd.testing.assert_frame_equal(result, df)

    def test_handles_missing_city_in_hh_areas(self):
        """Проверка обработки отсутствующего города в справочнике"""
        from app import apply_manual_selections_cached

        df = pd.DataFrame({
            'row_id': [1],
            'Исходное название': ['Тестовый'],
            'Итоговое гео': ['Тестовый'],
            'ID HH': [999],
            'Регион': ['Тестовый'],
            'Совпадение %': [100.0],
            'Изменение': ['Нет'],
            'Статус': ['✅ Найдено']
        })

        hh_areas = {'Москва': {'id': 1, 'parent': 'Москва'}}
        manual_selections = {1: 'Несуществующий город'}

        result = apply_manual_selections_cached(df, manual_selections, hh_areas)

        # Функция должна обработать это без ошибок
        assert result is not None
        assert len(result) == 1


class TestDataProcessing:
    """Тесты для функций обработки данных"""

    def test_dataframe_returns_copy_not_reference(self):
        """Проверка что apply_manual_selections возвращает копию"""
        from app import apply_manual_selections_cached

        df = pd.DataFrame({
            'row_id': [1],
            'Исходное название': ['Test'],
            'Итоговое гео': ['Test'],
            'ID HH': [1],
            'Регион': ['Test'],
            'Совпадение %': [100.0],
            'Изменение': ['Нет'],
            'Статус': ['✅ Найдено']
        })

        original_df = df.copy()
        manual_selections = {1: 'Новый'}
        hh_areas = {'Новый': {'id': 2, 'parent': 'Новый'}}

        result = apply_manual_selections_cached(df, manual_selections, hh_areas)

        # Оригинальный df не должен измениться
        pd.testing.assert_frame_equal(df, original_df)


class TestIntegration:
    """Интеграционные тесты"""

    def test_pipeline_from_hh_areas_to_results(self):
        """Проверка полного pipeline обработки"""
        from app import get_russian_cities_cached, apply_manual_selections_cached

        # 1. Получаем российские города
        mock_hh_areas = {
            'Москва': {'root_parent_id': '113', 'id': 1, 'parent': 'Москва'},
            'Лондон': {'root_parent_id': '5', 'id': 999, 'parent': 'UK'}
        }

        russia_cities = get_russian_cities_cached(mock_hh_areas)
        assert 'Москва' in russia_cities
        assert 'Лондон' not in russia_cities

        # 2. Применяем ручные изменения
        df = pd.DataFrame({
            'row_id': [1],
            'Исходное название': ['Test'],
            'Итоговое гео': ['Test'],
            'ID HH': [999],
            'Регион': ['Test'],
            'Совпадение %': [50.0],
            'Изменение': ['Нет'],
            'Статус': ['⚠️ Низкое совпадение']
        })

        manual_selections = {1: 'Москва'}
        result = apply_manual_selections_cached(df, manual_selections, mock_hh_areas)

        assert result.iloc[0]['Итоговое гео'] == 'Москва'
        assert result.iloc[0]['ID HH'] == 1


# Запуск тестов
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
