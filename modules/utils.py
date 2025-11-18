"""
Вспомогательные утилиты для VR Мультитул

Этот модуль содержит общие вспомогательные функции:
- Фильтрация городов по странам
- Обработка заголовков DataFrame
- Проверка изменений в данных
"""

from typing import Dict, List
import pandas as pd


def get_russian_cities(hh_areas: Dict) -> List[str]:
    """
    Возвращает список только российских городов из справочника HH

    Фильтрует справочник HH.ru, оставляя только города с root_parent_id = '113' (Россия).

    Args:
        hh_areas: Справочник регионов HH.ru (из get_hh_areas)

    Returns:
        List[str]: Список названий городов, относящихся к России

    Examples:
        >>> areas = get_hh_areas()
        >>> russian_cities = get_russian_cities(areas)
        >>> print(f"Найдено {len(russian_cities)} российских городов")
        >>> print("Москва" in russian_cities)  # True
    """
    russia_id = '113'
    return [
        city_name for city_name, city_info in hh_areas.items()
        if city_info.get('root_parent_id', '') == russia_id
    ]


def remove_header_row_if_needed(df: pd.DataFrame, first_col_name: str) -> pd.DataFrame:
    """
    Удаляет первую строку, если она является заголовком (не реальными данными города)

    Логика аналогична публикатору: проверяет первую строку на наличие ключевых слов
    заголовков ("название", "город", "регион" и т.д.) и удаляет её при совпадении.

    Args:
        df: DataFrame для обработки
        first_col_name: Название первого столбца для проверки

    Returns:
        pd.DataFrame: DataFrame с удаленной строкой-заголовком (если она была)
                     или оригинальный DataFrame

    Examples:
        >>> df = pd.DataFrame({'City': ['Город', 'Москва', 'Санкт-Петербург']})
        >>> cleaned_df = remove_header_row_if_needed(df, 'City')
        >>> print(len(cleaned_df))  # 2 (заголовок удален)
        >>> print(cleaned_df.iloc[0]['City'])  # 'Москва'
    """
    if len(df) == 0:
        return df

    # Получаем значение первой ячейки в первой строке
    first_value = df.iloc[0][first_col_name]

    if pd.isna(first_value):
        return df

    # Нормализуем значение для проверки
    first_value_str = str(first_value).strip().lower()

    # Список ключевых слов, указывающих на то, что это заголовок
    header_keywords = [
        'название', 'город', 'регион', 'гео', 'location', 'city',
        'region', 'населенный пункт', 'geography', 'область'
    ]

    # Если первая строка содержит ключевые слова заголовка - удаляем её
    if any(keyword in first_value_str for keyword in header_keywords):
        df = df.iloc[1:].reset_index(drop=True)

    return df


def check_if_changed(original: str, matched: str) -> bool:
    """
    Проверяет, изменилось ли название города после сопоставления

    Сравнивает оригинальное название города с сопоставленным названием из HH.ru.
    Возвращает False если город не найден (matched == None или "❌ Нет совпадения").

    Args:
        original: Исходное название города
        matched: Сопоставленное название города из HH.ru (или None)

    Returns:
        bool: True если названия отличаются, False если одинаковые или не найдены

    Examples:
        >>> check_if_changed("Москва", "Москва")
        False
        >>> check_if_changed("Спб", "Санкт-Петербург")
        True
        >>> check_if_changed("Лондон", None)
        False
        >>> check_if_changed("Питер", "❌ Нет совпадения")
        False
    """
    if matched is None or matched == "❌ Нет совпадения":
        return False

    original_clean = original.strip()
    matched_clean = matched.strip()

    return original_clean != matched_clean
