"""
Модуль высокоуровневого сопоставления городов для VR Мультитул

Этот модуль содержит функции для:
- Умного сопоставления отдельных городов с учетом предпочтений
- Массового сопоставления городов из DataFrame
- Слияния файлов с городами с удалением дублей
"""

from typing import Dict, List, Tuple, Optional
import pandas as pd
import streamlit as st
from rapidfuzz import fuzz, process

# Импорты из других модулей
from modules.matching import (
    normalize_city_name,
    extract_city_and_region,
    get_candidates_by_word,
    PREFERRED_MATCHES,
    EXCLUDED_EXACT_MATCHES
)
from modules.data_processing import normalize_region_name
from modules.utils import get_russian_cities, check_if_changed


def smart_match_city(
    client_city: str,
    hh_city_names: List[str],
    hh_areas: Dict,
    threshold: int = 85
) -> Tuple[Optional[Tuple[str, float, int]], List[Tuple[str, int]]]:
    """
    Умное сопоставление города с сохранением кандидатов и учетом предпочтительных совпадений

    Функция выполняет многоступенчатое сопоставление:
    1. Проверяет исключения (EXCLUDED_EXACT_MATCHES)
    2. Проверяет предпочтительные совпадения (PREFERRED_MATCHES)
    3. Использует сопоставление по начальному слову (get_candidates_by_word)
    4. Выполняет точное сопоставление с учетом региона
    5. Использует fuzzy matching с RapidFuzz
    6. Применяет adjustments к score на основе различных критериев

    Args:
        client_city: Название города клиента для сопоставления
        hh_city_names: Список названий городов из справочника HH.ru
        hh_areas: Справочник регионов HH.ru
        threshold: Порог совпадения (0-100), по умолчанию 85

    Returns:
        Tuple[Optional[Tuple[str, float, int]], List[Tuple[str, int]]]:
            - Лучшее совпадение: (название, score, index) или None
            - Список кандидатов: [(название, score), ...]

    Examples:
        >>> areas = get_hh_areas()
        >>> cities = get_russian_cities(areas)
        >>> match, candidates = smart_match_city("Москва", cities, areas)
        >>> if match:
        ...     print(f"Найдено: {match[0]}, Score: {match[1]}")
    """
    city_part, region_part = extract_city_and_region(client_city)
    city_part_lower = normalize_city_name(city_part)

    # Проверяем исключения - города, которые НЕ должны совпадать
    if city_part_lower in EXCLUDED_EXACT_MATCHES:
        word_candidates = get_candidates_by_word(city_part, hh_city_names)
        return None, word_candidates

    # Проверяем предпочтительные совпадения
    if city_part_lower in PREFERRED_MATCHES:
        preferred_match = PREFERRED_MATCHES[city_part_lower]
        if preferred_match in hh_city_names:
            score = fuzz.WRatio(city_part_lower, normalize_city_name(preferred_match))
            word_candidates = get_candidates_by_word(city_part, hh_city_names)
            return (preferred_match, score, 0), word_candidates

    word_candidates = get_candidates_by_word(city_part, hh_city_names)

    if word_candidates and len(word_candidates) > 0 and word_candidates[0][1] >= threshold:
        best_candidate = word_candidates[0]
        return (best_candidate[0], best_candidate[1], 0), word_candidates

    if not word_candidates or (word_candidates and word_candidates[0][1] < threshold):
        return None, word_candidates

    exact_matches = []
    exact_matches_with_region = []

    for hh_city_name in hh_city_names:
        hh_city_base = normalize_city_name(hh_city_name.split('(')[0].strip())

        if city_part_lower == hh_city_base:
            if region_part:
                region_normalized = normalize_region_name(region_part)
                hh_normalized = normalize_region_name(hh_city_name)

                if region_normalized in hh_normalized:
                    exact_matches_with_region.append(hh_city_name)
                else:
                    exact_matches.append(hh_city_name)
            else:
                exact_matches.append(hh_city_name)

    if exact_matches_with_region:
        best_match = exact_matches_with_region[0]
        score = fuzz.WRatio(city_part_lower, normalize_city_name(best_match))
        return (best_match, score, 0), word_candidates
    elif exact_matches:
        best_match = exact_matches[0]
        score = fuzz.WRatio(city_part_lower, normalize_city_name(best_match))
        return (best_match, score, 0), word_candidates

    candidates = process.extract(
        city_part,
        hh_city_names,
        scorer=fuzz.WRatio,
        limit=10
    )

    if not candidates:
        return None, word_candidates

    candidates = [c for c in candidates if c[1] >= threshold]

    if not candidates:
        return None, word_candidates

    if len(candidates) == 1:
        return candidates[0], word_candidates

    best_match = None
    best_score = 0

    for candidate_name, score, _ in candidates:
        candidate_lower = normalize_city_name(candidate_name)
        adjusted_score = score

        candidate_city = normalize_city_name(candidate_name.split('(')[0].strip())

        if city_part_lower == candidate_city:
            adjusted_score += 50
        elif city_part_lower in candidate_city:
            adjusted_score += 30
        elif candidate_city in city_part_lower:
            adjusted_score += 20
        else:
            adjusted_score -= 30

        if region_part:
            region_normalized = normalize_region_name(region_part)
            candidate_normalized = normalize_region_name(candidate_name)

            if region_normalized in candidate_normalized:
                adjusted_score += 40
            elif '(' in candidate_name:
                adjusted_score -= 25

        len_diff = abs(len(candidate_city) - len(city_part_lower))
        if len_diff > 3:
            adjusted_score -= 20

        if len(candidate_city) > len(city_part_lower) + 4:
            adjusted_score -= 25

        if len(candidate_name) > 15 and len(city_part) > 15:
            adjusted_score += 5

        region_keywords = ['oblast', 'край', 'республик', 'округ']
        client_has_region = any(keyword in city_part_lower for keyword in region_keywords)
        candidate_has_region = any(keyword in candidate_lower for keyword in region_keywords)

        if client_has_region and candidate_has_region:
            adjusted_score += 15
        elif client_has_region and not candidate_has_region:
            adjusted_score -= 15

        if adjusted_score > best_score:
            best_score = adjusted_score
            best_match = (candidate_name, score, _)

    return (best_match if best_match else candidates[0]), word_candidates


def match_cities(
    original_df: pd.DataFrame,
    hh_areas: Dict,
    threshold: int = 85,
    sheet_name: Optional[str] = None
) -> Tuple[pd.DataFrame, int, int, int]:
    """
    Сопоставляет города с сохранением кандидатов и всех столбцов

    Функция обрабатывает DataFrame с городами и сопоставляет каждый город с HH.ru.
    Отслеживает дубликаты по исходным названиям и результатам HH.
    Сохраняет все дополнительные столбцы из исходного DataFrame.

    Args:
        original_df: Исходный DataFrame с городами (первый столбец = названия городов)
        hh_areas: Справочник регионов HH.ru
        threshold: Порог совпадения (0-100), по умолчанию 85
        sheet_name: Название листа (для кэширования кандидатов), опционально

    Returns:
        Tuple[pd.DataFrame, int, int, int]:
            - DataFrame с результатами сопоставления
            - Количество дубликатов по исходным названиям
            - Количество дубликатов по результатам HH
            - Общее количество дубликатов

    Columns в результате:
        - Исходное название
        - Итоговое гео
        - ID HH
        - Регион
        - Совпадение %
        - Изменение (Да/Нет)
        - Статус (✅ Точное / ⚠️ Похожее / ❌ Не найдено / 🔄 Дубликат / ❌ Пустое значение)
        - row_id (индекс строки)
        - [дополнительные столбцы из original_df]

    Examples:
        >>> areas = get_hh_areas()
        >>> df = pd.DataFrame({'Город': ['Москва', 'Спб', 'Екб']})
        >>> result_df, dup_orig, dup_hh, total_dup = match_cities(df, areas)
        >>> print(f"Обработано {len(result_df)} городов, дубликатов: {total_dup}")
    """
    results = []
    # Используем только российские города
    hh_city_names = get_russian_cities(hh_areas)

    # Определяем названия столбцов
    first_col_name = original_df.columns[0]
    other_cols = original_df.columns[1:].tolist() if len(original_df.columns) > 1 else []

    seen_original_cities = {}
    seen_hh_cities = {}

    duplicate_original_count = 0
    duplicate_hh_count = 0

    # Не перезаписываем кэш, чтобы сохранить данные для всех вкладок

    progress_bar = st.progress(0)
    status_text = st.empty()

    for idx, row in original_df.iterrows():
        progress = (idx + 1) / len(original_df)
        progress_bar.progress(progress)
        status_text.text(f"Обработано {idx + 1} из {len(original_df)} городов...")

        client_city = row[first_col_name]

        # Сохраняем значения остальных столбцов
        other_values = {col: row[col] for col in other_cols}

        if pd.isna(client_city) or str(client_city).strip() == "":
            results.append({
                'Исходное название': client_city,
                'Итоговое гео': None,
                'ID HH': None,
                'Регион': None,
                'Совпадение %': 0,
                'Изменение': 'Нет',
                'Статус': '❌ Пустое значение',
                'row_id': idx,
                **other_values  # Добавляем остальные столбцы
            })
            continue

        client_city_original = str(client_city).strip()
        client_city_normalized = normalize_city_name(client_city_original)

        if client_city_normalized in seen_original_cities:
            duplicate_original_count += 1
            original_result = seen_original_cities[client_city_normalized]
            results.append({
                'Исходное название': client_city_original,
                'Итоговое гео': original_result['Итоговое гео'],
                'ID HH': original_result['ID HH'],
                'Регион': original_result['Регион'],
                'Совпадение %': original_result['Совпадение %'],
                'Изменение': original_result['Изменение'],
                'Статус': '🔄 Дубликат (исходное название)',
                'row_id': idx,
                **other_values
            })
            continue

        match_result, candidates = smart_match_city(client_city_original, hh_city_names, hh_areas, threshold)

        # Используем составной ключ для вкладок, простой для базового режима
        cache_key = (sheet_name, idx) if sheet_name else idx
        st.session_state.candidates_cache[cache_key] = candidates

        if match_result:
            matched_name = match_result[0]
            score = match_result[1]
            hh_info = hh_areas[matched_name]
            hh_city_normalized = normalize_city_name(hh_info['name'])

            is_changed = check_if_changed(client_city_original, hh_info['name'])
            change_status = 'Да' if is_changed else 'Нет'

            if hh_city_normalized in seen_hh_cities:
                duplicate_hh_count += 1
                city_result = {
                    'Исходное название': client_city_original,
                    'Итоговое гео': hh_info['name'],
                    'ID HH': hh_info['id'],
                    'Регион': hh_info['parent'],
                    'Совпадение %': round(score, 1),
                    'Изменение': change_status,
                    'Статус': '🔄 Дубликат (результат HH)',
                    'row_id': idx,
                    **other_values
                }
                results.append(city_result)
                seen_original_cities[client_city_normalized] = city_result
            else:
                status = '✅ Точное' if score >= 95 else '⚠️ Похожее'

                city_result = {
                    'Исходное название': client_city_original,
                    'Итоговое гео': hh_info['name'],
                    'ID HH': hh_info['id'],
                    'Регион': hh_info['parent'],
                    'Совпадение %': round(score, 1),
                    'Изменение': change_status,
                    'Статус': status,
                    'row_id': idx,
                    **other_values
                }

                results.append(city_result)
                seen_original_cities[client_city_normalized] = city_result
                seen_hh_cities[hh_city_normalized] = True
        else:
            city_result = {
                'Исходное название': client_city_original,
                'Итоговое гео': None,
                'ID HH': None,
                'Регион': None,
                'Совпадение %': 0,
                'Изменение': 'Нет',
                'Статус': '❌ Не найдено',
                'row_id': idx,
                **other_values
            }

            results.append(city_result)
            seen_original_cities[client_city_normalized] = city_result

    progress_bar.empty()
    status_text.empty()

    total_duplicates = duplicate_original_count + duplicate_hh_count

    return pd.DataFrame(results), duplicate_original_count, duplicate_hh_count, total_duplicates


def merge_cities_files(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    hh_areas: Dict,
    threshold: int = 85
) -> Tuple[pd.DataFrame, Dict]:
    """
    Объединяет два файла с городами с удалением дублей

    Функция обрабатывает два DataFrame с городами, сопоставляет каждый город с HH.ru
    и объединяет результаты, удаляя дубликаты как по исходным названиям, так и по
    результатам HH.

    Args:
        df1: Первый DataFrame с городами
        df2: Второй DataFrame с городами
        hh_areas: Справочник HH.ru
        threshold: Порог совпадения для сопоставления (0-100), по умолчанию 85

    Returns:
        Tuple[pd.DataFrame, Dict]:
            - merged_df: Объединенный DataFrame без дублей
            - stats: Словарь со статистикой объединения:
                - total_from_file1: количество городов из файла 1
                - total_from_file2: количество городов из файла 2
                - duplicates_removed: количество удаленных дублей
                - unique_cities: количество уникальных городов
                - merged_total: общее количество в результате

    Columns в результате:
        - Исходное название
        - Итоговое гео
        - ID HH
        - Регион
        - Совпадение %
        - Источник (Файл 1 / Файл 2)
        - Статус (✅ Точное / ⚠️ Похожее / ❌ Не найдено)

    Examples:
        >>> areas = get_hh_areas()
        >>> df1 = pd.DataFrame({'Город': ['Москва', 'Спб']})
        >>> df2 = pd.DataFrame({'Город': ['Спб', 'Екб']})  # Спб - дубликат
        >>> merged_df, stats = merge_cities_files(df1, df2, areas)
        >>> print(f"Файл 1: {stats['total_from_file1']}, Файл 2: {stats['total_from_file2']}")
        >>> print(f"Удалено дубликатов: {stats['duplicates_removed']}")
        >>> print(f"Итого уникальных: {stats['unique_cities']}")
    """
    # Используем только российские города
    hh_city_names = get_russian_cities(hh_areas)

    # Словари для отслеживания уникальных городов
    seen_original_cities = {}  # По исходному названию
    seen_hh_cities = {}  # По результату HH

    results = []
    stats = {
        'total_from_file1': len(df1),
        'total_from_file2': len(df2),
        'duplicates_removed': 0,
        'unique_cities': 0,
        'merged_total': 0
    }

    # Определяем названия столбцов для каждого файла
    first_col_name_df1 = df1.columns[0]
    first_col_name_df2 = df2.columns[0]

    # Обрабатываем первый файл
    st.info("📄 Обработка первого файла...")
    progress_bar = st.progress(0)

    for idx, row in df1.iterrows():
        progress = (idx + 1) / len(df1)
        progress_bar.progress(progress)

        client_city = row[first_col_name_df1]

        # Пропускаем пустые значения
        if pd.isna(client_city) or str(client_city).strip() == "":
            continue

        client_city_original = str(client_city).strip()
        client_city_normalized = normalize_city_name(client_city_original)

        # Проверяем, не видели ли мы уже этот город
        if client_city_normalized in seen_original_cities:
            stats['duplicates_removed'] += 1
            continue

        # Сопоставляем с HH
        match_result, candidates = smart_match_city(client_city_original, hh_city_names, hh_areas, threshold)

        if match_result:
            matched_name = match_result[0]
            score = match_result[1]
            hh_info = hh_areas[matched_name]
            hh_city_normalized = normalize_city_name(hh_info['name'])

            # Проверяем дубликат по результату HH
            if hh_city_normalized in seen_hh_cities:
                stats['duplicates_removed'] += 1
                continue

            # Добавляем город
            city_result = {
                'Исходное название': client_city_original,
                'Итоговое гео': hh_info['name'],
                'ID HH': hh_info['id'],
                'Регион': hh_info['parent'],
                'Совпадение %': round(score, 1),
                'Источник': 'Файл 1',
                'Статус': '✅ Точное' if score >= 95 else '⚠️ Похожее'
            }

            results.append(city_result)
            seen_original_cities[client_city_normalized] = city_result
            seen_hh_cities[hh_city_normalized] = True
            stats['unique_cities'] += 1
        else:
            # Город не найден в HH, но добавляем в список
            city_result = {
                'Исходное название': client_city_original,
                'Итоговое гео': None,
                'ID HH': None,
                'Регион': None,
                'Совпадение %': 0,
                'Источник': 'Файл 1',
                'Статус': '❌ Не найдено'
            }

            results.append(city_result)
            seen_original_cities[client_city_normalized] = city_result
            stats['unique_cities'] += 1

    progress_bar.empty()

    # Обрабатываем второй файл
    st.info("📄 Обработка второго файла...")
    progress_bar = st.progress(0)

    for idx, row in df2.iterrows():
        progress = (idx + 1) / len(df2)
        progress_bar.progress(progress)

        client_city = row[first_col_name_df2]

        # Пропускаем пустые значения
        if pd.isna(client_city) or str(client_city).strip() == "":
            continue

        client_city_original = str(client_city).strip()
        client_city_normalized = normalize_city_name(client_city_original)

        # Проверяем, не видели ли мы уже этот город (из первого файла или ранее из второго)
        if client_city_normalized in seen_original_cities:
            stats['duplicates_removed'] += 1
            continue

        # Сопоставляем с HH
        match_result, candidates = smart_match_city(client_city_original, hh_city_names, hh_areas, threshold)

        if match_result:
            matched_name = match_result[0]
            score = match_result[1]
            hh_info = hh_areas[matched_name]
            hh_city_normalized = normalize_city_name(hh_info['name'])

            # Проверяем дубликат по результату HH
            if hh_city_normalized in seen_hh_cities:
                stats['duplicates_removed'] += 1
                continue

            # Добавляем город
            city_result = {
                'Исходное название': client_city_original,
                'Итоговое гео': hh_info['name'],
                'ID HH': hh_info['id'],
                'Регион': hh_info['parent'],
                'Совпадение %': round(score, 1),
                'Источник': 'Файл 2',
                'Статус': '✅ Точное' if score >= 95 else '⚠️ Похожее'
            }

            results.append(city_result)
            seen_original_cities[client_city_normalized] = city_result
            seen_hh_cities[hh_city_normalized] = True
            stats['unique_cities'] += 1
        else:
            # Город не найден в HH, но добавляем в список
            city_result = {
                'Исходное название': client_city_original,
                'Итоговое гео': None,
                'ID HH': None,
                'Регион': None,
                'Совпадение %': 0,
                'Источник': 'Файл 2',
                'Статус': '❌ Не найдено'
            }

            results.append(city_result)
            seen_original_cities[client_city_normalized] = city_result
            stats['unique_cities'] += 1

    progress_bar.empty()

    stats['merged_total'] = len(results)

    return pd.DataFrame(results), stats
