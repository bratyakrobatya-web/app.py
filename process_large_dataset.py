"""
Standalone скрипт для обработки больших массивов городов (18,000+ строк)
Без зависимости от Streamlit - только pandas, rapidfuzz, requests

Использование:
    python process_large_dataset.py input.xlsx output.xlsx

Или в коде:
    from process_large_dataset import process_cities_file
    process_cities_file("input.xlsx", "output.xlsx")
"""

import pandas as pd
import requests
from rapidfuzz import fuzz, process
import re
import sys
from typing import Dict, List, Tuple, Optional

# ============================================
# КОНСТАНТЫ
# ============================================

PREFERRED_MATCHES = {
    'иваново': 'Иваново (Ивановская область)',
    'киров': 'Киров (Кировская область)',
    'подольск': 'Подольск (Московская область)',
    'троицк': 'Троицк (Москва)',
    'железногорск': 'Железногорск (Красноярский край)',
    'кировск': 'Кировск (Ленинградская область)',
    'истра': 'Истра (Московская область)',
    'красногорск': 'Красногорск (Московская область)',
    'истра, деревня покровское': 'Покровское (городской округ Истра)',
    'домодедово': 'Домодедово (Московская область)',
    'клин': 'Клин (Московская область)',
    'октябрьский': 'Октябрьский (Московская область, Люберецкий район)',
    'советск': 'Советск (Калининградская область)',
}

# ============================================
# ФУНКЦИИ НОРМАЛИЗАЦИИ
# ============================================

def normalize_city_name(text: str) -> str:
    """Нормализует название города: ё->е, нижний регистр, убирает лишние пробелы"""
    if not text:
        return ""
    text = text.replace('ё', 'е').replace('Ё', 'Е')
    text = text.lower().strip()
    text = re.sub(r'\s+', ' ', text)
    return text


def normalize_region_name(text: str) -> str:
    """Нормализует название региона для сравнения"""
    text = normalize_city_name(text)
    replacements = {
        'ленинградская': 'ленинград',
        'московская': 'москов',
        'курская': 'курск',
        'кемеровская': 'кемеров',
        'свердловская': 'свердлов',
        'нижегородская': 'нижегород',
        'новосибирская': 'новосибирск',
        'тамбовская': 'тамбов',
        'красноярская': 'красноярск',
        'область': '',
        'обл': '',
        'край': '',
        'республика': '',
        'респ': '',
        '  ': ' '
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text.strip()


def extract_city_and_region(text: str) -> Tuple[str, Optional[str]]:
    """Извлекает название города и региона из текста с учетом префиксов"""
    text_lower = text.lower()

    city_prefixes = ['г.', 'п.', 'д.', 'с.', 'пос.', 'дер.', 'село', 'город', 'поселок', 'деревня']

    if ',' in text:
        text = text.split(',')[0].strip()

    region_keywords = [
        'област', 'край', 'республик', 'округ',
        'ленинград', 'москов', 'курск', 'кемеров',
        'свердлов', 'нижегород', 'новосибирск', 'тамбов',
        'красноярск'
    ]

    text_cleaned = text.strip()
    for prefix in city_prefixes:
        if text_cleaned.lower().startswith(prefix + ' '):
            text_cleaned = text_cleaned[len(prefix) + 1:].strip()
            break
        elif text_cleaned.lower().startswith(prefix):
            text_cleaned = text_cleaned[len(prefix):].strip()
            break

    words = text_cleaned.split()

    if len(words) == 1:
        return text_cleaned, None

    city_words = []
    region_words = []
    region_found = False

    for word in words:
        word_lower = word.lower()
        if not region_found and any(keyword in word_lower for keyword in region_keywords):
            region_found = True
            region_words.append(word)
        elif region_found:
            region_words.append(word)
        else:
            city_words.append(word)

    city = ' '.join(city_words) if city_words else text_cleaned
    region = ' '.join(region_words) if region_words else None

    return city, region

# ============================================
# ФУНКЦИИ РАБОТЫ С HH.RU API
# ============================================

def get_hh_areas() -> Dict:
    """Получает справочник HH.ru"""
    print("Загрузка справочника HH.ru...")
    response = requests.get('https://api.hh.ru/areas')
    data = response.json()

    areas_dict = {}

    def parse_areas(areas, parent_name="", parent_id="", root_parent_id=""):
        for area in areas:
            area_id = area['id']
            area_name = area['name']

            current_root_id = root_parent_id if root_parent_id else parent_id if parent_id else area_id

            areas_dict[area_name] = {
                'id': area_id,
                'name': area_name,
                'parent': parent_name,
                'parent_id': parent_id,
                'root_parent_id': current_root_id
            }

            if 'areas' in area and area['areas']:
                parse_areas(area['areas'], area_name, area_id, current_root_id)

    parse_areas(data)
    print(f"✅ Загружено {len(areas_dict)} городов из HH.ru\n")
    return areas_dict

# ============================================
# ФУНКЦИИ СОПОСТАВЛЕНИЯ
# ============================================

def get_candidates_by_word(client_city: str, hh_city_names: List[str], limit: int = 20) -> List[Tuple[str, float]]:
    """Получает кандидатов по совпадению начального слова"""
    if not client_city or not client_city.strip():
        return []

    words = client_city.split()
    if not words:
        return []

    first_word = normalize_city_name(words[0])

    candidates = []
    for city_name in hh_city_names:
        city_lower = normalize_city_name(city_name)
        if first_word in city_lower:
            score = fuzz.WRatio(normalize_city_name(client_city), city_lower)
            candidates.append((city_name, score))

    candidates.sort(key=lambda x: x[1], reverse=True)

    return candidates[:limit]


def smart_match_city(client_city: str, hh_city_names: List[str], hh_areas: Dict, threshold: int = 85) -> Tuple[Optional[Tuple], List[Tuple[str, float]]]:
    """Умное сопоставление города с сохранением кандидатов и учетом предпочтительных совпадений"""

    city_part, region_part = extract_city_and_region(client_city)
    city_part_lower = normalize_city_name(city_part)

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


def match_cities_batch(original_df: pd.DataFrame, hh_areas: Dict, threshold: int = 85, batch_size: int = 1000) -> pd.DataFrame:
    """
    Сопоставляет города с обработкой батчами для больших массивов.
    ВАЖНО: Без Streamlit UI элементов (progress_bar, status_text)
    """
    results = []
    hh_city_names = list(hh_areas.keys())

    first_col_name = original_df.columns[0]
    other_cols = original_df.columns[1:].tolist() if len(original_df.columns) > 1 else []

    seen_original_cities = {}
    seen_hh_cities = {}

    duplicate_original_count = 0
    duplicate_hh_count = 0

    total_rows = len(original_df)

    # Обработка батчами
    for batch_start in range(0, total_rows, batch_size):
        batch_end = min(batch_start + batch_size, total_rows)
        print(f"Обработка строк {batch_start + 1}-{batch_end} из {total_rows}...")

        batch_df = original_df.iloc[batch_start:batch_end]

        for idx, row in batch_df.iterrows():
            if (idx - batch_start + 1) % 100 == 0:
                print(f"  Обработано {idx - batch_start + 1}/{len(batch_df)} в текущем батче")

            client_city = row[first_col_name]
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
                    **other_values
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

            if match_result:
                matched_name = match_result[0]
                score = match_result[1]
                hh_info = hh_areas[matched_name]
                hh_city_normalized = normalize_city_name(hh_info['name'])

                is_changed = client_city_original.strip() != hh_info['name'].strip()
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

    print(f"\n✅ Обработка завершена!")
    print(f"Дубликатов по исходному названию: {duplicate_original_count}")
    print(f"Дубликатов по результату HH: {duplicate_hh_count}")

    return pd.DataFrame(results)

# ============================================
# ОСНОВНЫЕ ФУНКЦИИ API
# ============================================

def process_cities_file(input_file: str, output_file: str, threshold: int = 85, batch_size: int = 1000):
    """
    Основная функция для обработки файла с городами.

    Args:
        input_file: Путь к входному файлу (.xlsx или .csv)
        output_file: Путь к выходному файлу (.xlsx или .csv)
        threshold: Порог совпадения (по умолчанию 85)
        batch_size: Размер батча для обработки (по умолчанию 1000)
    """
    # 1. Загрузка справочника HH.ru
    hh_areas = get_hh_areas()

    # 2. Загрузка исходных данных
    print(f"Загрузка файла {input_file}...")

    if input_file.endswith('.xlsx'):
        df = pd.read_excel(input_file)
    elif input_file.endswith('.csv'):
        df = pd.read_csv(input_file, encoding='utf-8')
    else:
        raise ValueError("Поддерживаются только файлы .xlsx и .csv")

    print(f"✅ Загружено {len(df)} строк\n")

    # 3. Обработка с батчами
    print("Начало обработки...\n")
    result_df = match_cities_batch(
        original_df=df,
        hh_areas=hh_areas,
        threshold=threshold,
        batch_size=batch_size
    )

    # 4. Сохранение результата
    print(f"\nСохранение результата в {output_file}...")

    if output_file.endswith('.xlsx'):
        result_df.to_excel(output_file, index=False)
    elif output_file.endswith('.csv'):
        result_df.to_csv(output_file, index=False, encoding='utf-8')
    else:
        raise ValueError("Поддерживаются только файлы .xlsx и .csv")

    print(f"✅ Готово! Файл сохранен: {output_file}")

    # 5. Статистика
    total = len(result_df)
    matched = len(result_df[result_df['Статус'].str.contains('✅', na=False)])
    similar = len(result_df[result_df['Статус'].str.contains('⚠️', na=False)])
    not_found = len(result_df[result_df['Статус'].str.contains('❌', na=False)])

    print(f"\n📊 СТАТИСТИКА:")
    print(f"Всего строк: {total}")
    print(f"Точных совпадений (✅): {matched} ({matched/total*100:.1f}%)")
    print(f"Похожих (⚠️): {similar} ({similar/total*100:.1f}%)")
    print(f"Не найдено (❌): {not_found} ({not_found/total*100:.1f}%)")

    return result_df


def process_in_chunks(input_file: str, output_file: str, chunk_size: int = 5000, batch_size: int = 1000):
    """
    Обработка очень больших файлов (50,000+ строк) по частям с записью в процессе.
    Для экстремально больших файлов, которые не помещаются в память целиком.

    Args:
        input_file: Путь к входному файлу (.xlsx или .csv)
        output_file: Путь к выходному файлу (.xlsx)
        chunk_size: Размер чанка для чтения (по умолчанию 5000)
        batch_size: Размер батча для обработки (по умолчанию 1000)
    """
    hh_areas = get_hh_areas()

    print(f"Обработка файла {input_file} по чанкам размером {chunk_size}...\n")

    # Читаем файл частями
    if input_file.endswith('.xlsx'):
        chunks = pd.read_excel(input_file, chunksize=chunk_size)
    elif input_file.endswith('.csv'):
        chunks = pd.read_csv(input_file, chunksize=chunk_size, encoding='utf-8')
    else:
        raise ValueError("Поддерживаются только файлы .xlsx и .csv")

    all_results = []

    for i, chunk in enumerate(chunks):
        print(f"\n🔄 Обработка чанка {i+1} ({len(chunk)} строк)...")

        result_chunk = match_cities_batch(chunk, hh_areas, threshold=85, batch_size=batch_size)
        all_results.append(result_chunk)

        print(f"✅ Чанк {i+1} обработан")

    # Объединяем все результаты
    print("\nОбъединение результатов...")
    final_result = pd.concat(all_results, ignore_index=True)

    # Сохраняем
    print(f"Сохранение в {output_file}...")
    final_result.to_excel(output_file, index=False)

    print(f"✅ Готово! Файл сохранен: {output_file}")

    # Статистика
    total = len(final_result)
    matched = len(final_result[final_result['Статус'].str.contains('✅', na=False)])
    similar = len(final_result[final_result['Статус'].str.contains('⚠️', na=False)])
    not_found = len(final_result[final_result['Статус'].str.contains('❌', na=False)])

    print(f"\n📊 СТАТИСТИКА:")
    print(f"Всего строк: {total}")
    print(f"Точных совпадений (✅): {matched} ({matched/total*100:.1f}%)")
    print(f"Похожих (⚠️): {similar} ({similar/total*100:.1f}%)")
    print(f"Не найдено (❌): {not_found} ({not_found/total*100:.1f}%)")

    return final_result

# ============================================
# CLI ИНТЕРФЕЙС
# ============================================

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Использование:")
        print("  python process_large_dataset.py input.xlsx output.xlsx [threshold] [batch_size]")
        print("\nПримеры:")
        print("  python process_large_dataset.py cities.xlsx result.xlsx")
        print("  python process_large_dataset.py cities.csv result.csv 90 500")
        print("\nАргументы:")
        print("  input.xlsx    - Входной файл (.xlsx или .csv)")
        print("  output.xlsx   - Выходной файл (.xlsx или .csv)")
        print("  threshold     - Порог совпадения (по умолчанию 85)")
        print("  batch_size    - Размер батча (по умолчанию 1000)")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    threshold = int(sys.argv[3]) if len(sys.argv) > 3 else 85
    batch_size = int(sys.argv[4]) if len(sys.argv) > 4 else 1000

    try:
        process_cities_file(input_file, output_file, threshold, batch_size)
    except Exception as e:
        print(f"\n❌ Ошибка: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
