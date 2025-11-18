"""
City Matching Module
Модуль для сопоставления названий городов
"""

import re
import pandas as pd
from rapidfuzz import fuzz
from typing import List, Tuple, Optional


# ============================================================================
# КОНСТАНТЫ
# ============================================================================

# Предпочтительные совпадения для неоднозначных городов
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
    'кировск Ленинградская': 'Кировск (Ленинградская область)',
    'звенигород': 'Звенигород (Московская область)',
    'радужный хмао': 'Радужный (Ханты-Мансийский АО - Югра)',
    'радужный': 'Радужный (Ханты-Мансийский АО - Югра)',
    'железногорск Курской области': 'Железногорск (Курская область)',
    'воскресенск': 'Воскресенск (Московская область)',
    'северск': 'Северск (Томская область)',
    'егорьевск': 'Егорьевск (Московская область)',
    'дмитров': 'Дмитров (Московская область)',
    'волжский': 'Волжский (Самарская область)',
}

# Исключения - названия, которые НЕ должны совпадать (вернуть None)
EXCLUDED_EXACT_MATCHES = {
    'ленинградская',  # Точное совпадение с "Ленинградская" = Нет совпадения
}


# ============================================================================
# ФУНКЦИИ НОРМАЛИЗАЦИИ
# ============================================================================

def normalize_city_name(text: str) -> str:
    """
    Нормализует название города: ё->е, нижний регистр, убирает лишние пробелы

    Args:
        text: Название города

    Returns:
        Нормализованное название города

    Examples:
        >>> normalize_city_name("  Москва  ")
        'москва'
        >>> normalize_city_name("Королёв")
        'королев'
    """
    # Проверяем, что text это строка, иначе возвращаем пустую строку
    if pd.isna(text) or not isinstance(text, str):
        return ""
    if not text:
        return ""
    # Заменяем ё на е
    text = text.replace('ё', 'е').replace('Ё', 'Е')
    # Приводим к нижнему регистру и убираем лишние пробелы
    text = text.lower().strip()
    # Заменяем множественные пробелы на один
    text = re.sub(r'\s+', ' ', text)
    return text


# ============================================================================
# ФУНКЦИИ ИЗВЛЕЧЕНИЯ
# ============================================================================

def extract_city_and_region(text: str) -> Tuple[str, Optional[str]]:
    """
    Извлекает название города и региона из текста с учетом префиксов

    Args:
        text: Текст с названием города (например, "г. Москва" или "Иваново Ивановская область")

    Returns:
        Tuple[str, Optional[str]]: (city, region) - название города и региона (или None)

    Examples:
        >>> extract_city_and_region("г. Москва")
        ('Москва', None)
        >>> extract_city_and_region("Иваново Ивановская область")
        ('Иваново', 'Ивановская область')
    """
    text_lower = text.lower()

    # Префиксы населенных пунктов
    city_prefixes = ['г.', 'п.', 'д.', 'с.', 'пос.', 'дер.', 'село', 'город', 'поселок', 'деревня']

    # Убираем всё после запятой (дополнительная информация типа "Истра, деревня Покровское")
    if ',' in text:
        text = text.split(',')[0].strip()

    region_keywords = [
        'област', 'край', 'республик', 'округ',
        'ленинград', 'москов', 'курск', 'кемеров',
        'свердлов', 'нижегород', 'новосибирск', 'тамбов',
        'красноярск'
    ]

    # Удаляем префиксы в начале строки (с пробелом и без)
    text_cleaned = text.strip()
    for prefix in city_prefixes:
        # Проверяем с пробелом: "г. Москва"
        if text_cleaned.lower().startswith(prefix + ' '):
            text_cleaned = text_cleaned[len(prefix) + 1:].strip()  # +1 для пробела
            break
        # Проверяем без пробела: "г.Москва"
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


# ============================================================================
# ФУНКЦИИ ПОИСКА КАНДИДАТОВ
# ============================================================================

def get_candidates_by_word(
    client_city: str,
    hh_city_names: List[str],
    limit: int = 20
) -> List[Tuple[str, int]]:
    """
    Получает кандидатов по совпадению начального слова с применением PREFERRED_MATCHES

    Args:
        client_city: Название города от клиента
        hh_city_names: Список городов из справочника HH
        limit: Максимальное количество кандидатов

    Returns:
        List[Tuple[str, int]]: Список кандидатов (название, score) отсортированный по убыванию score

    Examples:
        >>> candidates = get_candidates_by_word("Москва", ["Москва", "Московский"])
        >>> candidates[0][0]
        'Москва'
    """
    # Проверка на пустую строку
    if not client_city or not client_city.strip():
        return []

    # Нормализуем исходное название для проверки исключений
    client_city_normalized = normalize_city_name(client_city)

    # Проверяем исключения - города, которые НЕ должны совпадать
    if client_city_normalized in EXCLUDED_EXACT_MATCHES:
        return []

    # Проверяем предпочтительные совпадения
    if client_city_normalized in PREFERRED_MATCHES:
        preferred_match = PREFERRED_MATCHES[client_city_normalized]
        if preferred_match in hh_city_names:
            score = fuzz.WRatio(client_city_normalized, normalize_city_name(preferred_match))
            # Возвращаем предпочтительное совпадение с наивысшим приоритетом
            return [(preferred_match, score)]

    words = client_city.split()
    if not words:
        return []

    first_word = normalize_city_name(words[0])

    candidates = []
    for city_name in hh_city_names:
        city_lower = normalize_city_name(city_name)
        if first_word in city_lower:
            score = fuzz.WRatio(client_city_normalized, city_lower)
            candidates.append((city_name, score))

    candidates.sort(key=lambda x: x[1], reverse=True)

    return candidates[:limit]
