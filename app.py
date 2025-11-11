import streamlit as st  
import requests  
import pandas as pd  
from rapidfuzz import fuzz, process  
import io  
import re
import zipfile
from datetime import datetime

# Version: 3.2.0 - Added: Tabs mode in split view with download per sheet

# Настройка страницы  
st.set_page_config(  
    page_title="Синхронизатор гео HH.ru",  
    page_icon="🌍",  
    layout="wide"  
)  

# CSS для анимации земли и стилей  
st.markdown("""  
<style>  
@keyframes rotate {  
    from { transform: rotate(0deg); }  
    to { transform: rotate(360deg); }  
}  

.rotating-earth {  
    display: inline-block;  
    animation: rotate 3s linear infinite;  
    font-size: 3em;  
    vertical-align: middle;  
    margin-right: 15px;  
}  

.main-title {  
    display: inline-block;  
    font-size: 3em;  
    font-weight: bold;  
    vertical-align: middle;  
    margin: 0;  
}  

.title-container {  
    display: flex;  
    align-items: center;  
    margin-bottom: 20px;  
}

/* Стили для вкладок вакансий */
.stTabs [data-baseweb="tab-list"] {
    gap: 8px;
}

.stTabs [data-baseweb="tab"] {
    height: 60px;
    padding: 0px 24px;
    background-color: #f0f2f6;
    border-radius: 8px 8px 0px 0px;
    font-size: 18px;
    font-weight: 600;
}

.stTabs [aria-selected="true"] {
    background-color: #ff4b4b;
    color: white;
}

.stTabs [data-baseweb="tab"]:hover {
    background-color: #ff6b6b;
    color: white;
}
</style>  
""", unsafe_allow_html=True)  

# Инициализация session_state  
if 'result_df' not in st.session_state:  
    st.session_state.result_df = None  
if 'duplicate_count' not in st.session_state:  
    st.session_state.duplicate_count = 0  
if 'processed' not in st.session_state:  
    st.session_state.processed = False  
if 'manual_selections' not in st.session_state:  
    st.session_state.manual_selections = {}  
if 'candidates_cache' not in st.session_state:  
    st.session_state.candidates_cache = {}  
if 'search_query' not in st.session_state:  
    st.session_state.search_query = ""
if 'added_cities' not in st.session_state:
    st.session_state.added_cities = []
if 'original_df' not in st.session_state:
    st.session_state.original_df = None
if 'export_mode' not in st.session_state:
    st.session_state.export_mode = None

# ============================================  
# СПРАВОЧНИК ФЕДЕРАЛЬНЫХ ОКРУГОВ И РЕГИОНОВ  
# ============================================  
FEDERAL_DISTRICTS = {
    "Центральный федеральный округ": [
        "Белгородская область", "Брянская область", "Владимирская область",
        "Воронежская область", "Ивановская область", "Калужская область",
        "Костромская область", "Курская область", "Липецкая область",
        "Московская область", "Орловская область", "Рязанская область",
        "Смоленская область", "Тамбовская область", "Тверская область",
        "Тульская область", "Ярославская область", "Москва"
    ],
    "Южный федеральный округ": [
        "Республика Адыгея", "Республика Калмыкия", "Краснодарский край",
        "Астраханская область", "Волгоградская область", "Ростовская область"
    ],
    "Северо-Западный федеральный округ": [
        "Республика Карелия", "Республика Коми", "Архангельская область",
        "Вологодская область", "Калининградская область", "Ленинградская область",
        "Мурманская область", "Новгородская область", "Псковская область",
        "Санкт-Петербург", "Ненецкий автономный округ"
    ],
    "Дальневосточный федеральный округ": [
        "Республика Саха (Якутия)", "Камчатский край", "Приморский край",
        "Хабаровский край", "Амурская область", "Магаданская область",
        "Сахалинская область", "Еврейская автономная область", "Чукотский автономный округ"
    ],
    "Сибирский федеральный округ": [
        "Республика Алтай", "Республика Бурятия", "Республика Тыва",
        "Республика Хакасия", "Алтайский край", "Забайкальский край",
        "Красноярский край", "Иркутская область", "Кемеровская область",
        "Новосибирская область", "Омская область", "Томская область"
    ],
    "Уральский федеральный округ": [
        "Курганская область", "Свердловская область", "Тюменская область",
        "Челябинская область", "Ханты-Мансийский автономный округ — Югра",
        "Ямало-Ненецкий автономный округ"
    ],
    "Приволжский федеральный округ": [
        "Республика Башкортостан", "Республика Марий Эл", "Республика Мордовия",
        "Республика Татарстан", "Удмуртская Республика", "Чувашская Республика",
        "Кировская область", "Нижегородская область", "Оренбургская область",
        "Пензенская область", "Пермский край", "Самарская область",
        "Саратовская область", "Ульяновская область"
    ],
    "Северо-Кавказский федеральный округ": [
        "Республика Дагестан", "Республика Ингушетия", "Кабардино-Балкарская Республика",
        "Карачаево-Черкесская Республика", "Республика Северная Осетия — Алания",
        "Чеченская Республика", "Ставропольский край"
    ],
    "Крымский федеральный округ": [
        "Республика Крым", "Севастополь"
    ]
}

# ============================================
# СПРАВОЧНИК ПРЕДПОЧТИТЕЛЬНЫХ СОВПАДЕНИЙ
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
# ФУНКЦИИ  
# ============================================  
def normalize_city_name(text):
    """Нормализует название города: ё->е, нижний регистр, убирает лишние пробелы"""
    if not text:
        return ""
    # Заменяем ё на е
    text = text.replace('ё', 'е').replace('Ё', 'Е')
    # Приводим к нижнему регистру и убираем лишние пробелы
    text = text.lower().strip()
    # Заменяем множественные пробелы на один
    text = re.sub(r'\s+', ' ', text)
    return text

@st.cache_data(ttl=3600)  
def get_hh_areas():  
    """Получает справочник HH.ru"""  
    response = requests.get('https://api.hh.ru/areas')  
    data = response.json()  
      
    areas_dict = {}  
      
    def parse_areas(areas, parent_name="", parent_id="", root_parent_id=""):  
        for area in areas:  
            area_id = area['id']  
            area_name = area['name']  
            
            # Определяем корневой parent_id (страну)
            current_root_id = root_parent_id if root_parent_id else parent_id if parent_id else area_id
              
            areas_dict[area_name] = {  
                'id': area_id,  
                'name': area_name,  
                'parent': parent_name,
                'parent_id': parent_id,
                'root_parent_id': current_root_id  # ID страны верхнего уровня
            }  
              
            if area.get('areas'):  
                parse_areas(area['areas'], area_name, area_id, current_root_id)  
      
    parse_areas(data)  
    return areas_dict  

def get_cities_by_regions(hh_areas, selected_regions):
    """Получает все города из выбранных регионов (только Россия, только города)"""
    cities = []
    
    # Список исключений - что не выгружать (нормализованные названия)
    excluded_names_normalized = [
        normalize_city_name('Россия'),
        normalize_city_name('Другие регионы'),
        normalize_city_name('Другие страны'),
        normalize_city_name('Чукотский АО'),
        normalize_city_name('Ямало-Ненецкий АО'),
        normalize_city_name('Ненецкий АО'),
        normalize_city_name('Ханты-Мансийский АО - Югра'),
        normalize_city_name('Еврейская АО'),
        normalize_city_name('Беловское'),
        normalize_city_name('Горькая Балка')
    ]
    
    # Ключевые слова, которые указывают на регион, а не город
    region_keywords = ['область', 'край', 'республика', 'округ', 'автономн']
    
    # ID России
    russia_id = '113'
    
    for city_name, city_info in hh_areas.items():
        parent = city_info['parent']
        root_parent_id = city_info.get('root_parent_id', '')
        
        # Пропускаем всё, что не относится к России
        if root_parent_id != russia_id:
            continue
        
        # Нормализуем название для проверки исключений
        city_name_normalized = normalize_city_name(city_name)
        
        # Пропускаем исключенные названия (нормализованное сравнение)
        if city_name_normalized in excluded_names_normalized:
            continue
        
        # Пропускаем области, края, республики
        if not parent or parent == 'Россия':
            # Проверяем, не является ли это областью/краем/республикой по названию
            is_region = any(keyword in city_name_normalized for keyword in region_keywords)
            if is_region:
                continue
            
            # Дополнительная проверка: если название заканчивается на "АО" и это не город
            if city_name.endswith(' АО') or city_name.endswith('АО'):
                continue
        
        # Проверяем, входит ли город в выбранные регионы
        for region in selected_regions:
            # Нормализуем названия для сравнения
            region_normalized = normalize_city_name(region)
            parent_normalized = normalize_city_name(parent) if parent else ""
            
            # Проверяем различные варианты совпадений
            if (region_normalized in parent_normalized or 
                parent_normalized in region_normalized or
                region_normalized == parent_normalized or
                region_normalized == city_name_normalized):
                cities.append({
                    'Город': city_name,
                    'ID HH': city_info['id'],
                    'Регион': parent if parent else 'Россия'
                })
                break
    
    # Создаем DataFrame
    df = pd.DataFrame(cities)
    
    # Удаляем дубликаты по нормализованному названию города
    if not df.empty:
        df['_город_normalized'] = df['Город'].apply(normalize_city_name)
        df = df.drop_duplicates(subset=['_город_normalized'], keep='first')
        df = df.drop(columns=['_город_normalized'])
    
    return df

def get_all_cities(hh_areas):
    """Получает все города из справочника HH (только Россия, только города)"""
    cities = []
    
    # Список исключений - что не выгружать (нормализованные названия)
    excluded_names_normalized = [
        normalize_city_name('Россия'),
        normalize_city_name('Другие регионы'),
        normalize_city_name('Другие страны'),
        normalize_city_name('Чукотский АО'),
        normalize_city_name('Ямало-Ненецкий АО'),
        normalize_city_name('Ненецкий АО'),
        normalize_city_name('Ханты-Мансийский АО - Yugра'),
        normalize_city_name('Еврейская АО'),
        normalize_city_name('Беловское'),
        normalize_city_name('Горькая Балка')
    ]
    
    # Ключевые слова, которые указывают на регион, а не город
    region_keywords = ['область', 'край', 'республика', 'округ', 'автономн']
    
    # ID России
    russia_id = '113'
    
    for city_name, city_info in hh_areas.items():
        parent = city_info['parent']
        root_parent_id = city_info.get('root_parent_id', '')
        
        # Пропускаем всё, что не относится к России
        if root_parent_id != russia_id:
            continue
        
        # Нормализуем название для проверки исключений
        city_name_normalized = normalize_city_name(city_name)
        
        # Пропускаем исключенные названия (нормализованное сравнение)
        if city_name_normalized in excluded_names_normalized:
            continue
        
        # Пропускаем области, края, республики
        if not parent or parent == 'Россия':
            # Проверяем, не является ли это областью/краем/республикой по названию
            is_region = any(keyword in city_name_normalized for keyword in region_keywords)
            if is_region:
                continue
            
            # Дополнительная проверка: если название заканчивается на "АО" и это не город
            if city_name.endswith(' АО') or city_name.endswith('АО'):
                continue
        
        cities.append({
            'Город': city_name,
            'ID HH': city_info['id'],
            'Регион': parent if parent else 'Россия'
        })
    
    # Создаем DataFrame
    df = pd.DataFrame(cities)
    
    # Удаляем дубликаты по нормализованному названию города
    if not df.empty:
        df['_город_normalized'] = df['Город'].apply(normalize_city_name)
        df = df.drop_duplicates(subset=['_город_normalized'], keep='first')
        df = df.drop(columns=['_город_normalized'])
    
    return df

def normalize_region_name(text):  
    """Нормализует название региона для сравнения"""  
    text = normalize_city_name(text)  # Используем общую нормализацию с ё->е
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

def extract_city_and_region(text):  
    """Извлекает название города и региона из текста с учетом префиксов"""  
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

def check_if_changed(original, matched):  
    """Проверяет, изменилось ли название города"""  
    if matched is None or matched == "❌ Нет совпадения":  
        return False  
      
    original_clean = original.strip()  
    matched_clean = matched.strip()  
      
    return original_clean != matched_clean  

def get_candidates_by_word(client_city, hh_city_names, limit=20):  
    """Получает кандидатов по совпадению начального слова"""  
    # Проверка на пустую строку
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

def smart_match_city(client_city, hh_city_names, hh_areas, threshold=85):  
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

def match_cities(original_df, hh_areas, threshold=85):  
    """Сопоставляет города с сохранением кандидатов и всех столбцов"""  
    results = []  
    hh_city_names = list(hh_areas.keys())  
    
    # Определяем названия столбцов
    first_col_name = original_df.columns[0]
    other_cols = original_df.columns[1:].tolist() if len(original_df.columns) > 1 else []
      
    seen_original_cities = {}  
    seen_hh_cities = {}  
      
    duplicate_original_count = 0  
    duplicate_hh_count = 0  
      
    st.session_state.candidates_cache = {}  
      
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
          
        st.session_state.candidates_cache[idx] = candidates  
          
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

# ============================================  
# ИНТЕРФЕЙС  
# ============================================  
# Заголовок с анимированной землей  
st.markdown(  
    '<div class="title-container">'  
    '<span class="rotating-earth">🌍</span>'  
    '<span class="main-title">Синхронизатор гео HH.ru</span>'  
    '</div>',  
    unsafe_allow_html=True  
)  
st.markdown("---")  

# Загрузка справочника HH
try:  
    hh_areas = get_hh_areas()  
except Exception as e:  
    st.error(f"❌ Ошибка загрузки справочника: {str(e)}")  
    hh_areas = None  

# ============================================
# БЛОК: ПРОВЕРКА ГЕО
# ============================================
if hh_areas:
    st.header("🔍 Проверка гео")
    st.info("Введите название города, чтобы найти правильное написание в справочнике HH.ru")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Получаем только города России
        russia_cities = []
        for city_name, city_info in hh_areas.items():
            if city_info.get('root_parent_id') == '113':
                russia_cities.append(city_name)
        
        search_geo = st.selectbox(
            "Выберите город для проверки:",
            options=[""] + sorted(russia_cities),
            key="geo_checker",
            help="Начните вводить название города"
        )
    
    with col2:
        if search_geo:
            city_info = hh_areas[search_geo]
            st.success("✅ Найдено")
            st.info(f"**ID HH:** {city_info['id']}")
            st.info(f"**Регион:** {city_info['parent']}")

st.markdown("---")

# ============================================
# БЛОК: СИНХРОНИЗАТОР ГОРОДОВ
# ============================================
st.header("📤 Синхронизатор городов")

with st.sidebar:  
    st.header("⚙️ Настройки")  
    threshold = st.slider(  
        "Порог совпадения (%)",  
        min_value=50,  
        max_value=100,  
        value=85,  
        help="Минимальный процент совпадения"  
    )  
      
    st.markdown("---")  
      
    st.markdown("### 📖 Инструкция")  
    st.markdown("""  
    **Как использовать:**  
      
    1. **Загрузите файл** Excel или CSV с городами  
    2. Города должны быть в **первой колонке**  
    3. Остальные столбцы сохранятся без изменений
    4. Нажмите **"🚀 Начать сопоставление"**  
    5. Проверьте результаты в таблице  
    6. Отредактируйте города с совпадением ≤ 90%  
    7. Добавьте дополнительные города при необходимости
    8. Скачайте итоговый файл  
      
    **Формат файла:**  
    - Первый столбец - города  
    - Остальные столбцы - любые данные
    - Можно указывать область/регион  
    - Поддерживаются префиксы: г., п., д., с.
    """)  
      
    st.markdown("---")  
    st.markdown("### 📊 Статусы")  
    st.markdown("""  
    - ✅ **Точное** - совпадение ≥95%  
    - ⚠️ **Похожее** - совпадение ≥порога  
    - 🔄 **Дубликат** - повторы  
    - ❌ **Не найдено** - совпадение <порога  
    """)  

col1, col2 = st.columns([1, 1])  

with col1:  
    st.subheader("📤 Загрузка файла")  
    uploaded_file = st.file_uploader(  
        "Выберите файл с городами",  
        type=['xlsx', 'csv'],  
        help="Поддерживаются форматы: Excel (.xlsx) и CSV"  
    )  
      
    with st.expander("📋 Показать пример формата файла"):  
        example_df = pd.DataFrame({  
            'Город': ['г. Москва', 'п. Внуковское', 'Санкт-Петербург', 'Екатеринбург'],
            'Данные 1': ['Значение 1', 'Значение 2', 'Значение 3', 'Значение 4'],
            'Данные 2': ['A', 'B', 'C', 'D']
        })  
        st.dataframe(example_df, use_container_width=True, hide_index=True)
        st.caption("✅ Первый столбец - города (с префиксами г., п. и др.)")
        st.caption("✅ Остальные столбцы сохраняются без изменений")

with col2:  
    st.subheader("ℹ️ Информация")  
    if hh_areas:
        st.success(f"✅ Справочник HH загружен: **{len(hh_areas)}** городов")  

if uploaded_file is not None and hh_areas is not None:  
    st.markdown("---")  
      
    try:  
        # Определяем тип файла и читаем все вкладки
        if uploaded_file.name.endswith('.csv'):
            # CSV - одна вкладка
            df = pd.read_csv(uploaded_file, header=None)
            sheets_data = {'Sheet1': df}
        else:
            # Excel - читаем все вкладки
            excel_file = pd.ExcelFile(uploaded_file)
            sheets_data = {}
            for sheet_name in excel_file.sheet_names:
                df_sheet = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
                if len(df_sheet) > 0:  # Только непустые вкладки
                    sheets_data[sheet_name] = df_sheet
        
        # Анализируем структуру файла
        st.session_state.sheets_data = {}
        st.session_state.has_multiple_sheets = len(sheets_data) > 1
        st.session_state.sheet_mode = None  # 'tabs' или 'columns' или 'both' или None
        
        # Обрабатываем каждую вкладку
        for sheet_name, df in sheets_data.items():
            has_header = False
            has_vacancy_column = False
            vacancy_col_idx = None
            
            # Проверяем первую строку на наличие заголовков
            if len(df) > 0:
                first_row = df.iloc[0]
                # Проверяем первую ячейку на "Город"
                if pd.notna(first_row[0]) and 'город' in str(first_row[0]).lower():
                    has_header = True
                    # Ищем столбец "Вакансия"
                    for idx, val in enumerate(first_row):
                        if pd.notna(val) and 'вакансия' in str(val).lower():
                            has_vacancy_column = True
                            vacancy_col_idx = idx
                            break
            
            # Если есть заголовок, делаем его названиями столбцов
            if has_header:
                df.columns = df.iloc[0]
                df = df.iloc[1:].reset_index(drop=True)
            
            # Сохраняем данные вкладки
            st.session_state.sheets_data[sheet_name] = {
                'df': df.copy(),
                'has_vacancy_column': has_vacancy_column,
                'vacancy_col_idx': vacancy_col_idx
            }
        
        # Определяем режим работы
        if st.session_state.has_multiple_sheets:
            # Проверяем есть ли вкладки с "вакансия" в названии
            vacancy_sheets = [name for name in sheets_data.keys() 
                            if 'вакансия' in name.lower() or 'вакансии' in name.lower()]
            
            # Проверяем есть ли столбцы "Вакансия" в каких-то вкладках
            sheets_with_vacancy_column = [name for name, data in st.session_state.sheets_data.items() 
                                         if data['has_vacancy_column']]
            
            if vacancy_sheets or len(st.session_state.sheets_data) > 1:
                # Есть вкладки - режим вкладок
                st.session_state.sheet_mode = 'tabs'
                
                # Если еще и столбцы есть - комбинированный режим
                if sheets_with_vacancy_column:
                    st.session_state.sheet_mode = 'both'
                    
                st.info(f"📄 Загружено **{len(sheets_data)}** вкладок | 🎯 **Обнаружен режим работы с вкладками**")
            else:
                st.session_state.sheet_mode = None
                st.info(f"📄 Загружено **{len(sheets_data)}** вкладок")
        else:
            # Одна вкладка - проверяем столбец "Вакансия"
            first_sheet_data = list(st.session_state.sheets_data.values())[0]
            if first_sheet_data['has_vacancy_column']:
                st.session_state.sheet_mode = 'columns'
                st.info(f"📄 Загружено **{len(first_sheet_data['df'])}** строк, **{len(first_sheet_data['df'].columns)}** столбцов | 🎯 **Обнаружен столбец 'Вакансия'**")
            else:
                st.info(f"📄 Загружено **{len(first_sheet_data['df'])}** строк, **{len(first_sheet_data['df'].columns)}** столбцов")
        
        # Для обратной совместимости - сохраняем первую вкладку как основной DF
        first_sheet_name = list(sheets_data.keys())[0]
        st.session_state.original_df = st.session_state.sheets_data[first_sheet_name]['df'].copy()
        st.session_state.has_vacancy_mode = st.session_state.sheet_mode in ['columns', 'tabs', 'both']
        
        # Показываем превью файла
        if has_vacancy_column:
            st.info(f"📄 Загружено **{len(df)}** строк, **{len(df.columns)}** столбцов | 🎯 **Обнаружен столбец 'Вакансия'**")
        else:
            st.info(f"📄 Загружено **{len(df)}** строк, **{len(df.columns)}** столбцов")
        
        with st.expander("👀 Превью загруженного файла (первые 5 строк)"):
            if st.session_state.has_multiple_sheets:
                # Показываем вкладки для выбора
                sheet_tabs = st.tabs(list(st.session_state.sheets_data.keys()))
                for tab, sheet_name in zip(sheet_tabs, st.session_state.sheets_data.keys()):
                    with tab:
                        st.dataframe(st.session_state.sheets_data[sheet_name]['df'].head(), use_container_width=True)
            else:
                # Одна вкладка
                st.dataframe(st.session_state.original_df.head(), use_container_width=True)
          
        if st.button("🚀 Начать сопоставление", type="primary", use_container_width=True):  
            with st.spinner("Обрабатываю..."):  
                # Обрабатываем каждую вкладку
                st.session_state.sheets_results = {}
                
                for sheet_name, sheet_data in st.session_state.sheets_data.items():
                    df_sheet = sheet_data['df']
                    result_df, dup_original, dup_hh, total_dup = match_cities(df_sheet, hh_areas, threshold)  
                    
                    st.session_state.sheets_results[sheet_name] = {
                        'result_df': result_df,
                        'dup_original': dup_original,
                        'dup_hh': dup_hh,
                        'total_dup': total_dup,
                        'has_vacancy_column': sheet_data['has_vacancy_column']
                    }
                
                # Для обратной совместимости - сохраняем первую вкладку
                first_sheet = list(st.session_state.sheets_results.keys())[0]
                st.session_state.result_df = st.session_state.sheets_results[first_sheet]['result_df']
                st.session_state.dup_original = st.session_state.sheets_results[first_sheet]['dup_original']
                st.session_state.dup_hh = st.session_state.sheets_results[first_sheet]['dup_hh']
                st.session_state.total_dup = st.session_state.sheets_results[first_sheet]['total_dup']
                
                st.session_state.processed = True  
                st.session_state.manual_selections = {}  
                st.session_state.search_query = ""
                st.session_state.added_cities = []
                st.session_state.export_mode = None  # Сбрасываем режим экспорта
          
        if st.session_state.processed and st.session_state.result_df is not None:  
            result_df = st.session_state.result_df.copy()  
            dup_original = st.session_state.dup_original  
            dup_hh = st.session_state.dup_hh  
            total_dup = st.session_state.total_dup  
            
            # ПРОВЕРЯЕМ РЕЖИМ ВАКАНСИЙ И ДАЕМ ВЫБОР
            if st.session_state.get('has_vacancy_mode', False):
                st.markdown("---")
                st.subheader("🎯 Выбор режима работы")
                
                # Инициализируем выбранный режим
                if 'export_mode' not in st.session_state:
                    st.session_state.export_mode = None
                
                # CSS для стилизации кнопок режимов
                st.markdown("""
                <style>
                .mode-button {
                    padding: 30px;
                    border-radius: 10px;
                    text-align: center;
                    cursor: pointer;
                    transition: all 0.3s;
                    border: 3px solid #e0e0e0;
                    background: white;
                    margin: 10px 0;
                }
                .mode-button:hover {
                    border-color: #ff4b4b;
                    box-shadow: 0 4px 12px rgba(255, 75, 75, 0.3);
                }
                .mode-button.selected {
                    border-color: #ff4b4b;
                    background: #fff5f5;
                    box-shadow: 0 4px 12px rgba(255, 75, 75, 0.4);
                }
                .mode-icon {
                    font-size: 32px;
                    margin-bottom: 10px;
                }
                .mode-title {
                    font-size: 20px;
                    font-weight: 600;
                    color: #262730;
                    margin-bottom: 8px;
                }
                .mode-desc {
                    font-size: 14px;
                    color: #6c757d;
                }
                </style>
                """, unsafe_allow_html=True)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    selected_split = st.session_state.export_mode == "split"
                    if st.button(
                        "📦 Разделение по вакансиям\n\nОтдельный файл для каждой вакансии", 
                        use_container_width=True, 
                        type="primary" if selected_split else "secondary",
                        key="mode_split"
                    ):
                        st.session_state.export_mode = "split"
                        st.rerun()
                
                with col2:
                    selected_single = st.session_state.export_mode == "single"
                    if st.button(
                        "📄 Единым файлом\n\nБез разделения на вакансии", 
                        use_container_width=True,
                        type="primary" if selected_single else "secondary",
                        key="mode_single"
                    ):
                        st.session_state.export_mode = "single"
                        st.rerun()
                
                # Показываем выбранный режим
                if st.session_state.export_mode == "split":
                    st.success("🎯 **Режим разделения по вакансиям активирован**")
                elif st.session_state.export_mode == "single":
                    st.info("🎯 **Режим единого архива активирован**")
                
                # Если режим еще не выбран, останавливаем дальнейшую обработку
                if st.session_state.export_mode is None:
                    st.info("👆 Выберите режим экспорта для продолжения")
                    st.stop()
                
                # Если выбран режим split - переходим сразу к блоку редактирования по вакансиям
                if st.session_state.export_mode == "split":
                    # Переходим к блоку "Редактирование и выгрузка по вакансиям" ниже
                    pass
                else:
                    # Для режима "single" показываем стандартные блоки
                    pass
            else:
                # Для обычного режима (без вакансий) показываем стандартные блоки
                pass
            
            # Стандартные блоки показываем только если НЕТ режима вакансий
            # Если есть вакансии - используем специальные блоки для split или single
            show_standard_blocks = not st.session_state.get('has_vacancy_mode', False)
            
            if show_standard_blocks:
                st.markdown("---")  
                st.subheader("📊 Результаты")  
                  
                col1, col2, col3, col4, col5, col6 = st.columns(6)  
                  
                total = len(result_df)  
                exact = len(result_df[result_df['Статус'] == '✅ Точное'])  
                similar = len(result_df[result_df['Статус'] == '⚠️ Похожее'])  
                duplicates = len(result_df[result_df['Статус'].str.contains('Дубликат', na=False)])  
                not_found = len(result_df[result_df['Статус'] == '❌ Не найдено'])  
                  
                to_export = len(result_df[  
                    (~result_df['Статус'].str.contains('Дубликат', na=False)) &   
                    (result_df['Итоговое гео'].notna())  
                ])  
                  
                col1.metric("Всего", total)  
                col2.metric("✅ Точных", exact)  
                col3.metric("⚠️ Похожих", similar)  
                col4.metric("🔄 Дубликатов", duplicates)  
                col5.metric("❌ Не найдено", not_found)  
                col6.metric("📤 К выгрузке", to_export)  
                  
                if duplicates > 0:  
                    st.warning(f"""  
                    ⚠️ **Найдено {duplicates} дубликатов:**  
                    - 🔄 По исходному названию: **{dup_original}**  
                    - 🔄 По результату HH: **{dup_hh}**  
                    """)  
            
            # РАННЯЯ ОСТАНОВКА ДЛЯ РЕЖИМА SPLIT
            # Если режим split - пропускаем все стандартные блоки и сразу переходим к вакансиям
            if st.session_state.get('has_vacancy_mode', False) and st.session_state.export_mode == "split":
                # Переход к блоку "ПРОВЕРЯЕМ РЕЖИМ РАБОТЫ" ниже
                pass
            else:
                # Для остальных режимов показываем стандартные блоки
              
                    st.markdown("---")  
                    st.subheader("📋 Таблица сопоставлений")  
              
                    st.text_input(  
                        "🔍 Поиск по таблице",  
                        key="search_query",
                        placeholder="Начните вводить название города...",  
                        label_visibility="visible"  
                    )  
              
                    result_df['sort_priority'] = result_df.apply(  
                        lambda row: 0 if row['Совпадение %'] == 0 else (1 if row['Изменение'] == 'Да' else 2),  
                        axis=1  
                    )  
              
                    result_df_sorted = result_df.sort_values(  
                        by=['sort_priority', 'Совпадение %'],  
                        ascending=[True, True]  
                    ).reset_index(drop=True)  
              
                    if st.session_state.search_query and st.session_state.search_query.strip():  
                        search_lower = st.session_state.search_query.lower().strip()  
                        mask = result_df_sorted.apply(  
                            lambda row: (  
                                search_lower in str(row['Исходное название']).lower() or  
                                search_lower in str(row['Итоговое гео']).lower() or  
                                search_lower in str(row['Регион']).lower() or  
                                search_lower in str(row['Статус']).lower()  
                            ),  
                            axis=1  
                        )  
                        result_df_filtered = result_df_sorted[mask]  
                  
                        if len(result_df_filtered) == 0:  
                            st.warning(f"По запросу **'{st.session_state.search_query}'** ничего не найдено")  
                        else:  
                            st.info(f"Найдено совпадений: **{len(result_df_filtered)}** из {len(result_df_sorted)}")  
                    else:  
                        result_df_filtered = result_df_sorted  
              
                    display_df = result_df_filtered.copy()  
                    display_df = display_df.drop(['row_id', 'sort_priority'], axis=1, errors='ignore')  
              
                    st.dataframe(display_df, use_container_width=True, height=400)  
              
                    # ИЗМЕНЕНО: Исключаем дубликаты из редактирования
                    editable_rows = result_df_sorted[
                        (result_df_sorted['Совпадение %'] <= 90) & 
                        (~result_df_sorted['Статус'].str.contains('Дубликат', na=False))
                    ].copy()  
              
                    if len(editable_rows) > 0:  
                        st.markdown("---")  
                        st.subheader("✏️ Редактирование городов с совпадением ≤ 90%")  
                        st.info(f"Найдено **{len(editable_rows)}** городов, доступных для редактирования")  
                  
                        # Получаем список всех городов России для выбора
                        russia_cities_for_select = []
                        for city_name, city_info in hh_areas.items():
                            if city_info.get('root_parent_id') == '113':
                                russia_cities_for_select.append(city_name)
                        russia_cities_for_select = sorted(russia_cities_for_select)
                
                        for idx, row in editable_rows.iterrows():  
                            with st.container():  
                                col1, col2, col3, col4 = st.columns([2, 3, 1, 1])  
                          
                                with col1:  
                                    st.markdown(f"**{row['Исходное название']}**")  
                          
                                with col2:  
                                    row_id = row['row_id']  
                                    candidates = st.session_state.candidates_cache.get(row_id, [])  
                            
                                    # ИЗМЕНЕНО: Если нет кандидатов или "не найдено", даем выбор из всего списка
                                    if not candidates or row['Статус'] == '❌ Не найдено':
                                        options = ["❌ Нет совпадения"] + russia_cities_for_select
                                
                                        current_value = row['Итоговое гео']
                                
                                        if row_id in st.session_state.manual_selections:
                                            selected_value = st.session_state.manual_selections[row_id]
                                            if selected_value == "❌ Нет совпадения":
                                                default_idx = 0
                                            else:
                                                try:
                                                    default_idx = options.index(selected_value)
                                                except ValueError:
                                                    default_idx = 0
                                        else:
                                            default_idx = 0
                                            if current_value and current_value in options:
                                                default_idx = options.index(current_value)
                                
                                        selected = st.selectbox(
                                            "Выберите город:",
                                            options=options,
                                            index=default_idx,
                                            key=f"select_{row_id}",
                                            label_visibility="collapsed"
                                        )
                                
                                        st.session_state.manual_selections[row_id] = selected
                                
                                    else:
                                        # Есть кандидаты - показываем их
                                        options = ["❌ Нет совпадения"] + [f"{c[0]} ({c[1]:.1f}%)" for c in candidates]  
                                  
                                        current_value = row['Итоговое гео']  
                                  
                                        if row_id in st.session_state.manual_selections:  
                                            selected_value = st.session_state.manual_selections[row_id]  
                                            if selected_value == "❌ Нет совпадения":  
                                                default_idx = 0  
                                            else:  
                                                default_idx = 0  
                                                for i, c in enumerate(candidates):  
                                                    if c[0] == selected_value:  
                                                        default_idx = i + 1  
                                                        break  
                                        else:  
                                            default_idx = 0  
                                            if current_value:  
                                                for i, c in enumerate(candidates):  
                                                    if c[0] == current_value:  
                                                        default_idx = i + 1  
                                                        break  
                                  
                                        selected = st.selectbox(  
                                            "Выберите город:",  
                                            options=options,  
                                            index=default_idx,  
                                            key=f"select_{row_id}",  
                                            label_visibility="collapsed"  
                                        )  
                                  
                                        if selected == "❌ Нет совпадения":  
                                            st.session_state.manual_selections[row_id] = "❌ Нет совпадения"  
                                        else:  
                                            selected_city = selected.rsplit(' (', 1)[0]  
                                            st.session_state.manual_selections[row_id] = selected_city  
                          
                                with col3:  
                                    st.text(f"{row['Совпадение %']}%")  
                          
                                with col4:  
                                    st.text(row['Статус'])  
                          
                                st.markdown("<hr style='margin-top: 5px; margin-bottom: 5px;'>", unsafe_allow_html=True)  
                  
                        if st.session_state.manual_selections:  
                            no_match_count = sum(1 for v in st.session_state.manual_selections.values() if v == "❌ Нет совпадения")  
                            changed_count = len(st.session_state.manual_selections) - no_match_count  
                      
                            st.success(f"✅ Внесено изменений: {changed_count} | ❌ Отмечено как 'Нет совпадения': {no_match_count}")  
                
                        # ============================================
                        # БЛОК: ДОБАВЛЕНИЕ ЛЮБОГО ГОРОДА (только для НЕ split режима)
                        # ============================================
                        st.markdown("---")
                        st.subheader("➕ Добавить дополнительные города")
                        st.info("Добавленные города будут использовать значения из последней строки исходного файла")
                
                        col1, col2, col3 = st.columns([3, 1, 1])
                
                        with col1:
                            # Получаем только города России из справочника
                            russia_cities = []
                            for city_name, city_info in hh_areas.items():
                                if city_info.get('root_parent_id') == '113':
                                    russia_cities.append(city_name)
                    
                            selected_city = st.selectbox(
                                "Выберите город:",
                                options=sorted(russia_cities),
                                key="city_selector",
                                help="Выберите город из справочника HH.ru"
                            )
                
                        with col2:
                            if st.button("➕ Добавить", use_container_width=True, type="primary"):
                                if selected_city and selected_city not in st.session_state.added_cities:
                                    st.session_state.added_cities.append(selected_city)
                                    st.success(f"✅ {selected_city}")
                                elif selected_city in st.session_state.added_cities:
                                    st.warning(f"⚠️ Уже добавлен")
                
                        with col3:
                            if st.button("🗑️ Очистить", use_container_width=True):
                                st.session_state.added_cities = []
                                st.rerun()
                
                        # Показываем список добавленных городов
                        if st.session_state.added_cities:
                            st.success(f"📋 Добавлено городов: **{len(st.session_state.added_cities)}**")
                    
                            # Показываем города в компактном виде
                            added_cities_text = ", ".join(st.session_state.added_cities)
                            st.text_area(
                                "Список добавленных городов:",
                                value=added_cities_text,
                                height=100,
                                disabled=True,
                                label_visibility="collapsed"
                            )
                  
                        st.markdown("---")  
                        st.subheader("💾 Скачать результаты")  
            
                    # Если режим split - переходим сразу к блоку редактирования по вакансиям, пропуская стандартные блоки скачивания
                    if not show_standard_blocks:
                        # Режим split - пропускаем весь блок скачивания и идем к вакансиям
                        pass
                    else:
                        # Обычный режим или single - показываем блок скачивания
              
                        final_result_df = result_df.copy()
                
                        # Применяем ручные изменения
                        if st.session_state.manual_selections:  
                            for row_id, new_value in st.session_state.manual_selections.items():  
                                mask = final_result_df['row_id'] == row_id  
                          
                                if new_value == "❌ Нет совпадения":  
                                    final_result_df.loc[mask, 'Итоговое гео'] = None  
                                    final_result_df.loc[mask, 'ID HH'] = None  
                                    final_result_df.loc[mask, 'Регион'] = None  
                                    final_result_df.loc[mask, 'Совпадение %'] = 0  
                                    final_result_df.loc[mask, 'Изменение'] = 'Нет'  
                                    final_result_df.loc[mask, 'Статус'] = '❌ Не найдено'  
                                else:  
                                    final_result_df.loc[mask, 'Итоговое гео'] = new_value  
                              
                                    if new_value in hh_areas:  
                                        final_result_df.loc[mask, 'ID HH'] = hh_areas[new_value]['id']  
                                        final_result_df.loc[mask, 'Регион'] = hh_areas[new_value]['parent']  
                              
                                    original = final_result_df.loc[mask, 'Исходное название'].values[0]  
                                    final_result_df.loc[mask, 'Изменение'] = 'Да' if check_if_changed(original, new_value) else 'Нет'  
            
            # ПРОВЕРЯЕМ РЕЖИМ РАБОТЫ
            # Если режим split - показываем только блок редактирования по вакансиям/вкладкам
            if st.session_state.get('has_vacancy_mode', False) and st.session_state.export_mode == "split":
                # РЕЖИМ: Разделение по вакансиям/вкладкам с редактированием
                st.markdown("---")
                
                # Определяем тип разделения: по вкладкам или по столбцу вакансий
                if st.session_state.sheet_mode == 'tabs':
                    # РЕЖИМ ВКЛАДОК: каждая вкладка = отдельный файл
                    st.subheader("🎯 Редактирование и выгрузка по вкладкам")
                    
                    # Получаем список вкладок
                    sheet_names = list(st.session_state.sheets_results.keys())
                    st.success(f"📊 Найдено **{len(sheet_names)}** вкладок")
                    
                    # Инициализируем состояние
                    if 'vacancy_files' not in st.session_state:
                        st.session_state.vacancy_files = {}
                    
                    # Создаем вкладки для каждого листа Excel
                    tabs = st.tabs([f"{name}" for name in sheet_names])
                    
                    for tab_idx, (tab, sheet_name) in enumerate(zip(tabs, sheet_names)):
                        with tab:
                            st.markdown(f"### 📄 {sheet_name}")
                            
                            # Получаем данные этой вкладки
                            sheet_result = st.session_state.sheets_results[sheet_name]
                            result_df_sheet = sheet_result['result_df']
                            original_df_sheet = st.session_state.sheets_data[sheet_name]['df']
                            
                            st.info(f"📍 Всего строк: **{len(result_df_sheet)}**")
                            
                            # Показываем таблицу с редактированием (упрощенная версия без редактирования)
                            # Формируем итоговый файл для этой вкладки
                            output_sheet_df = result_df_sheet[
                                (result_df_sheet['Итоговое гео'].notna()) &
                                (~result_df_sheet['Статус'].str.contains('Не найдено', na=False)) &
                                (~result_df_sheet['Статус'].str.contains('Пустое значение', na=False))
                            ].copy()
                            
                            if len(output_sheet_df) > 0:
                                # Берем столбцы из исходного файла
                                original_cols = original_df_sheet.columns.tolist()
                                final_output = pd.DataFrame()
                                final_output[original_cols[0]] = output_sheet_df['Итоговое гео']
                                
                                for col in original_cols[1:]:
                                    if col in original_df_sheet.columns:
                                        indices = output_sheet_df['row_id'].values
                                        final_output[col] = original_df_sheet.iloc[indices][col].values
                                
                                # Удаляем дубликаты
                                final_output['_normalized'] = final_output[original_cols[0]].apply(normalize_city_name)
                                final_output = final_output.drop_duplicates(subset=['_normalized'], keep='first')
                                final_output = final_output.drop(columns=['_normalized'])
                                
                                # Превью
                                st.markdown(f"#### 👀 Превью итогового файла - {sheet_name}")
                                st.dataframe(final_output, use_container_width=True, height=300)
                                
                                # Кнопка скачивания
                                st.markdown("---")
                                safe_sheet_name = str(sheet_name).replace('/', '_').replace('\\', '_')[:50]
                                
                                file_buffer = io.BytesIO()
                                with pd.ExcelWriter(file_buffer, engine='openpyxl') as writer:
                                    final_output.to_excel(writer, index=False, header=True, sheet_name='Результат')
                                file_buffer.seek(0)
                                
                                st.download_button(
                                    label=f"📥 Скачать файл ({len(final_output)} уникальных городов)",
                                    data=file_buffer,
                                    file_name=f"{safe_sheet_name}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    use_container_width=True,
                                    type="primary",
                                    key=f"download_sheet_{sheet_name}_{tab_idx}"
                                )
                                
                                # Сохраняем в session_state для архива
                                st.session_state.vacancy_files[sheet_name] = {
                                    'data': file_buffer.getvalue(),
                                    'name': f"{safe_sheet_name}.xlsx",
                                    'count': len(final_output)
                                }
                            else:
                                st.warning("⚠️ Нет данных для выгрузки")
                    
                    # Кнопка для скачивания всех файлов архивом
                    st.markdown("---")
                    st.markdown("### 📦 Скачать все вкладки одним архивом")
                    
                    if 'vacancy_files' in st.session_state and st.session_state.vacancy_files:
                        total_cities = sum(f['count'] for f in st.session_state.vacancy_files.values())
                        
                        if st.button("📦 Сформировать архив", use_container_width=True, type="primary", key="create_sheets_archive"):
                            zip_buffer = io.BytesIO()
                            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                                for sheet_name, file_info in st.session_state.vacancy_files.items():
                                    zip_file.writestr(file_info['name'], file_info['data'])
                            
                            zip_buffer.seek(0)
                            
                            st.download_button(
                                label=f"📥 Скачать архив ({len(st.session_state.vacancy_files)} вкладок, {total_cities} городов)",
                                data=zip_buffer,
                                file_name=f"all_sheets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                                mime="application/zip",
                                use_container_width=True,
                                type="secondary"
                            )
                    
                    # Останавливаем выполнение
                    st.stop()
                
                elif st.session_state.sheet_mode == 'columns':
                    # РЕЖИМ СТОЛБЦА: оригинальная логика с столбцом "Вакансия"
                    st.subheader("🎯 Редактирование и выгрузка по вакансиям")
                
                # Получаем названия столбцов
                original_cols = st.session_state.original_df.columns.tolist()
                
                # Находим столбец "Вакансия"
                vacancy_col = None
                for col in original_cols:
                    if 'вакансия' in str(col).lower():
                        vacancy_col = col
                        break
                
                if vacancy_col:
                    # Формируем данные для экспорта - используем result_df напрямую
                    export_df = result_df[
                        (result_df['Итоговое гео'].notna()) &
                        (~result_df['Статус'].str.contains('Не найдено', na=False)) &
                        (~result_df['Статус'].str.contains('Пустое значение', na=False))
                    ].copy()
                    
                    # Получаем уникальные вакансии
                    if vacancy_col in export_df.columns:
                        unique_vacancies = sorted(export_df[vacancy_col].dropna().unique())
                        
                        st.success(f"📊 Найдено **{len(unique_vacancies)}** уникальных вакансий")
                        
                        # Инициализируем состояние для редактирования вакансий
                        if 'vacancy_edits' not in st.session_state:
                            st.session_state.vacancy_edits = {}
                        
                        # Создаем вкладки для каждой вакансии
                        tabs = st.tabs([f"{v}" for v in unique_vacancies])
                        
                        for tab_idx, (tab, vacancy) in enumerate(zip(tabs, unique_vacancies)):
                            with tab:
                                # Фильтруем данные по вакансии
                                vacancy_df = export_df[export_df[vacancy_col] == vacancy].copy()
                                
                                st.info(f"📍 Всего строк: **{len(vacancy_df)}**")
                                
                                # Показываем таблицу с возможностью редактирования
                                st.markdown("#### Города для редактирования (совпадение ≤ 90%)")
                                
                                editable_vacancy_rows = vacancy_df[vacancy_df['Совпадение %'] <= 90].copy()
                                
                                # Убираем дубликаты по исходному названию для редактирования
                                if len(editable_vacancy_rows) > 0:
                                    editable_vacancy_rows['_normalized_original'] = editable_vacancy_rows['Исходное название'].apply(normalize_city_name)
                                    editable_vacancy_rows = editable_vacancy_rows.drop_duplicates(subset=['_normalized_original'], keep='first')
                                
                                if len(editable_vacancy_rows) > 0:
                                    st.warning(f"⚠️ Найдено **{len(editable_vacancy_rows)}** городов для проверки")
                                    
                                    # Получаем список всех городов России для выбора
                                    russia_cities_for_select = []
                                    for city_name, city_info in hh_areas.items():
                                        if city_info.get('root_parent_id') == '113':
                                            russia_cities_for_select.append(city_name)
                                    russia_cities_for_select = sorted(russia_cities_for_select)
                                    
                                    for idx, row in editable_vacancy_rows.iterrows():
                                        col1, col2, col3 = st.columns([2, 3, 1])
                                        
                                        with col1:
                                            st.markdown(f"**{row['Исходное название']}**")
                                        
                                        with col2:
                                            row_id = row['row_id']
                                            city_name = row['Исходное название']
                                            current_value = row['Итоговое гео']
                                            current_match = row['Совпадение %']
                                            
                                            # Всегда ищем кандидатов заново для корректного отображения
                                            candidates = get_candidates_by_word(city_name, list(hh_areas.keys()), limit=20)
                                            
                                            # Если есть текущее значение из сопоставления - добавляем его в начало
                                            if current_value and current_value != city_name:
                                                # Проверяем, есть ли уже это значение в кандидатах
                                                candidate_names = [c[0] for c in candidates]
                                                if current_value not in candidate_names:
                                                    # Добавляем текущее значение в начало списка
                                                    candidates.insert(0, (current_value, current_match))
                                            
                                            # Формируем опции - всегда показываем топ кандидатов
                                            if candidates:
                                                options = ["❌ Нет совпадения"] + [f"{c[0]} ({c[1]:.1f}%)" for c in candidates[:20]]
                                            else:
                                                # Если совсем нет кандидатов - показываем хотя бы "Нет совпадения"
                                                options = ["❌ Нет совпадения"]
                                            
                                            # Уникальный ключ для каждой вакансии
                                            unique_key = f"select_{vacancy}_{row_id}_{tab_idx}"
                                            
                                            if row_id in st.session_state.manual_selections:
                                                selected_value = st.session_state.manual_selections[row_id]
                                                if selected_value == "❌ Нет совпадения":
                                                    default_idx = 0
                                                else:
                                                    # Ищем в options, может быть как с процентом, так и без
                                                    default_idx = 0
                                                    for i, opt in enumerate(options):
                                                        if selected_value in opt or opt.startswith(selected_value):
                                                            default_idx = i
                                                            break
                                            else:
                                                # Если manual_selections нет, используем current_value из результата сопоставления
                                                default_idx = 0
                                                if current_value:
                                                    # Ищем current_value в options (может быть с процентом)
                                                    for i, opt in enumerate(options):
                                                        # opt вида "Город (Область) (90.0%)", current_value вида "Город (Область)"
                                                        if opt.startswith(current_value) or current_value in opt:
                                                            default_idx = i
                                                            break
                                            
                                            selected = st.selectbox(
                                                "Выберите город:",
                                                options=options,
                                                index=default_idx,
                                                key=unique_key,
                                                label_visibility="collapsed"
                                            )
                                            
                                            if selected == "❌ Нет совпадения":
                                                st.session_state.manual_selections[row_id] = "❌ Нет совпадения"
                                            else:
                                                if "(" in selected and selected.startswith("❌") == False:
                                                    selected_city = selected.rsplit(' (', 1)[0]
                                                    st.session_state.manual_selections[row_id] = selected_city
                                                else:
                                                    st.session_state.manual_selections[row_id] = selected
                                        
                                        with col3:
                                            st.text(f"{row['Совпадение %']}%")
                                        
                                        st.markdown("<hr style='margin-top: 5px; margin-bottom: 5px;'>", unsafe_allow_html=True)
                                else:
                                    st.success("✅ Все города распознаны корректно!")
                                
                                # ============================================
                                # БЛОК: ДОБАВЛЕНИЕ ГОРОДОВ ДЛЯ ЭТОЙ ВАКАНСИИ
                                # ============================================
                                st.markdown("---")
                                st.markdown("#### ➕ Добавить дополнительные города")
                                
                                # Инициализируем список добавленных городов для каждой вакансии
                                vacancy_key = f"added_cities_{vacancy}"
                                if vacancy_key not in st.session_state:
                                    st.session_state[vacancy_key] = []
                                
                                col_add1, col_add2, col_add3 = st.columns([3, 1, 1])
                                
                                with col_add1:
                                    # Получаем только города России
                                    russia_cities = []
                                    for city_name, city_info in hh_areas.items():
                                        if city_info.get('root_parent_id') == '113':
                                            russia_cities.append(city_name)
                                    
                                    selected_add_city = st.selectbox(
                                        "Выберите город:",
                                        options=sorted(russia_cities),
                                        key=f"city_selector_{vacancy}_{tab_idx}",
                                        help="Выберите город из справочника HH.ru"
                                    )
                                
                                with col_add2:
                                    if st.button("➕ Добавить", use_container_width=True, type="secondary", key=f"add_btn_{vacancy}_{tab_idx}"):
                                        if selected_add_city and selected_add_city not in st.session_state[vacancy_key]:
                                            st.session_state[vacancy_key].append(selected_add_city)
                                            st.success(f"✅ {selected_add_city}")
                                        elif selected_add_city in st.session_state[vacancy_key]:
                                            st.warning(f"⚠️ Уже добавлен")
                                
                                with col_add3:
                                    if st.button("🗑️ Очистить", use_container_width=True, key=f"clear_btn_{vacancy}_{tab_idx}"):
                                        st.session_state[vacancy_key] = []
                                        st.rerun()
                                
                                # Показываем список добавленных городов
                                if st.session_state[vacancy_key]:
                                    st.info(f"📋 Добавлено городов: **{len(st.session_state[vacancy_key])}**")
                                    added_text = ", ".join(st.session_state[vacancy_key])
                                    st.text_area(
                                        "Список:",
                                        value=added_text,
                                        height=80,
                                        disabled=True,
                                        label_visibility="collapsed",
                                        key=f"added_list_{vacancy}_{tab_idx}"
                                    )
                                
                                st.markdown("---")
                                
                                # Формируем итоговый DataFrame для этой вакансии
                                vacancy_final_df = vacancy_df.copy()
                                
                                # Применяем ручные изменения ТОЛЬКО для строк этой вакансии
                                for row_id, new_value in st.session_state.manual_selections.items():
                                    if row_id in vacancy_final_df['row_id'].values:
                                        mask = vacancy_final_df['row_id'] == row_id
                                        
                                        if new_value == "❌ Нет совпадения":
                                            vacancy_final_df.loc[mask, 'Итоговое гео'] = None
                                        else:
                                            vacancy_final_df.loc[mask, 'Итоговое гео'] = new_value
                                            if new_value in hh_areas:
                                                vacancy_final_df.loc[mask, 'ID HH'] = hh_areas[new_value]['id']
                                                vacancy_final_df.loc[mask, 'Регион'] = hh_areas[new_value]['parent']
                                
                                # Исключаем не найденные
                                vacancy_final_df = vacancy_final_df[vacancy_final_df['Итоговое гео'].notna()].copy()
                                
                                # Формируем DataFrame для выгрузки
                                output_vacancy_df = pd.DataFrame()
                                output_vacancy_df[original_cols[0]] = vacancy_final_df['Итоговое гео']
                                
                                for col in original_cols[1:]:
                                    if col != vacancy_col and col in vacancy_final_df.columns:
                                        output_vacancy_df[col] = vacancy_final_df[col].values
                                
                                # Добавляем дополнительные города для этой вакансии
                                vacancy_key = f"added_cities_{vacancy}"
                                if vacancy_key in st.session_state and st.session_state[vacancy_key]:
                                    # Получаем последнюю строку для значений других столбцов
                                    if len(output_vacancy_df) > 0:
                                        last_row_values = output_vacancy_df.iloc[-1].tolist()
                                        
                                        for add_city in st.session_state[vacancy_key]:
                                            new_row = [add_city] + last_row_values[1:]
                                            output_vacancy_df.loc[len(output_vacancy_df)] = new_row
                                
                                # Удаляем дубликаты по городу
                                output_vacancy_df['_normalized'] = output_vacancy_df[original_cols[0]].apply(normalize_city_name)
                                output_vacancy_df = output_vacancy_df.drop_duplicates(subset=['_normalized'], keep='first')
                                output_vacancy_df = output_vacancy_df.drop(columns=['_normalized'])
                                
                                # Показываем превью
                                st.markdown(f"#### 👀 Превью итогового файла - {vacancy}")
                                st.dataframe(output_vacancy_df, use_container_width=True, height=300)
                                
                                # Кнопка выгрузки для этой вакансии
                                st.markdown("---")
                                safe_vacancy_name = str(vacancy).replace('/', '_').replace('\\', '_')[:50]
                                
                                file_buffer = io.BytesIO()
                                with pd.ExcelWriter(file_buffer, engine='openpyxl') as writer:
                                    output_vacancy_df.to_excel(writer, index=False, header=True, sheet_name='Результат')
                                file_buffer.seek(0)
                                
                                st.download_button(
                                    label=f"📥 Скачать файл ({len(output_vacancy_df)} уникальных городов)",
                                    data=file_buffer,
                                    file_name=f"{safe_vacancy_name}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    use_container_width=True,
                                    type="primary",
                                    key=f"download_{vacancy}_{tab_idx}"
                                )
                                
                                # Сохраняем файл в session_state для архива
                                if 'vacancy_files' not in st.session_state:
                                    st.session_state.vacancy_files = {}
                                st.session_state.vacancy_files[vacancy] = {
                                    'data': file_buffer.getvalue(),
                                    'name': f"{safe_vacancy_name}.xlsx",
                                    'count': len(output_vacancy_df)
                                }
                        
                        # Кнопка для скачивания всех файлов архивом
                        st.markdown("---")
                        st.markdown("### 📦 Скачать все вакансии одним архивом")
                        
                        # Проверяем что есть сохраненные файлы
                        if 'vacancy_files' in st.session_state and st.session_state.vacancy_files:
                            total_cities = sum(f['count'] for f in st.session_state.vacancy_files.values())
                            
                            if st.button("📦 Сформировать архив", use_container_width=True, type="primary"):
                                # Создаем ZIP-архив из сохраненных файлов
                                zip_buffer = io.BytesIO()
                                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                                    for vacancy_name, file_info in st.session_state.vacancy_files.items():
                                        zip_file.writestr(file_info['name'], file_info['data'])
                                
                                zip_buffer.seek(0)
                                
                                st.download_button(
                                    label=f"📥 Скачать архив ({len(st.session_state.vacancy_files)} вакансий, {total_cities} городов)",
                                    data=zip_buffer,
                                    file_name=f"all_vacancies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                                    mime="application/zip",
                                    use_container_width=True,
                                    type="secondary"
                                )
                        else:
                            st.info("ℹ️ Пройдитесь по всем вкладкам, чтобы сформировать архив")
                
                # После обработки всех вакансий - останавливаем выполнение
                # Не показываем стандартные блоки для режима split
                st.stop()
                
            elif st.session_state.get('has_vacancy_mode', False) and st.session_state.export_mode == "single":
                # РЕЖИМ: Единым файлом (все вакансии в одном файле, как обычный режим)
                st.markdown("---")
                st.subheader("💾 Скачать результаты")
                
                # Создаем копию result_df для применения изменений
                final_result_df = result_df.copy()
                
                # Применяем ручные изменения к final_result_df
                if st.session_state.manual_selections:
                    for row_id, new_value in st.session_state.manual_selections.items():
                        mask = final_result_df['row_id'] == row_id
                        
                        if new_value == "❌ Нет совпадения":
                            final_result_df.loc[mask, 'Итоговое гео'] = None
                            final_result_df.loc[mask, 'ID HH'] = None
                            final_result_df.loc[mask, 'Регион'] = None
                            final_result_df.loc[mask, 'Совпадение %'] = 0
                            final_result_df.loc[mask, 'Изменение'] = 'Нет'
                            final_result_df.loc[mask, 'Статус'] = '❌ Не найдено'
                        else:
                            final_result_df.loc[mask, 'Итоговое гео'] = new_value
                        
                        if new_value in hh_areas:
                            final_result_df.loc[mask, 'ID HH'] = hh_areas[new_value]['id']
                            final_result_df.loc[mask, 'Регион'] = hh_areas[new_value]['parent']
                        
                        original = final_result_df.loc[mask, 'Исходное название'].values[0]
                        final_result_df.loc[mask, 'Изменение'] = 'Да' if check_if_changed(original, new_value) else 'Нет'
                
                # Добавляем города из added_cities
                if st.session_state.added_cities:
                    original_cols = st.session_state.original_df.columns.tolist()
                    
                    for city in st.session_state.added_cities:
                        if city in hh_areas:
                            last_row = st.session_state.original_df.iloc[-1] if len(st.session_state.original_df) > 0 else {}
                            
                            new_row_data = {col: last_row.get(col, '') for col in original_cols}
                            new_row_data[original_cols[0]] = city
                            
                            new_row = pd.DataFrame([new_row_data])
                            st.session_state.original_df = pd.concat([st.session_state.original_df, new_row], ignore_index=True)
                            
                            final_result_df = pd.concat([final_result_df, pd.DataFrame([{
                                'row_id': len(final_result_df),
                                'Исходное название': city,
                                'Итоговое гео': city,
                                'ID HH': hh_areas[city]['id'],
                                'Регион': hh_areas[city]['parent'],
                                'Совпадение %': 100.0,
                                'Статус': '✅ Добавлено',
                                'Изменение': 'Нет'
                            }])], ignore_index=True)
                
                # Формируем итоговый файл для скачивания (все вакансии вместе)
                original_cols = st.session_state.original_df.columns.tolist()
                
                # Оставляем только строки с найденным гео
                export_df = final_result_df[
                    (final_result_df['Итоговое гео'].notna()) &
                    (~final_result_df['Статус'].str.contains('Не найдено', na=False)) &
                    (~final_result_df['Статус'].str.contains('Пустое значение', na=False))
                ].copy()
                
                # Создаем итоговый DataFrame
                output_df = pd.DataFrame()
                output_df[original_cols[0]] = export_df['Итоговое гео']
                
                for col in original_cols[1:]:
                    if col in st.session_state.original_df.columns:
                        indices = export_df['row_id'].values
                        output_df[col] = st.session_state.original_df.iloc[indices][col].values
                
                # Удаляем дубликаты
                output_df['_normalized'] = output_df[original_cols[0]].apply(normalize_city_name)
                output_df = output_df.drop_duplicates(subset=['_normalized'], keep='first')
                output_df = output_df.drop(columns=['_normalized'])
                
                st.success(f"✅ Готово к выгрузке: **{len(output_df)}** уникальных городов")
                
                # Кнопка скачивания одного файла
                output_all = io.BytesIO()
                with pd.ExcelWriter(output_all, engine='openpyxl') as writer:
                    output_df.to_excel(writer, index=False, header=True, sheet_name='Результат')
                output_all.seek(0)
                
                st.download_button(
                    label=f"📥 Скачать файл ({len(output_df)} городов)",
                    data=output_all,
                    file_name=f"all_vacancies_combined_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    type="primary"
                )
                
                # Превью
                st.markdown("---")
                st.markdown("#### 👀 Превью итогового файла")
                st.dataframe(output_df, use_container_width=True, height=400)
                
            else:
                # ОБЫЧНЫЙ РЕЖИМ (как было раньше)
                col1, col2 = st.columns(2)
                
                with col1:  
                    # Формируем файл для публикатора с исходными столбцами
                    # Исключаем не найденные и дубликаты
                    export_df = final_result_df[
                        (~final_result_df['Статус'].str.contains('Дубликат', na=False)) & 
                        (final_result_df['Итоговое гео'].notna())
                    ].copy()
                    
                    # Получаем названия столбцов из исходного файла
                    original_cols = st.session_state.original_df.columns.tolist()
                    
                    # Формируем итоговый DataFrame: первый столбец - итоговое гео, остальные - из исходного файла
                    publisher_df = pd.DataFrame()
                    publisher_df[original_cols[0]] = export_df['Итоговое гео']
                    
                    # Добавляем остальные столбцы из исходного файла
                    for col in original_cols[1:]:
                        if col in export_df.columns:
                            publisher_df[col] = export_df[col].values
                    
                    # Добавляем дополнительные города с значениями из последней строки
                    if st.session_state.added_cities:
                        # Получаем последнюю строку из исходного файла
                        last_row_values = st.session_state.original_df.iloc[-1].tolist()
                        
                        for city in st.session_state.added_cities:
                            new_row = [city] + last_row_values[1:]  # Город + остальные значения из последней строки
                            publisher_df.loc[len(publisher_df)] = new_row
                        
                        # Удаляем дубликаты
                        publisher_df['_normalized'] = publisher_df[original_cols[0]].apply(normalize_city_name)
                        publisher_df = publisher_df.drop_duplicates(subset=['_normalized'], keep='first')
                        publisher_df = publisher_df.drop(columns=['_normalized'])
                    
                    output_publisher = io.BytesIO()  
                    with pd.ExcelWriter(output_publisher, engine='openpyxl') as writer:  
                        publisher_df.to_excel(writer, index=False, header=False, sheet_name='Результат')  
                    output_publisher.seek(0)  
                      
                    publisher_count = len(publisher_df)  
                      
                    st.download_button(  
                        label=f"📤 Файл для публикатора\n{publisher_count} строк",  
                        data=output_publisher,  
                        file_name=f"geo_result_{uploaded_file.name.rsplit('.', 1)[0]}.xlsx",  
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  
                        use_container_width=True,
                        type="primary",
                        key='download_publisher'  
                    )
                    
                    st.caption("✅ Первый столбец заменен на итоговое гео")
                    st.caption("✅ Остальные столбцы из исходного файла")
                    st.caption("✅ Исключены не найденные и дубликаты")
                    if st.session_state.added_cities:
                        st.caption(f"✅ Добавлено городов: {len(st.session_state.added_cities)}")
                  
                with col2:  
                    output = io.BytesIO()  
                    export_full_df = final_result_df.drop(['row_id', 'sort_priority'], axis=1, errors='ignore')  
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:  
                        export_full_df.to_excel(writer, index=False, sheet_name='Результат')  
                    output.seek(0)  
                      
                    st.download_button(  
                        label="📥 Полный отчет с анализом",  
                        data=output,  
                        file_name=f"full_report_{uploaded_file.name.rsplit('.', 1)[0]}.xlsx",  
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  
                        use_container_width=True,  
                        key='download_full'  
                    )
                    
                    st.caption("📊 Подробный отчет со всеми данными")
                    st.caption("📊 Включает статусы и проценты совпадений")
      
    except Exception as e:  
        st.error(f"❌ Ошибка обработки файла: {str(e)}")  
        import traceback  
        st.code(traceback.format_exc())  

st.markdown("---")

# ============================================
# БЛОК: ВЫБОР РЕГИОНОВ
# ============================================
st.header("🗺️ Выбор регионов")
st.markdown("Выберите федеральные округа и области для получения списка всех городов")

if hh_areas is not None:
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Федеральные округа")
        selected_districts = st.multiselect(
            "Выберите федеральные округа:",
            options=list(FEDERAL_DISTRICTS.keys()),
            help="Можно выбрать несколько округов",
            key="districts_select"
        )
    
    # Формируем список доступных регионов на основе выбранных округов
    available_regions = []
    if selected_districts:
        for district in selected_districts:
            available_regions.extend(FEDERAL_DISTRICTS[district])
    else:
        # Если округа не выбраны, показываем все регионы
        for regions in FEDERAL_DISTRICTS.values():
            available_regions.extend(regions)
    
    with col2:
        st.subheader("Области/Регионы")
        selected_regions = st.multiselect(
            "Выберите области/регионы:",
            options=sorted(available_regions),
            help="Можно выбрать несколько регионов",
            key="regions_select"
        )
    
    # Определяем, какие регионы использовать для поиска
    regions_to_search = []
    
    # Если выбраны конкретные регионы, используем их
    if selected_regions:
        regions_to_search = selected_regions
    # Если выбраны только округа (без конкретных регионов), берем все регионы из этих округов
    elif selected_districts:
        for district in selected_districts:
            regions_to_search.extend(FEDERAL_DISTRICTS[district])
    
    # Кнопки действий
    col_btn1, col_btn2 = st.columns(2)
    
    with col_btn1:
        # Показываем кнопку только если что-то выбрано
        if regions_to_search:
            # Информация о выборе
            if selected_regions:
                st.info(f"📍 Выбрано регионов: **{len(selected_regions)}**")
            elif selected_districts:
                st.info(f"📍 Выбрано округов: **{len(selected_districts)}** (включает {len(regions_to_search)} регионов)")
            
            if st.button("🔍 Получить список городов по регионам", type="primary", use_container_width=True):
                with st.spinner("Формирую список городов..."):
                    cities_df = get_cities_by_regions(hh_areas, regions_to_search)
                    
                    if not cities_df.empty:
                        st.success(f"✅ Найдено **{len(cities_df)}** городов в выбранных регионах")
                        
                        # Показываем таблицу
                        st.dataframe(cities_df, use_container_width=True, height=400)
                        
                        # Кнопки для скачивания
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            # Полный отчет
                            output_full = io.BytesIO()
                            with pd.ExcelWriter(output_full, engine='openpyxl') as writer:
                                cities_df.to_excel(writer, index=False, sheet_name='Города')
                            output_full.seek(0)
                            
                            st.download_button(
                                label=f"📥 Скачать полный отчет ({len(cities_df)} городов)",
                                data=output_full,
                                file_name="cities_full_report.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True,
                                key="download_regions_full"
                            )
                        
                        with col2:
                            # Только названия городов для публикатора
                            publisher_df = pd.DataFrame({'Город': cities_df['Город']})
                            output_publisher = io.BytesIO()
                            with pd.ExcelWriter(output_publisher, engine='openpyxl') as writer:
                                publisher_df.to_excel(writer, index=False, header=False, sheet_name='Гео')
                            output_publisher.seek(0)
                            
                            st.download_button(
                                label=f"📤 Для публикатора ({len(cities_df)} городов)",
                                data=output_publisher,
                                file_name="cities_for_publisher.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True,
                                key="download_regions_publisher"
                            )
                    else:
                        st.warning("⚠️ Города не найдены в выбранных регионах")
        else:
            st.info("👆 Выберите федеральные округа или конкретные регионы для получения списка городов")
    
    with col_btn2:
        # Кнопка для выгрузки всех городов
        if st.button("🌍 Выгрузить ВСЕ города из справочника", type="secondary", use_container_width=True):
            with st.spinner("Формирую полный список городов..."):
                all_cities_df = get_all_cities(hh_areas)
                
                if not all_cities_df.empty:
                    st.success(f"✅ Найдено **{len(all_cities_df)}** городов в справочнике HH.ru")
                    
                    # Показываем таблицу
                    st.dataframe(all_cities_df, use_container_width=True, height=400)
                    
                    # Кнопки для скачивания
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Полный отчет
                        output_all_full = io.BytesIO()
                        with pd.ExcelWriter(output_all_full, engine='openpyxl') as writer:
                            all_cities_df.to_excel(writer, index=False, sheet_name='Города')
                        output_all_full.seek(0)
                        
                        st.download_button(
                            label=f"📥 Скачать полный отчет ({len(all_cities_df)} городов)",
                            data=output_all_full,
                            file_name="all_cities_full_report.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                            key="download_all_full"
                        )
                    
                    with col2:
                        # Только названия городов для публикатора
                        publisher_all_df = pd.DataFrame({'Город': all_cities_df['Город']})
                        output_all_publisher = io.BytesIO()
                        with pd.ExcelWriter(output_all_publisher, engine='openpyxl') as writer:
                            publisher_all_df.to_excel(writer, index=False, header=False, sheet_name='Гео')
                        output_all_publisher.seek(0)
                        
                        st.download_button(
                            label=f"📤 Для публикатора ({len(all_cities_df)} городов)",
                            data=output_all_publisher,
                            file_name="all_cities_for_publisher.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                            key="download_all_publisher"
                        )
                else:
                    st.warning("⚠️ Не удалось получить список городов")

st.markdown("---")  
st.markdown(  
    "Сделано с ❤️ | Данные из API HH.ru",  
    unsafe_allow_html=True  
)
