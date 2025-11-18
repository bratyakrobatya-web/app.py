import streamlit as st
import requests
import pandas as pd
from rapidfuzz import fuzz, process
import io
import re
import zipfile
from datetime import datetime
import os

# Version: 3.3.2 - Fixed: corrected all indentation in single mode block

# ============================================
# КОНФИГУРАЦИЯ: API КЛЮЧИ
# ============================================
# Для безопасного хранения ключа используется следующий приоритет:
# 1. Streamlit secrets (.streamlit/secrets.toml)
# 2. Переменная окружения ANTHROPIC_API_KEY
#
# Для настройки создайте файл .streamlit/secrets.toml с содержимым:
# ANTHROPIC_API_KEY = "ваш-ключ-здесь"

# Настройка страницы
st.set_page_config(
    page_title="Синхронизатор",
    page_icon="",
    layout="wide"
)

# Кастомный CSS для современного дизайна
st.markdown("""
<style>
    /* Подключение шрифта Golos Text через Google Fonts - ДОЛЖНО БЫТЬ ПЕРВЫМ */
    @import url('https://fonts.googleapis.com/css2?family=Golos+Text:wght@400&display=swap');

    /* =============================================== */
    /* CSS ПЕРЕМЕННЫЕ ДЛЯ ГРАДИЕНТА */
    /* =============================================== */
    :root {
        /* Базовый красный цвет для кнопок */
        --button-color: #f4301f;
        --button-hover: #d32f2f;

        /* Цвета для UI элементов (красный) */
        --ui-color: #f4301f;
        --ui-shadow: rgba(244, 48, 31, 0.25);
        --ui-shadow-hover: rgba(244, 48, 31, 0.35);

        /* Цвета для кнопок (красный) */
        --primary-color: #f4301f;
        --primary-dark: #d32f2f;

        /* Тени для кнопок - красный */
        --shadow-primary: rgba(244, 48, 31, 0.25);
        --shadow-hover: rgba(244, 48, 31, 0.35);
        --shadow-glow: 0 6px 20px rgba(244, 48, 31, 0.35);
    }

    /* Анимация для логотипа */
    @keyframes rotate {
        from { transform: rotate(0deg); }
        to { transform: rotate(360deg); }
    }

    .rotating-earth {
        display: inline-block;
        animation: rotate 6s linear infinite;
        vertical-align: middle;
        margin-right: 8px;
        width: 1em;
        height: 1em;
    }

    .rotating-earth img {
        width: 100%;
        height: 100%;
        display: block;
        filter: brightness(0) saturate(100%) invert(24%) sepia(95%) saturate(3456%) hue-rotate(353deg) brightness(99%) contrast(93%);
    }

    /* Круги с цифрами с градиентом */
    .step-number {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 32px;
        height: 32px;
        background: transparent;
        color: var(--ui-color);
        border: 2px solid var(--ui-color);
        border-radius: 50%;
        font-family: 'Golos Text' !important;
        font-weight: normal;
        font-size: 16px;
        margin-right: 8px;
        vertical-align: middle;
    }

    /* Белая галочка в круге с градиентом */
    .check-circle {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 20px;
        height: 20px;
        background: var(--ui-color);
        color: white;
        border-radius: 50%;
        font-family: 'Golos Text' !important;
        font-weight: normal;
        font-size: 14px;
        margin-right: 8px;
        vertical-align: middle;
    }

    .main-title {
        display: inline-block;
        font-family: 'Golos Text' !important;
        font-size: 3em;
        font-weight: normal;
        vertical-align: middle;
        margin: 0;
        color: var(--ui-color);
    }

    .title-container {
        display: flex;
        align-items: center;
        margin-bottom: 20px;
    }

    /* Адаптация логотипа для sidebar */
    [data-testid="stSidebar"] .rotating-earth {
        width: 0.67em;
        height: 0.67em;
        margin-right: 6px;
    }

    [data-testid="stSidebar"] .main-title {
        font-size: 1.5em;
    }

    /* Базовые стили */
    html, body, [class*="css"] {
        font-family: 'Golos Text' !important;
        font-size: 14px;
    }

    /* Применяем шрифт ко всем элементам Streamlit, кроме иконок */
    .stButton button, .stDownloadButton button,
    .stTextInput input, .stSelectbox, .stMultiSelect,
    .stTextArea textarea, .stNumberInput input,
    [data-testid="stFileUploader"], .uploadedFileName,
    p, div, label, h1, h2, h3, h4, h5, h6 {
        font-family: 'Golos Text' !important;
    }

    /* Исключаем иконочные шрифты из глобального применения */
    span[data-icon], span[class*="icon"], span.material-icons, span[class*="material"],
    button span[data-icon], button span[class*="icon"],
    [data-testid="collapsedControl"] span,
    [data-testid="stSidebarCollapsedControl"] span {
        font-family: 'Material Symbols Outlined', 'Material Icons', system-ui ;
    }

    /* Улучшение качества изображений - максимальная четкость */
    img {
        image-rendering: high-quality;
        image-rendering: -webkit-optimize-contrast;
        -ms-interpolation-mode: bicubic;
        max-width: 100%;
        height: auto;
    }

    /* Специально для логотипа в sidebar - ультра-качество */
    [data-testid="stSidebar"] img {
        image-rendering: high-quality ;
        image-rendering: -webkit-optimize-contrast ;
        backface-visibility: hidden;
        transform: translateZ(0);
        -webkit-font-smoothing: antialiased;
        will-change: transform;
        filter: contrast(1.02) saturate(1.05);
    }

    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }

    /* Заголовки */
    h1 {
        font-family: 'Golos Text' !important;
        font-weight: normal;
        color: #1a1a1a;
        margin-bottom: 0.5rem;
        font-size: 33px;
    }

    h2 {
        font-family: 'Golos Text' !important;
        font-weight: normal;
        color: #2d2d2d;
        margin-top: 2rem;
        margin-bottom: 1rem;
        font-size: 10px;
    }

    h3 {
        font-family: 'Golos Text' !important;
        font-weight: normal;
        color: #4a4a4a;
        font-size: 10px;
    }

    /* =============================================== */
    /* СТИЛИ КНОПОК - ГРАДИЕНТНЫЙ СТИЛЬ ДЛЯ ВСЕХ КНОПОК */
    /* =============================================== */

    /* Все обычные кнопки (включая primary и secondary) - БАЗОВЫЙ КРАСНЫЙ */
    .stButton>button {
        border-radius: 20px ;
        padding: 10px 20px ;
        font-family: 'Golos Text' !important;
        font-weight: normal ;
        font-size: 14px ;
        background: var(--button-color) ;
        border: none ;
        transition: all 0.3s ease ;
        box-shadow: none ;
        color: white ;
        cursor: pointer ;
    }

    /* Текст внутри кнопок - Regular шрифт */
    .stButton>button, .stButton>button span, .stButton>button p,
    .stButton>button div, .stButton>button * {
        font-family: 'Golos Text' !important;
        font-weight: normal ;
    }

    .stButton>button:hover {
        background: var(--button-hover) ;
        transform: translateY(-2px) ;
        box-shadow: var(--shadow-glow) ;
        color: white ;
    }

    .stButton>button:active {
        transform: translateY(0px) ;
        box-shadow: none ;
    }

    /* Download кнопки - БАЗОВЫЙ КРАСНЫЙ */
    .stDownloadButton>button {
        border-radius: 20px ;
        padding: 10px 20px ;
        font-family: 'Golos Text' !important;
        font-weight: normal ;
        font-size: 14px ;
        background: var(--button-color) ;
        border: none ;
        transition: all 0.3s ease ;
        box-shadow: none ;
        color: white ;
        cursor: pointer ;
    }

    /* Текст внутри download кнопок - Regular шрифт */
    .stDownloadButton>button, .stDownloadButton>button span, .stDownloadButton>button p,
    .stDownloadButton>button div, .stDownloadButton>button * {
        font-family: 'Golos Text' !important;
        font-weight: normal ;
    }

    .stDownloadButton>button:hover {
        background: var(--button-hover) ;
        transform: translateY(-2px) ;
        box-shadow: var(--shadow-glow) ;
        color: white ;
    }

    .stDownloadButton>button:active {
        transform: translateY(0px) ;
        box-shadow: none ;
    }

    /* Tab кнопки - размер как обычные кнопки */
    .stTabs [data-baseweb="tab-list"] button {
        padding: 10px 20px ;
        font-size: 14px ;
        font-family: 'Golos Text' !important;
        font-weight: normal ;
    }

    /* File Uploader */
    [data-testid="stFileUploader"] {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        border: 2px dashed #adb5bd;
        border-radius: 16px;
        padding: 2.5rem;
        transition: all 0.3s ease;
    }

    [data-testid="stFileUploader"]:hover {
        border-color: var(--ui-color);
        background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%);
    }

    /* Центрирование содержимого file uploader */
    [data-testid="stFileUploader"] > div {
        display: flex;
        justify-content: center;
        align-items: center;
    }


    .uploadedFileName {
        color: var(--ui-color);
        font-weight: normal;
    }

    /* Inputs - Selectbox с черной окантовкой */
    div[data-baseweb="select"] > div,
    .stSelectbox > div > div,
    [data-testid="stSelectbox"] > div > div {
        position: relative;
        border: 2px solid #1a1a1a ;
        border-radius: 10px ;
        background: white ;
        transition: all 0.3s ease ;
        cursor: pointer ;
    }

    div[data-baseweb="select"] > div:hover,
    .stSelectbox:hover > div > div,
    [data-testid="stSelectbox"]:hover > div > div {
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.15);
        filter: brightness(1.02);
    }

    div[data-baseweb="select"] > div:focus-within,
    .stSelectbox > div > div:focus-within,
    [data-testid="stSelectbox"] > div > div:focus-within {
        box-shadow: 0 0 0 3px rgba(0, 0, 0, 0.1) ;
    }

    .stTextInput > div > div {
        border-radius: 10px;
        border: 1px solid #dee2e6;
    }

    /* MultiSelect с черной окантовкой */
    [data-testid="stMultiSelect"] [data-baseweb="select"] > div {
        border: 2px solid #1a1a1a ;
        border-radius: 10px ;
        background: white ;
        transition: all 0.3s ease ;
        cursor: pointer ;
    }

    [data-testid="stMultiSelect"] [data-baseweb="select"] > div:hover {
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.15);
        filter: brightness(1.02);
    }

    /* Информационные блоки - С ГРАДИЕНТОМ */
    .stInfo {
        background: linear-gradient(135deg, #fff5f5 0%, #ffe8e8 100%) ;
        border-left: 5px solid var(--ui-color) ;
        border-radius: 10px;
        padding: 1rem;
    }

    .stSuccess {
        background: rgba(244, 48, 31, 0.1) ;
        border: 2px solid var(--ui-color) ;
        border-radius: 10px;
        padding: 1rem;
        color: #1a1a1a ;
    }

    .stSuccess > div {
        color: #1a1a1a ;
    }

    .stSuccess p, .stSuccess strong {
        color: #1a1a1a ;
    }

    .stWarning {
        background: linear-gradient(135deg, #fff5f5 0%, #ffe8e8 100%) ;
        border-left: 5px solid var(--ui-color) ;
        border-radius: 10px;
        padding: 1rem;
    }

    .stError {
        background: linear-gradient(135deg, #fff5f5 0%, #ffe8e8 100%) ;
        border-left: 5px solid var(--ui-color) ;
        border-radius: 10px;
        padding: 1rem;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #f8f9fa 0%, #ffffff 100%);
        border-right: 1px solid #e9ecef;
    }

    [data-testid="stSidebar"] h1 {
        font-size: 1.5rem;
        padding-bottom: 1rem;
        border-bottom: 2px solid var(--ui-color);
    }

    /* Expander */
    .streamlit-expanderHeader {
        background: #f8f9fa;
        border-radius: 10px;
        font-weight: normal;
        border: 1px solid #e9ecef;
    }

    .streamlit-expanderHeader:hover {
        background: #e9ecef;
    }

    /* Slider - простой стиль */
    .stSlider > div > div {
        background: #dee2e6 ;
        height: 4px ;
    }

    /* Slider - активная часть с градиентом */
    .stSlider > div > div > div {
        background: var(--ui-color) ;
    }

    /* Тумблер слайдера - простой круг */
    .stSlider > div > div > div > div {
        background-color: white ;
        border: 2px solid var(--ui-color) ;
        height: 20px ;
    }

    .stSlider > div > div > div > div:hover {
        background-color: white ;
        box-shadow: 0 0 8px var(--ui-shadow) ;
        border: 2px solid var(--ui-color) ;
    }

    /* Вкладки */
    .stTabs [data-baseweb="tab-list"] {
        gap: 12px;
        background: transparent;
    }

    .stTabs [data-baseweb="tab"] {
        height: 60px;
        padding: 0px 24px;
        border-radius: 10px 10px 0 0;
        font-weight: normal;
        background: #f8f9fa;
        border: 1px solid #dee2e6;
        border-bottom: none;
        font-size: 20px;
        transition: all 0.3s ease;
    }

    .stTabs [aria-selected="true"] {
        background: var(--ui-color);
        color: white;
        border-bottom: 2px solid var(--ui-color);
    }

    .stTabs [data-baseweb="tab"]:hover {
        background: #e9ecef;
    }

    .stTabs [aria-selected="true"]:hover {
        filter: brightness(1.1);
    }

    /* DataFrame */
    [data-testid="stDataFrame"] {
        border-radius: 10px;
        overflow: hidden;
        border: 1px solid #e9ecef;
    }

    [data-testid="stDataFrameResizable"] {
        border-radius: 10px;
    }

    /* Checkbox */
    div.stCheckbox {
        padding: 0.5rem;
        border-radius: 8px;
        transition: background 0.2s ease;
    }

    div.stCheckbox:hover {
        background: #f8f9fa;
    }

    /* Метрики */
    .stMetric {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        border: 1px solid #f0f0f0;
    }

    /* Прогресс бар - базовый черный цвет вместо оранжевого */
    .stProgress > div > div > div > div {
        background-color: #1a1a1a ;
    }

    .stProgress [data-testid="stProgressBar"] > div > div {
        background-color: #1a1a1a ;
    }

    /* Divider */
    hr {
        margin: 2rem 0;
        border: none;
        height: 2px;
        background: linear-gradient(90deg, transparent 0%, #dee2e6 50%, transparent 100%);
    }


    /* Кнопка Browse files в File Uploader - базовый красный */
    [data-testid="stFileUploader"] button {
        background: var(--button-color) ;
        border: none ;
        color: white ;
        border-radius: 20px ;
        padding: 10px 20px ;
        font-weight: normal ;
        font-size: 14px ;
        transition: all 0.3s ease ;
        cursor: pointer ;
    }

    [data-testid="stFileUploader"] button:hover {
        background: var(--button-hover) ;
        transform: translateY(-2px);
        box-shadow: var(--shadow-glow) ;
    }

    /* Теги в мультиселекте - красный цвет вместо оранжевого */
    [data-testid="stMultiSelect"] span[data-baseweb="tag"] {
        background-color: var(--ui-color) ;
        color: white ;
        border-radius: 6px;
    }

    [data-testid="stMultiSelect"] span[data-baseweb="tag"]:hover {
        background-color: #d42817 ;
    }

    /* Глобальные стили для ссылок - черный цвет вместо оранжевого */
    a {
        color: #1a1a1a ;
        text-decoration: none;
    }

    a:hover {
        color: #000000 ;
        text-decoration: none ;
    }

    a:visited {
        color: #1a1a1a ;
    }

    /* =============================================== */
    /* СТИЛЬ МАТРИЦЫ ДЛЯ БЛОКА КОДА В СВЕРКАХ */
    /* =============================================== */

    /* Контейнер для блоков кода в стиле Матрицы */
    .matrix-code-section div[data-testid="stCodeBlock"] {
        max-height: 250px !important;
        overflow-y: auto !important;
        background-color: #000000 !important;
        border: 1px solid #00FF00 !important;
    }

    /* Кнопка копирования */
    .matrix-code-section div[data-testid="stCodeBlock"] button {
        background-color: #f4301f !important;
        color: white !important;
        border: 2px solid #c42d1a !important;
        font-weight: bold !important;
        transition: all 0.3s ease !important;
        padding: 8px 16px !important;
    }

    .matrix-code-section div[data-testid="stCodeBlock"] button:hover {
        background-color: #c42d1a !important;
        transform: scale(1.05) !important;
        box-shadow: 0 4px 12px rgba(244, 48, 31, 0.4) !important;
    }

    /* Стиль Матрицы для поля кода - максимальная специфичность */
    .matrix-code-section div[data-testid="stCodeBlock"] pre,
    .matrix-code-section div[data-testid="stCodeBlock"] pre[class],
    .matrix-code-section [data-testid="stCodeBlock"] pre {
        background-color: #000000 !important;
        background: #000000 !important;
        color: #00FF00 !important;
        font-family: 'Courier New', Consolas, Monaco, monospace !important;
        text-shadow: 0 0 5px #00FF00 !important;
    }

    .matrix-code-section div[data-testid="stCodeBlock"] code,
    .matrix-code-section div[data-testid="stCodeBlock"] code[class],
    .matrix-code-section [data-testid="stCodeBlock"] code {
        background-color: #000000 !important;
        background: #000000 !important;
        color: #00FF00 !important;
        font-family: 'Courier New', Consolas, Monaco, monospace !important;
    }

    .matrix-code-section div[data-testid="stCodeBlock"] pre code,
    .matrix-code-section [data-testid="stCodeBlock"] pre code {
        background-color: #000000 !important;
        background: #000000 !important;
        color: #00FF00 !important;
    }

    /* Переопределяем стили для всех дочерних элементов */
    .matrix-code-section [data-testid="stCodeBlock"] * {
        background-color: #000000 !important;
    }

    .matrix-code-section [data-testid="stCodeBlock"] span {
        color: #00FF00 !important;
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
        "Республика Адыгея", "Республика Калмыкия", "Республика Крым",
        "Краснодарский край", "Астраханская область", "Волгоградская область",
        "Ростовская область", "Севастополь"
    ],
    "Северо-Западный федеральный округ": [
        "Республика Карелия", "Республика Коми", "Архангельская область",
        "Вологодская область", "Калининградская область", "Ленинградская область",
        "Мурманская область", "Новгородская область", "Псковская область",
        "Санкт-Петербург", "Ненецкий автономный округ"
    ],
    "Дальневосточный федеральный округ": [
        "Республика Бурятия", "Республика Саха (Якутия)", "Забайкальский край",
        "Камчатский край", "Приморский край", "Хабаровский край",
        "Амурская область", "Магаданская область", "Сахалинская область",
        "Еврейская автономная область", "Чукотский автономный округ"
    ],
    "Сибирский федеральный округ": [
        "Республика Алтай", "Республика Тыва", "Республика Хакасия",
        "Алтайский край", "Красноярский край", "Иркутская область",
        "Кемеровская область", "Новосибирская область", "Омская область",
        "Томская область"
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

# ============================================
# ФУНКЦИИ
# ============================================
def get_russian_cities(hh_areas):
    """Возвращает список только российских городов из справочника HH"""
    russia_id = '113'
    return [
        city_name for city_name, city_info in hh_areas.items()
        if city_info.get('root_parent_id', '') == russia_id
    ]

def remove_header_row_if_needed(df, first_col_name):
    """
    Удаляет первую строку, если она является заголовком (не реальными данными города).
    Логика как в публикаторе.
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

def normalize_city_name(text):
    """Нормализует название города: ё->е, нижний регистр, убирает лишние пробелы"""
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

            # Получаем информацию о часовом поясе напрямую из объекта
            utc_offset = area.get('utc_offset', '')

            areas_dict[area_name] = {
                'id': area_id,
                'name': area_name,
                'parent': parent_name,
                'parent_id': parent_id,
                'root_parent_id': current_root_id,  # ID страны верхнего уровня
                'utc_offset': utc_offset  # Смещение UTC (например, "+03:00")
            }

            if area.get('areas'):
                parse_areas(area['areas'], area_name, area_id, current_root_id)  
      
    parse_areas(data)
    return areas_dict

@st.cache_data(ttl=3600)
def load_population_data():
    """Загружает данные о населении городов из CSV файла"""
    try:
        # Читаем CSV с разделителем точка с запятой
        df = pd.read_csv('population.csv', sep=';', encoding='utf-8')

        # Создаем словарь {город: население}
        population_dict = {}
        for _, row in df.iterrows():
            city_name = row['ГОРОДА']
            population = int(row['Население'])
            population_dict[city_name] = population

        return population_dict
    except FileNotFoundError:
        st.warning("⚠️ Файл population.csv не найден. Фильтр по населению будет недоступен.")
        return {}
    except Exception as e:
        st.error(f"❌ Ошибка загрузки данных о населении: {str(e)}")
        return {}

def get_federal_district_by_region(region_name):
    """Определяет федеральный округ по названию региона"""
    for district, regions in FEDERAL_DISTRICTS.items():
        if region_name in regions:
            return district
    return "Не определен"

def get_cities_by_regions(hh_areas, selected_regions):
    """Получает все города из выбранных регионов (только Россия, только города)"""
    cities = []

    # Загружаем данные о населении
    population_dict = load_population_data()

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
            city_name_normalized = normalize_city_name(city_name)

            # Используем ТОЧНОЕ совпадение, а не substring matching
            # Это предотвращает ложные срабатывания (например: "Москва" in "Московская область")
            if (region_normalized == parent_normalized or
                region_normalized == city_name_normalized):
                # Получаем часовой пояс
                utc_offset = city_info.get('utc_offset', '')

                # Вычисляем разницу с Москвой (UTC+3)
                moscow_offset = 3
                city_offset_hours = 0
                if utc_offset:
                    try:
                        # Парсим смещение вида "+03:00" или "-05:00"
                        sign = 1 if utc_offset[0] == '+' else -1
                        hours = int(utc_offset[1:3])
                        city_offset_hours = sign * hours
                    except:
                        city_offset_hours = 0

                diff_with_moscow = city_offset_hours - moscow_offset

                # Определяем федеральный округ
                region = parent if parent else 'Россия'
                federal_district = get_federal_district_by_region(region)

                # Получаем население из словаря (0 если данных нет)
                population = population_dict.get(city_name, 0)

                cities.append({
                    'Город': city_name,
                    'ID HH': city_info['id'],
                    'Регион': region,
                    'Федеральный округ': federal_district,
                    'UTC': utc_offset,
                    'Разница с МСК': f"{diff_with_moscow:+d}ч" if diff_with_moscow != 0 else "0ч",
                    'Население': population
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

    # Загружаем данные о населении
    population_dict = load_population_data()

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
        
        # Получаем часовой пояс
        utc_offset = city_info.get('utc_offset', '')

        # Вычисляем разницу с Москвой (UTC+3)
        moscow_offset = 3
        city_offset_hours = 0
        if utc_offset:
            try:
                # Парсим смещение вида "+03:00" или "-05:00"
                sign = 1 if utc_offset[0] == '+' else -1
                hours = int(utc_offset[1:3])
                city_offset_hours = sign * hours
            except:
                city_offset_hours = 0

        diff_with_moscow = city_offset_hours - moscow_offset

        # Определяем федеральный округ
        region = parent if parent else 'Россия'
        federal_district = get_federal_district_by_region(region)

        # Получаем население из словаря (0 если данных нет)
        population = population_dict.get(city_name, 0)

        cities.append({
            'Город': city_name,
            'ID HH': city_info['id'],
            'Регион': region,
            'Федеральный округ': federal_district,
            'UTC': utc_offset,
            'Разница с МСК': f"{diff_with_moscow:+d}ч" if diff_with_moscow != 0 else "0ч",
            'Население': population
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
    """Получает кандидатов по совпадению начального слова с применением PREFERRED_MATCHES"""
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

def smart_match_city(client_city, hh_city_names, hh_areas, threshold=85):
    """Умное сопоставление города с сохранением кандидатов и учетом предпочтительных совпадений"""

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

def match_cities(original_df, hh_areas, threshold=85, sheet_name=None):
    """Сопоставляет города с сохранением кандидатов и всех столбцов"""
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

def merge_cities_files(df1, df2, hh_areas, threshold=85):
    """
    Объединяет два файла с городами с удалением дублей.

    Args:
        df1: Первый DataFrame с городами
        df2: Второй DataFrame с городами
        hh_areas: Справочник HH.ru
        threshold: Порог совпадения для сопоставления

    Returns:
        merged_df: Объединенный DataFrame без дублей
        stats: Словарь со статистикой объединения
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

# ============================================
# ИНТЕРФЕЙС
# ============================================

# Загрузка иконки synchronize.png
try:
    import base64
    from io import BytesIO
    from PIL import Image

    sync_icon_image = Image.open("synchronize.png")
    buffered = BytesIO()
    sync_icon_image.save(buffered, format="PNG")
    sync_icon_base64 = base64.b64encode(buffered.getvalue()).decode()
    SYNC_ICON = f'<img src="data:image/png;base64,{sync_icon_base64}" style="width: 1em; height: 1em; display: inline-block;">'
except Exception as e:
    # Fallback если файл не найден
    SYNC_ICON = '🔄'

# Загрузка справочника HH
try:
    hh_areas = get_hh_areas()
except Exception as e:
    st.error(f"❌ Ошибка загрузки справочника: {str(e)}")
    hh_areas = None

# ============================================
# ГЛАВНЫЙ ЗАГОЛОВОК
# ============================================
st.markdown('''
<div style="margin-bottom: 2rem;">
    <h1 style="text-align: left; color: #f4301f; margin-bottom: 0.3rem;">🪗 ГАРМОНЬ</h1>
    <p style="text-align: left; color: #6c757d; font-size: 14px; margin-top: 0;">
        Сыграй порядок из хаоса
    </p>
</div>
''', unsafe_allow_html=True)
st.markdown("---")

# ============================================
# БЛОК: ПРОВЕРКА ГЕО
# ============================================
if hh_areas:
    st.markdown('<div id="проверка-гео"></div>', unsafe_allow_html=True)
    st.header("🔍 Проверка гео и выгрузка базы")

    # Получаем только города России
    russia_cities = []
    for city_name, city_info in hh_areas.items():
        if city_info.get('root_parent_id') == '113':
            russia_cities.append(city_name)

    # Мультиселект для выбора городов
    selected_cities = st.multiselect(
        "Выберите город(а) для проверки и выгрузки:",
        options=sorted(russia_cities),
        key="geo_checker",
        help="Выберите один или несколько городов"
    )

    # Показываем информацию о выбранных городах
    if selected_cities:
        st.markdown(f"**Выбрано городов:** {len(selected_cities)}")

        # Создаем DataFrame для выбранных городов
        selected_cities_data = []
        for city_name in selected_cities:
            city_info = hh_areas[city_name]
            selected_cities_data.append({
                'Город': city_name,
                'ID HH': city_info['id'],
                'Регион': city_info['parent']
            })

        selected_cities_df = pd.DataFrame(selected_cities_data)
        st.dataframe(selected_cities_df, use_container_width=True, hide_index=True)

        # Кнопка выгрузки выбранных городов
        col1, col2 = st.columns(2)
        with col1:
            # Для публикатора (только названия городов)
            publisher_df = pd.DataFrame({'Город': selected_cities_df['Город']})
            output_pub = io.BytesIO()
            with pd.ExcelWriter(output_pub, engine='openpyxl') as writer:
                publisher_df.to_excel(writer, index=False, header=False, sheet_name='Гео')
            output_pub.seek(0)
            st.download_button(
                label=f"📤 Для публикатора ({len(selected_cities)} городов)",
                data=output_pub,
                file_name="selected_cities_publisher.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                type="primary",
                key="download_selected_publisher"
            )
        with col2:
            # Полный отчет с ID и регионами
            output_full = io.BytesIO()
            with pd.ExcelWriter(output_full, engine='openpyxl') as writer:
                selected_cities_df.to_excel(writer, index=False, sheet_name='Города')
            output_full.seek(0)
            st.download_button(
                label=f"📥 Полный отчет ({len(selected_cities)} городов)",
                data=output_full,
                file_name="selected_cities.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                type="primary",
                key="download_selected_full"
            )

    # КНОПКА ВЫГРУЗКИ ВСЕХ ГОРОДОВ
    st.markdown("")
    if st.button("🌍 Выгрузить ВСЕ города из справочника", type="secondary", use_container_width=False, key="export_all_cities_btn"):
        with st.spinner("Формирую полный список..."):
            all_cities_df = get_all_cities(hh_areas)
            if not all_cities_df.empty:
                st.success(f"✅ Найдено **{len(all_cities_df)}** городов в справочнике HH.ru")
                st.dataframe(all_cities_df, use_container_width=True, height=400)

                col1, col2 = st.columns(2)
                with col1:
                    publisher_df = pd.DataFrame({'Город': all_cities_df['Город']})
                    output_pub = io.BytesIO()
                    with pd.ExcelWriter(output_pub, engine='openpyxl') as writer:
                        publisher_df.to_excel(writer, index=False, header=False, sheet_name='Гео')
                    output_pub.seek(0)
                    st.download_button(
                        label=f"📤 Для публикатора ({len(all_cities_df)} городов)",
                        data=output_pub,
                        file_name="all_cities_publisher.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        type="primary",
                        key="download_all_publisher"
                    )
                with col2:
                    output_full = io.BytesIO()
                    with pd.ExcelWriter(output_full, engine='openpyxl') as writer:
                        all_cities_df.to_excel(writer, index=False, sheet_name='Города')
                    output_full.seek(0)
                    st.download_button(
                        label=f"📥 Скачать полный отчет ({len(all_cities_df)} городов)",
                        data=output_full,
                        file_name="all_cities.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        type="primary",
                        key="download_all_full"
                    )

st.markdown("---")

# ============================================
# БЛОК: СИНХРОНИЗАТОР ГОРОДОВ
# ============================================
st.markdown('<div id="синхронизатор-городов"></div>', unsafe_allow_html=True)
st.header("📤 Синхронизатор городов")

with st.sidebar:
    # Логотип - используем base64 для полного обхода кэша
    try:
        import base64
        from io import BytesIO
        from PIL import Image

        # Читаем изображение
        logo_image = Image.open("min-hh-red.png")

        # Конвертируем в base64
        buffered = BytesIO()
        logo_image.save(buffered, format="PNG", optimize=False, quality=100)
        img_str = base64.b64encode(buffered.getvalue()).decode()

        # Вставляем через HTML с прямыми стилями для максимального качества
        st.markdown(
            f'''<img src="data:image/png;base64,{img_str}"
            style="width: 200px;
                   height: auto;
                   image-rendering: auto;
                   -ms-interpolation-mode: bicubic;
                   display: block;
                   margin-bottom: 10px;
                   object-fit: contain;" />''',
            unsafe_allow_html=True
        )
    except Exception as e:
        # Fallback если PNG еще не создан
        st.markdown(
            f'<div class="title-container">'
            f'<span class="rotating-earth">{SYNC_ICON}</span>'
            f'</div>',
            unsafe_allow_html=True
        )
    st.markdown("---")

    # Инициализация состояния для отображения инструкций
    if 'show_instruction' not in st.session_state:
        st.session_state.show_instruction = None

    # Словарь с инструкциями для каждого раздела
    instructions = {
        "проверка-гео": """
### Проверка гео и выгрузка базы

<p><span class="step-number">1</span> <strong>Быстрая проверка города</strong></p>

- Введите название города в поисковое поле
- Система покажет ID города в HH.ru и его регион
- Используйте для быстрой проверки наличия города в справочнике

<p><span class="step-number">2</span> <strong>Выгрузка всех городов</strong></p>

- Нажмите кнопку "Выгрузить ВСЕ города"
- Получите Excel-файл со всеми городами России из справочника HH.ru
- Файл содержит: название города, ID, регион, тип населенного пункта
        """,

        "синхронизатор-городов": """
### Синхронизатор городов

<p><span class="step-number">1</span> <strong>Простой сценарий (один столбец)</strong></p>

- Загрузите файл, где в первом столбце указаны города
- Система автоматически сопоставит города со справочником HH.ru
- Подходит для быстрой проверки списка городов

<p><span class="step-number">2</span> <strong>Сценарий со столбцом "Вакансия"</strong></p>

- Загрузите файл с колонкой "Вакансия"
- Данные будут разделены по вакансиям и обработаны отдельно
- Скачайте результат единым архивом или отдельными файлами

<p><span class="step-number">3</span> <strong>Сценарий с вкладками "вакансия"</strong></p>

- Загрузите Excel с несколькими вкладками (названия начинаются на "вакансия")
- Каждая вкладка обрабатывается как отдельная вакансия
- Идеально для структурированной работы с множеством вакансий

**Порядок работы:**
1. Загрузите файл → 2. Выберите режим → 3. Нажмите "Начать сопоставление" → 4. Проверьте и отредактируйте → 5. Скачайте
        """,

        "выбор-регионов-и-городов": """
### Выбор регионов и городов

<p><span class="step-number">1</span> <strong>Поиск по регионам</strong></p>

- Выберите регионы из списка (можно несколько)
- Получите Excel-файл со всеми городами выбранных регионов
- Файл содержит полную информацию о городах

<p><span class="step-number">2</span> <strong>Поиск по городам</strong></p>

- Выберите конкретные города (можно несколько)
- Получите информацию: ID, название, регион
- Скачайте в формате Excel

<p><span class="step-number">3</span> <strong>Поиск по населению</strong></p>

- Укажите минимальное и максимальное население
- Система найдет все города в указанном диапазоне
- Данные о населении из актуального справочника
        """,

        "объединитель-файлов": """
### Объединитель файлов

<p><span class="step-number">1</span> <strong>Загрузите файлы</strong></p>

- Поддерживаются форматы: Excel (xlsx, xls, xlsm, xlsb) и CSV
- Можно загрузить несколько файлов одновременно
- Все файлы должны иметь одинаковую структуру столбцов

<p><span class="step-number">2</span> <strong>Обработка</strong></p>

- Система автоматически объединит все файлы
- Полные дубликаты будут выделены оранжевым цветом
- Дубликаты размещаются в начале файла для удобства

<p><span class="step-number">3</span> <strong>Скачивание</strong></p>

- Нажмите кнопку "Скачать объединенный файл"
- Файл содержит статистику: общее количество, дубликаты, уникальные записи
        """,

        "сверки-с-клиентами": """
### Сверки с клиентами

<p><span class="step-number">1</span> <strong>Сверка Я.Еда</strong></p>

- Нажмите на желтую карточку "Яндекс.Еда"
- Скопируйте код установки библиотек (Блок 1)
- Запустите его в Google Colab и дождитесь завершения

<p><span class="step-number">2</span> <strong>Основной код</strong></p>

- Скопируйте основной код сверки (Блок 2)
- Вставьте в новую ячейку Google Colab
- Запустите и следуйте инструкциям на экране

<p><span class="step-number">3</span> <strong>Файлы</strong></p>

Подготовьте файлы с названиями:
- "ООО Хэдхантер Биллинг....." (отчет биллинг)
- "Отчет-по-откликам-по-проектам-работодателя-" (внутренний отчет HH)
- "Leads_" (лиды из ЛК Я.Еды)

⏱️ Время выполнения: 30-40 минут
        """
    }

    st.markdown("### 🧭 Навигация")

    # Стили для якорной навигации
    st.markdown("""
    <style>
    .nav-link {
        display: block;
        padding: 0.4rem 0.75rem;
        margin: 0.2rem 0;
        background: #f8f9fa;
        border-radius: 6px;
        border-left: 3px solid var(--ui-color);
        text-decoration: none !important;
        color: #1a1a1a !important;
        font-weight: normal;
        font-size: 0.9rem;
        transition: all 0.3s ease;
    }
    .nav-link:hover {
        background: var(--button-hover);
        color: white !important;
        transform: translateX(5px);
        border-left: 3px solid transparent;
    }
    </style>
    """, unsafe_allow_html=True)

    # Якорная навигация
    nav_items = [
        ("Проверка гео и выгрузка базы", "проверка-гео"),
        ("Синхронизатор городов", "синхронизатор-городов"),
        ("Выбор регионов и городов", "выбор-регионов-и-городов"),
        ("Объединитель файлов", "объединитель-файлов"),
        ("Сверки с клиентами", "сверки-с-клиентами")
    ]

    for name, anchor in nav_items:
        st.markdown(f'<a class="nav-link" href="#{anchor}">{name}</a>', unsafe_allow_html=True)

    st.markdown("---")

    # Инструкции в раскрывающихся блоках
    st.markdown("### 📖 Инструкции")

    with st.expander("Проверка гео и выгрузка базы"):
        st.markdown(instructions["проверка-гео"], unsafe_allow_html=True)

    with st.expander("Синхронизатор городов"):
        st.markdown(instructions["синхронизатор-городов"], unsafe_allow_html=True)

    with st.expander("Выбор регионов и городов"):
        st.markdown(instructions["выбор-регионов-и-городов"], unsafe_allow_html=True)

    with st.expander("Объединитель файлов"):
        st.markdown(instructions["объединитель-файлов"], unsafe_allow_html=True)

    with st.expander("Сверки с клиентами"):
        st.markdown(instructions["сверки-с-клиентами"], unsafe_allow_html=True)

    st.markdown("---")

# Устанавливаем порог совпадения как константу
threshold = 85

# ============================================
# ЗАГРУЗКА И ОБРАБОТКА ФАЙЛОВ
# ============================================
st.subheader("📁 Загрузка файлов")
uploaded_files = st.file_uploader(
    "Выберите один или несколько файлов с городами",
    type=['xlsx', 'csv'],
    help="Поддерживаются форматы: Excel (.xlsx) и CSV. Можно загрузить несколько файлов одновременно",
    accept_multiple_files=True,
    key="files_uploader"
)

if uploaded_files and hh_areas is not None:
    st.markdown("---")

    try:
        # Обрабатываем все загруженные файлы
        sheets_data = {}
        file_counter = 1

        for uploaded_file in uploaded_files:
            # Определяем тип файла и читаем все вкладки
            if uploaded_file.name.endswith('.csv'):
                # CSV - одна вкладка
                df = pd.read_csv(uploaded_file, header=None)
                # Если несколько файлов, добавляем префикс к имени
                sheet_key = f"Файл{file_counter}_Sheet1" if len(uploaded_files) > 1 else "Sheet1"
                sheets_data[sheet_key] = df
            else:
                # Excel - читаем все вкладки
                excel_file = pd.ExcelFile(uploaded_file)
                for sheet_name in excel_file.sheet_names:
                    df_sheet = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
                    if len(df_sheet) > 0:  # Только непустые вкладки
                        # Если несколько файлов, добавляем префикс к имени вкладки
                        sheet_key = f"Файл{file_counter}_{sheet_name}" if len(uploaded_files) > 1 else sheet_name
                        sheets_data[sheet_key] = df_sheet
            file_counter += 1
        
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
                    
                files_info = f" из **{len(uploaded_files)}** файлов" if len(uploaded_files) > 1 else ""
                st.info(f"📄 Загружено **{len(sheets_data)}** вкладок{files_info} | 🎯 **Обнаружен режим работы с вкладками**")
            else:
                st.session_state.sheet_mode = None
                files_info = f" из **{len(uploaded_files)}** файлов" if len(uploaded_files) > 1 else ""
                st.info(f"📄 Загружено **{len(sheets_data)}** вкладок{files_info}")
        else:
            # Одна вкладка - проверяем столбец "Вакансия"
            first_sheet_data = list(st.session_state.sheets_data.values())[0]
            if first_sheet_data['has_vacancy_column']:
                st.session_state.sheet_mode = 'columns'
        
        # Для обратной совместимости - сохраняем первую вкладку как основной DF
        first_sheet_name = list(sheets_data.keys())[0]
        st.session_state.original_df = st.session_state.sheets_data[first_sheet_name]['df'].copy()
        st.session_state.has_vacancy_mode = st.session_state.sheet_mode in ['columns', 'tabs', 'both']

        # Показываем превью файла с информацией о размерах
        vacancy_info = " | 🎯 **Обнаружен столбец 'Вакансия'**" if has_vacancy_column else ""
        with st.expander(f"👀 Превью ({len(df)} строк, {len(df.columns)} столбцов{vacancy_info})", expanded=False):
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
                    result_df, dup_original, dup_hh, total_dup = match_cities(df_sheet, hh_areas, threshold, sheet_name=sheet_name)  
                    
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

                # СТИЛИ ДЛЯ КНОПОК РЕЖИМА применяются через CSS (см. ниже в комментарии про стили кнопок)
                # Красная заливка применяется через специальные CSS селекторы

                # Кнопки выбора режима работы
                col1, col2 = st.columns(2)

                with col1:
                    selected_split = st.session_state.export_mode == "split"
                    # ИСПОЛЬЗУЕМ type="primary" для гарантированной красной заливки
                    if st.button(
                        "**РАЗДЕЛЕНИЕ ПО ВАКАНСИЯМ**\n\n(работа с отдельными файлами)",
                        use_container_width=True,
                        key="mode_split",
                        type="primary"
                    ):
                        st.session_state.export_mode = "split"
                        st.rerun()

                with col2:
                    selected_single = st.session_state.export_mode == "single"
                    # ИСПОЛЬЗУЕМ type="primary" для гарантированной красной заливки
                    if st.button(
                        "**ЕДИНЫМ ФАЙЛОМ**\n\n(работа с общим списком гео)",
                        use_container_width=True,
                        key="mode_single",
                        type="primary"
                    ):
                        st.session_state.export_mode = "single"
                        st.rerun()

                # Добавляем текст со стрелочкой вверх
                st.markdown('<p style="text-align: center; margin-top: 10px; color: rgba(49, 51, 63, 0.6); font-size: 0.9rem;"><span style="color: #B22222; font-size: 1.2rem;">↑</span> Выберите режим работы</p>', unsafe_allow_html=True)

                # Показываем выбранный режим (скрыто)
                # if st.session_state.export_mode == "split":
                #     st.success("🎯 **Режим разделения по вакансиям активирован**")
                # elif st.session_state.export_mode == "single":
                #     st.info("🎯 **Режим единого архива активирован**")
                
                # Если режим еще не выбран, останавливаем дальнейшую обработку
                if st.session_state.export_mode is None:
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

                # Проверяем наличие гео из других стран
                russia_id = '113'
                non_russian_cities = []
                for idx, row in result_df.iterrows():
                    geo_name = row['Итоговое гео']
                    if pd.notna(geo_name) and geo_name in hh_areas:
                        city_info = hh_areas[geo_name]
                        if city_info.get('root_parent_id', '') != russia_id:
                            non_russian_cities.append({
                                'original': row['Исходное название'],
                                'matched': geo_name,
                                'country_id': city_info.get('root_parent_id', 'Unknown')
                            })

                if non_russian_cities:
                    st.error(f"""
                    🌍 **Обнаружено {len(non_russian_cities)} гео из других стран!**

                    Эти города не из России и не должны попадать в выгрузку.
                    Пожалуйста, проверьте и исправьте совпадения ниже в блоке редактирования.
                    """)

                    # Показываем список
                    with st.expander("🔍 Показать гео из других стран"):
                        for city in non_russian_cities:
                            st.text(f"• {city['original']} → {city['matched']}")

            # РАННЯЯ ОСТАНОВКА ДЛЯ РЕЖИМА SPLIT
            # Если режим split - пропускаем все стандартные блоки и сразу переходим к вакансиям
            if st.session_state.get('has_vacancy_mode', False) and st.session_state.export_mode == "split":
                # Переход к блоку "ПРОВЕРЯЕМ РЕЖИМ РАБОТЫ" ниже
                pass
            else:
                # Для остальных режимов показываем стандартные блоки

                    st.markdown("---")
                    st.subheader("📋 Таблица сопоставлений")

                    # Поле поиска и фильтры в двух колонках
                    col_search, col_status = st.columns([2, 1])

                    with col_search:
                        st.text_input(
                            "🔍 Поиск по таблице",
                            key="search_query",
                            placeholder="Начните вводить название города...",
                            label_visibility="visible"
                        )

                    with col_status:
                        # Определяем доступные статусы
                        available_statuses = result_df['Статус'].unique().tolist()
                        status_filter = st.multiselect(
                            "📊 Фильтр по статусам",
                            options=available_statuses,
                            default=[],
                            key="status_filter",
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

                    # Применяем фильтр по статусам
                    if status_filter:
                        result_df_sorted = result_df_sorted[result_df_sorted['Статус'].isin(status_filter)]

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
                            st.info(f"Найдено совпадений: **{len(result_df_filtered)}** из {len(result_df)}")
                    else:
                        result_df_filtered = result_df_sorted  
              
                    display_df = result_df_filtered.copy()
                    display_df = display_df.drop(['row_id', 'sort_priority'], axis=1, errors='ignore')

                    # Сбрасываем индекс чтобы избежать дублирования
                    display_df = display_df.reset_index(drop=True)

                    st.dataframe(display_df, use_container_width=True, height=400, hide_index=True)  
              
                    # ИЗМЕНЕНО: Исключаем дубликаты из редактирования
                    editable_rows = result_df_sorted[
                        (result_df_sorted['Совпадение %'] <= 90) &
                        (~result_df_sorted['Статус'].str.contains('Дубликат', na=False))
                    ].copy()

                    # Сортируем: сначала "Нет совпадения", затем по возрастанию процента
                    if len(editable_rows) > 0:
                        # Создаем приоритет: 0 для "Нет совпадения", 1 для остальных
                        editable_rows['_sort_priority'] = editable_rows['Статус'].apply(
                            lambda x: 0 if '❌ Не найдено' in str(x) else 1
                        )
                        editable_rows = editable_rows.sort_values(
                            ['_sort_priority', 'Совпадение %'],
                            ascending=[True, True]
                        )
                        editable_rows = editable_rows.drop(columns=['_sort_priority'])  
              
                    if len(editable_rows) > 0:
                        st.markdown("---")
                        st.subheader("✏️ Редактирование городов с совпадением ≤ 90%")
                        st.info(f"Найдено **{len(editable_rows)}** городов, доступных для редактирования")

                        # Обертка для черной окантовки
                        st.markdown('<div class="edit-cities-block">', unsafe_allow_html=True)

                        for idx, row in editable_rows.iterrows():
                            with st.container():
                                row_id = row['row_id']
                                candidates = st.session_state.candidates_cache.get(row_id, [])

                                # Если кандидатов нет в кэше, получаем их заново
                                if not candidates:
                                    city_name = row['Исходное название']
                                    # Используем только российские города
                                    candidates = get_candidates_by_word(city_name, get_russian_cities(hh_areas), limit=20)

                                current_value = row['Итоговое гео']
                                current_match = row['Совпадение %']

                                # Добавляем текущее значение в список, если его нет
                                if current_value and current_value != row['Исходное название']:
                                    candidate_names = [c[0] for c in candidates]
                                    if current_value not in candidate_names:
                                        candidates.append((current_value, current_match))

                                # Сортируем кандидатов по убыванию процента совпадения
                                candidates.sort(key=lambda x: x[1], reverse=True)

                                # Формируем список опций с процентами
                                if candidates:
                                    options = ["❌ Нет совпадения"] + [f"{c[0]} ({c[1]:.1f}%)" for c in candidates[:20]]
                                else:
                                    options = ["❌ Нет совпадения"]

                                # Определяем выбранный элемент
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

                                # Определяем цвет окантовки
                                if default_idx == 0:
                                    border_color = "#ea3324"  # Красная для "Нет совпадения"
                                else:
                                    border_color = "#ea3324"  # Оранжевая для городов с процентом

                                col1, col2, col3, col4 = st.columns([2, 3, 1, 1])

                                with col1:
                                    st.markdown(f"**{row['Исходное название']}**")

                                with col2:
                                    selected = st.selectbox(
                                        "Выберите город:",
                                        options=options,
                                        index=default_idx,
                                        key=f"select_{row_id}",
                                        label_visibility="collapsed"
                                    )

                                    # Inject CSS для этого конкретного selectbox
                                    st.markdown(f"""
                                    <style>
                                    div[data-testid="stSelectbox"]:has(select[id*="select_{row_id}"]) > div > div,
                                    div[data-testid="stSelectbox"]:has(select[id*="select_{row_id}"]) > div > div > div,
                                    div[data-testid="stSelectbox"]:has(select[id*="select_{row_id}"]) [data-baseweb="select"] > div {{
                                        border-color: {border_color} ;
                                        border: 2px solid {border_color} ;
                                    }}
                                    </style>
                                    """, unsafe_allow_html=True)

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

                        # Закрываем обертку для черной окантовки
                        st.markdown('</div>', unsafe_allow_html=True)

                        # ============================================
                        # БЛОК: ДОБАВЛЕНИЕ ЛЮБОГО ГОРОДА (только для НЕ split режима)
                        # ============================================
                        st.markdown("---")
                        st.subheader("➕ Добавить дополнительные города")
                
                        # Селектор на половину ширины экрана
                        col_selector = st.columns([1, 1])
                        with col_selector[0]:
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

                        # Кнопки под селектором
                        col_btn1, col_btn2 = st.columns(2)
                        with col_btn1:
                            if st.button("➕ Добавить", use_container_width=True, type="primary"):
                                if selected_city and selected_city not in st.session_state.added_cities:
                                    st.session_state.added_cities.append(selected_city)
                                    st.success(f"✅ {selected_city}")
                                elif selected_city in st.session_state.added_cities:
                                    st.warning(f"⚠️ Уже добавлен")

                        with col_btn2:
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

                            # Блок редактирования городов с совпадением ≤ 90%
                            editable_rows = result_df_sheet[
                                (result_df_sheet['Совпадение %'] <= 90) & 
                                (~result_df_sheet['Статус'].str.contains('Дубликат', na=False))
                            ].copy()
                            
                            if len(editable_rows) > 0:
                                # Убираем дубликаты по исходному названию
                                editable_rows['_normalized_original'] = editable_rows['Исходное название'].apply(normalize_city_name)
                                editable_rows = editable_rows.drop_duplicates(subset=['_normalized_original'], keep='first')

                                # Сортируем: сначала "Нет совпадения", затем по возрастанию процента
                                editable_rows['_sort_priority'] = editable_rows['Статус'].apply(
                                    lambda x: 0 if '❌ Не найдено' in str(x) else 1
                                )
                                editable_rows = editable_rows.sort_values(
                                    ['_sort_priority', 'Совпадение %'],
                                    ascending=[True, True]
                                )
                                editable_rows = editable_rows.drop(columns=['_sort_priority'])

                                st.markdown("#### ✏️ Редактирование городов с совпадением ≤ 90%")
                                st.warning(f"⚠️ Найдено **{len(editable_rows)}** городов для проверки")

                                # Обертка для черной окантовки
                                st.markdown('<div class="edit-cities-block">', unsafe_allow_html=True)

                                # Для каждого города показываем выбор
                                for idx, row in editable_rows.iterrows():
                                    row_id = row['row_id']
                                    city_name = row['Исходное название']

                                    # Используем кэш кандидатов из smart_match_city
                                    cache_key = (sheet_name, row_id)
                                    candidates = st.session_state.candidates_cache.get(cache_key, [])

                                    # Если кэша нет, ищем заново (для обратной совместимости)
                                    if not candidates:
                                        # Используем только российские города
                                        candidates = get_candidates_by_word(city_name, get_russian_cities(hh_areas), limit=20)

                                    current_value = row['Итоговое гео']
                                    current_match = row['Совпадение %']

                                    # Если есть текущее значение - добавляем в список
                                    if current_value and current_value != city_name:
                                        candidate_names = [c[0] for c in candidates]
                                        if current_value not in candidate_names:
                                            candidates.append((current_value, current_match))

                                    # Сортируем кандидатов по убыванию процента совпадения
                                    candidates.sort(key=lambda x: x[1], reverse=True)

                                    # Формируем опции
                                    if candidates:
                                        options = ["❌ Нет совпадения"] + [f"{c[0]} ({c[1]:.1f}%)" for c in candidates[:20]]
                                    else:
                                        options = ["❌ Нет совпадения"]

                                    # Определяем текущий выбор
                                    unique_key = f"select_{sheet_name}_{row_id}_{tab_idx}"
                                    selection_key = (sheet_name, row_id)

                                    if selection_key in st.session_state.manual_selections:
                                        selected_value = st.session_state.manual_selections[selection_key]
                                        default_idx = 0
                                        for i, opt in enumerate(options):
                                            if selected_value in opt or opt.startswith(selected_value):
                                                default_idx = i
                                                break
                                    else:
                                        default_idx = 0
                                        if current_value:
                                            for i, opt in enumerate(options):
                                                if opt.startswith(current_value) or current_value in opt:
                                                    default_idx = i
                                                    break

                                    # Определяем цвет окантовки
                                    if default_idx == 0:
                                        border_color = "#ea3324"  # Красная для "Нет совпадения"
                                    else:
                                        border_color = "#ea3324"  # Оранжевая для городов с процентом

                                    col1, col2, col3 = st.columns([2, 3, 1])

                                    with col1:
                                        st.text(city_name)

                                    with col2:
                                        selected = st.selectbox(
                                            "Выберите город:",
                                            options=options,
                                            index=default_idx,
                                            key=unique_key,
                                            label_visibility="collapsed"
                                        )

                                        # Inject CSS для этого конкретного selectbox
                                        st.markdown(f"""
                                        <style>
                                        div[data-testid="stSelectbox"]:has(select[id*="{unique_key}"]) > div > div,
                                        div[data-testid="stSelectbox"]:has(select[id*="{unique_key}"]) > div > div > div,
                                        div[data-testid="stSelectbox"]:has(select[id*="{unique_key}"]) [data-baseweb="select"] > div {{
                                            border-color: {border_color} ;
                                            border: 2px solid {border_color} ;
                                        }}
                                        </style>
                                        """, unsafe_allow_html=True)

                                        if selected == "❌ Нет совпадения":
                                            st.session_state.manual_selections[selection_key] = "❌ Нет совпадения"
                                        else:
                                            # Извлекаем название без процента
                                            city_match = selected.rsplit(' (', 1)[0]
                                            st.session_state.manual_selections[selection_key] = city_match

                                    with col3:
                                        st.text(f"{row['Совпадение %']:.1f}%")

                                st.markdown("---")

                                # Закрываем обертку для черной окантовки
                                st.markdown('</div>', unsafe_allow_html=True)

                            # Применяем ручные изменения
                            result_df_sheet_final = result_df_sheet.copy()
                            for selection_key, new_value in st.session_state.manual_selections.items():
                                # selection_key это кортеж (sheet_name, row_id) или просто row_id для старых данных
                                if isinstance(selection_key, tuple):
                                    key_sheet_name, row_id = selection_key
                                    # Применяем только для текущей вкладки
                                    if key_sheet_name != sheet_name:
                                        continue
                                else:
                                    # Для обратной совместимости - применяем как раньше
                                    row_id = selection_key

                                if row_id in result_df_sheet_final['row_id'].values:
                                    mask = result_df_sheet_final['row_id'] == row_id

                                    if new_value == "❌ Нет совпадения":
                                        result_df_sheet_final.loc[mask, 'Итоговое гео'] = None
                                    else:
                                        result_df_sheet_final.loc[mask, 'Итоговое гео'] = new_value
                                        if new_value in hh_areas:
                                            result_df_sheet_final.loc[mask, 'ID HH'] = hh_areas[new_value]['id']
                                            result_df_sheet_final.loc[mask, 'Регион'] = hh_areas[new_value]['parent']
                            
                            # Формируем итоговый файл для этой вкладки
                            output_sheet_df = result_df_sheet_final[
                                (result_df_sheet_final['Итоговое гео'].notna()) &
                                (~result_df_sheet_final['Статус'].str.contains('Не найдено', na=False)) &
                                (~result_df_sheet_final['Статус'].str.contains('Пустое значение', na=False))
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

                                # Удаляем первую строку, если она является заголовком
                                final_output = remove_header_row_if_needed(final_output, original_cols[0])

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

                        # Инициализируем состояние для редактирования вакансий
                        if 'vacancy_edits' not in st.session_state:
                            st.session_state.vacancy_edits = {}
                        
                        # Создаем вкладки для каждой вакансии
                        tabs = st.tabs([f"{v}" for v in unique_vacancies])
                        
                        for tab_idx, (tab, vacancy) in enumerate(zip(tabs, unique_vacancies)):
                            with tab:
                                # Фильтруем данные по вакансии
                                vacancy_df = export_df[export_df[vacancy_col] == vacancy].copy()

                                # Показываем таблицу с возможностью редактирования
                                st.markdown("#### Города для редактирования (совпадение ≤ 90%)")
                                
                                editable_vacancy_rows = vacancy_df[vacancy_df['Совпадение %'] <= 90].copy()
                                
                                # Убираем дубликаты по исходному названию для редактирования
                                if len(editable_vacancy_rows) > 0:
                                    editable_vacancy_rows['_normalized_original'] = editable_vacancy_rows['Исходное название'].apply(normalize_city_name)
                                    editable_vacancy_rows = editable_vacancy_rows.drop_duplicates(subset=['_normalized_original'], keep='first')

                                    # Сортируем: сначала "Нет совпадения", затем по возрастанию процента
                                    editable_vacancy_rows['_sort_priority'] = editable_vacancy_rows['Статус'].apply(
                                        lambda x: 0 if '❌ Не найдено' in str(x) else 1
                                    )
                                    editable_vacancy_rows = editable_vacancy_rows.sort_values(
                                        ['_sort_priority', 'Совпадение %'],
                                        ascending=[True, True]
                                    )
                                    editable_vacancy_rows = editable_vacancy_rows.drop(columns=['_sort_priority'])

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

                                            # Используем кэш кандидатов из smart_match_city
                                            candidates = st.session_state.candidates_cache.get(row_id, [])

                                            # Если кэша нет, ищем заново (для обратной совместимости)
                                            if not candidates:
                                                # Используем только российские города
                                                candidates = get_candidates_by_word(city_name, get_russian_cities(hh_areas), limit=20)
                                            
                                            # Если есть текущее значение из сопоставления - добавляем его в список
                                            if current_value and current_value != city_name:
                                                # Проверяем, есть ли уже это значение в кандидатах
                                                candidate_names = [c[0] for c in candidates]
                                                if current_value not in candidate_names:
                                                    # Добавляем текущее значение в список
                                                    candidates.append((current_value, current_match))

                                            # Сортируем кандидатов по убыванию процента совпадения
                                            candidates.sort(key=lambda x: x[1], reverse=True)

                                            # Формируем опции - всегда показываем топ кандидатов
                                            if candidates:
                                                options = ["❌ Нет совпадения"] + [f"{c[0]} ({c[1]:.1f}%)" for c in candidates[:20]]
                                            else:
                                                # Если совсем нет кандидатов - показываем хотя бы "Нет совпадения"
                                                options = ["❌ Нет совпадения"]
                                            
                                            # Уникальный ключ для каждой вакансии
                                            unique_key = f"select_{vacancy}_{row_id}_{tab_idx}"
                                            selection_key = (vacancy, row_id)

                                            if selection_key in st.session_state.manual_selections:
                                                selected_value = st.session_state.manual_selections[selection_key]
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
                                                st.session_state.manual_selections[selection_key] = "❌ Нет совпадения"
                                            else:
                                                if "(" in selected and selected.startswith("❌") == False:
                                                    selected_city = selected.rsplit(' (', 1)[0]
                                                    st.session_state.manual_selections[selection_key] = selected_city
                                                else:
                                                    st.session_state.manual_selections[selection_key] = selected
                                        
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
                                
                                # Селектор на половину ширины экрана
                                col_add_selector = st.columns([1, 1])
                                with col_add_selector[0]:
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

                                # Кнопки под селектором
                                col_add_btn1, col_add_btn2 = st.columns(2)
                                with col_add_btn1:
                                    if st.button("➕ Добавить", use_container_width=True, type="secondary", key=f"add_btn_{vacancy}_{tab_idx}"):
                                        if selected_add_city and selected_add_city not in st.session_state[vacancy_key]:
                                            st.session_state[vacancy_key].append(selected_add_city)
                                            st.success(f"✅ {selected_add_city}")
                                        elif selected_add_city in st.session_state[vacancy_key]:
                                            st.warning(f"⚠️ Уже добавлен")

                                with col_add_btn2:
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
                                for selection_key, new_value in st.session_state.manual_selections.items():
                                    # selection_key это кортеж (vacancy, row_id) или просто row_id для старых данных
                                    if isinstance(selection_key, tuple):
                                        key_vacancy, row_id = selection_key
                                        # Применяем только для текущей вакансии
                                        if key_vacancy != vacancy:
                                            continue
                                    else:
                                        # Для обратной совместимости - применяем как раньше
                                        row_id = selection_key

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

                                # Удаляем первую строку, если она является заголовком
                                output_vacancy_df = remove_header_row_if_needed(output_vacancy_df, original_cols[0])

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
                # РЕЖИМ: Единым файлом
                st.markdown("---")
                st.subheader("💾 Скачать результаты")
                
                if st.session_state.sheet_mode == 'tabs':
                    # Режим вкладок - объединяем все вкладки в один файл
                    all_data = []
                    
                    for sheet_name, sheet_result in st.session_state.sheets_results.items():
                        result_df_sheet = sheet_result['result_df']
                        original_df_sheet = st.session_state.sheets_data[sheet_name]['df']
                        
                        # Применяем изменения
                        for selection_key, new_value in st.session_state.manual_selections.items():
                            # selection_key это кортеж (sheet_name, row_id) или просто row_id для старых данных
                            if isinstance(selection_key, tuple):
                                key_sheet_name, row_id = selection_key
                                # Применяем только для текущей вкладки
                                if key_sheet_name != sheet_name:
                                    continue
                            else:
                                # Для обратной совместимости - применяем как раньше
                                row_id = selection_key

                            if row_id in result_df_sheet['row_id'].values:
                                mask = result_df_sheet['row_id'] == row_id

                                if new_value == "❌ Нет совпадения":
                                    result_df_sheet.loc[mask, 'Итоговое гео'] = None
                                else:
                                    result_df_sheet.loc[mask, 'Итоговое гео'] = new_value
                                    if new_value in hh_areas:
                                        result_df_sheet.loc[mask, 'ID HH'] = hh_areas[new_value]['id']
                                        result_df_sheet.loc[mask, 'Регион'] = hh_areas[new_value]['parent']
                        
                        # Формируем данные для этой вкладки
                        output_sheet = result_df_sheet[
                            (result_df_sheet['Итоговое гео'].notna()) &
                            (~result_df_sheet['Статус'].str.contains('Не найдено', na=False)) &
                            (~result_df_sheet['Статус'].str.contains('Пустое значение', na=False))
                        ].copy()

                        if len(output_sheet) > 0:
                            # Получаем индексы из row_id
                            indices = output_sheet['row_id'].values

                            # Формируем данные: первая колонка - Итоговое гео, остальные из оригинала
                            sheet_data = pd.DataFrame()

                            # Первая колонка - Итоговое гео
                            first_col_name = original_df_sheet.columns[0]
                            sheet_data[first_col_name] = output_sheet['Итоговое гео'].values

                            # Остальные колонки из оригинального датафрейма (используем loc для правильного доступа по индексам)
                            for col in original_df_sheet.columns[1:]:
                                sheet_data[col] = original_df_sheet.loc[indices, col].values

                            all_data.append(sheet_data)
                    
                    # Объединяем все вкладки
                    if all_data:
                        output_df = pd.concat(all_data, ignore_index=True)
                        
                        # Удаляем дубликаты
                        output_df['_normalized'] = output_df.iloc[:, 0].apply(normalize_city_name)
                        output_df = output_df.drop_duplicates(subset=['_normalized'], keep='first')
                        output_df = output_df.drop(columns=['_normalized'])

                        # Удаляем первую строку, если она является заголовком
                        first_col_name = output_df.columns[0]
                        output_df = remove_header_row_if_needed(output_df, first_col_name)

                        st.success(f"✅ Готово к выгрузке: **{len(output_df)}** уникальных городов")
                        
                        # Кнопка скачивания
                        output_all = io.BytesIO()
                        with pd.ExcelWriter(output_all, engine='openpyxl') as writer:
                            output_df.to_excel(writer, index=False, header=True, sheet_name='Результат')
                        output_all.seek(0)
                        
                        st.download_button(
                            label=f"📥 Скачать файл ({len(output_df)} городов)",
                            data=output_all,
                            file_name=f"all_sheets_combined_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                            type="primary"
                        )
                        
                        # Превью
                        st.markdown("---")
                        st.markdown("#### 👀 Превью итогового файла")
                        st.dataframe(output_df, use_container_width=True, height=400)
                    else:
                        st.warning("⚠️ Нет данных для выгрузки")
                
                else:
                    # Режим столбца вакансий - оригинальная логика
                
                    # Создаем копию result_df для применения изменений
                    final_result_df = result_df.copy()
                    
                    # Применяем ручные изменения к final_result_df
                    if st.session_state.manual_selections:
                        for row_id, new_value in st.session_state.manual_selections.items():
                            # row_id может быть кортежем (sheet_name, row_id) или просто значением
                            if isinstance(row_id, tuple):
                                # Для режима вакансий с листами - пропускаем, т.к. это другой блок
                                continue

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

                    # Удаляем первую строку, если она является заголовком
                    output_df = remove_header_row_if_needed(output_df, original_cols[0])

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

                    # Удаляем первую строку, если она является заголовком (применяется до добавления городов)
                    publisher_df = remove_header_row_if_needed(publisher_df, original_cols[0])

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
                    
                    st.markdown('<p style="font-size: 0.875rem; color: rgba(49, 51, 63, 0.6);"><span class="check-circle">✓</span>Остальные столбцы из исходного файла</p>', unsafe_allow_html=True)
                    st.markdown('<p style="font-size: 0.875rem; color: rgba(49, 51, 63, 0.6);"><span class="check-circle">✓</span>Исключены не найденные и дубликаты</p>', unsafe_allow_html=True)
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
                        type="primary",
                        key='download_full'
                    )
                    
                    st.caption("📊 Подробный отчет со всеми данными")
                    st.caption("📊 Включает статусы и проценты совпадений")
      
    except Exception as e:
        st.error(f"❌ Ошибка обработки файла: {str(e)}")
        import traceback
        st.code(traceback.format_exc())

# ============================================
# БЛОК: ВЫБОР РЕГИОНОВ И ГОРОДОВ
# ============================================
st.markdown('<div id="выбор-регионов-и-городов"></div>', unsafe_allow_html=True)
st.header("🗺️ Выбор регионов и городов")

if hh_areas is not None:
    # Получаем полный список городов для фильтров
    all_cities_full = get_all_cities(hh_areas)

    # ФИЛЬТРЫ В ОДНОМ БЛОКЕ
    st.markdown("### 🔍 Фильтры")
    col_filter1, col_filter2, col_filter3, col_filter4 = st.columns(4)

    with col_filter1:
        # Форматируем федеральные округа с количеством регионов
        districts_formatted = []
        districts_mapping = {}
        for district, regions in FEDERAL_DISTRICTS.items():
            formatted = f"{district} ({len(regions)} рег.)"
            districts_formatted.append(formatted)
            districts_mapping[formatted] = district

        selected_districts_formatted = st.multiselect(
            "Федеральные округа:",
            options=districts_formatted,
            help="Можно выбрать несколько",
            key="districts_select"
        )

        # Получаем оригинальные названия округов
        selected_districts = [districts_mapping[d] for d in selected_districts_formatted]

    # Формируем список доступных регионов на основе выбранных округов
    available_regions = []
    if selected_districts:
        for district in selected_districts:
            available_regions.extend(FEDERAL_DISTRICTS[district])
    else:
        for regions in FEDERAL_DISTRICTS.values():
            available_regions.extend(regions)

    with col_filter2:
        # Форматируем регионы с указанием федерального округа
        regions_formatted = []
        regions_mapping = {}
        for region in sorted(available_regions):
            # Находим федеральный округ для региона
            fed_district = get_federal_district_by_region(region)
            if fed_district != "Не определен":
                # Сокращаем название округа для компактности
                district_short = fed_district.replace("Федеральный округ", "ФО").replace("федеральный округ", "ФО")
                formatted = f"{region} ({district_short})"
            else:
                formatted = region
            regions_formatted.append(formatted)
            regions_mapping[formatted] = region

        selected_regions_formatted = st.multiselect(
            "Области/Регионы:",
            options=regions_formatted,
            help="Можно выбрать несколько",
            key="regions_select"
        )

        # Получаем оригинальные названия регионов
        selected_regions = [regions_mapping[r] for r in selected_regions_formatted]

    with col_filter3:
        # Фильтр по часовому поясу (мультиселект)
        if not all_cities_full.empty:
            unique_timezones = sorted([tz for tz in all_cities_full['UTC'].unique() if tz and str(tz) != 'nan'])

            # Создаем форматированные опции с разницей от МСК
            timezone_options_formatted = []
            timezone_mapping = {}  # Маппинг отформатированных значений на оригинальные UTC

            for tz in unique_timezones:
                try:
                    # Парсим UTC offset для вычисления разницы с Москвой
                    sign = 1 if tz[0] == '+' else -1
                    hours = int(tz[1:3])
                    tz_hours = sign * hours
                    diff_msk = tz_hours - 3  # Москва = UTC+3

                    if diff_msk == 0:
                        formatted = f"{tz} (МСК)"
                    elif diff_msk > 0:
                        formatted = f"{tz} (+{diff_msk}ч от МСК)"
                    else:
                        formatted = f"{tz} ({diff_msk}ч от МСК)"

                    timezone_options_formatted.append(formatted)
                    timezone_mapping[formatted] = tz
                except:
                    # Если не удалось распарсить, добавляем как есть
                    timezone_options_formatted.append(tz)
                    timezone_mapping[tz] = tz

            selected_timezones_formatted = st.multiselect(
                "Часовой пояс (UTC):",
                options=timezone_options_formatted,
                help="Можно выбрать несколько",
                key="timezone_filter"
            )

            # Получаем оригинальные значения UTC
            selected_timezones = [timezone_mapping.get(tz_fmt, tz_fmt) for tz_fmt in selected_timezones_formatted]
        else:
            selected_timezones = []

    with col_filter4:
        # Выбор городов (множественный выбор)
        if not all_cities_full.empty:
            city_options = sorted(all_cities_full['Город'].unique())
            selected_cities = st.multiselect(
                "Выбрать город:",
                options=city_options,
                help="Можно выбрать несколько",
                key="cities_multiselect"
            )
        else:
            selected_cities = []

    # ВТОРАЯ СТРОКА ФИЛЬТРОВ - Население
    st.markdown("---")
    col_filter_pop1, col_filter_pop2 = st.columns([1, 3])

    with col_filter_pop1:
        # Фильтр по населению (multiselect)
        if not all_cities_full.empty and 'Население' in all_cities_full.columns:
            # Определяем диапазоны населения
            population_ranges = {
                "До 10,000 человек": (1, 10_000),
                "10,000 - 100,000 человек": (10_000, 100_000),
                "100,000 - 500,000 человек": (100_000, 500_000),
                "500,000 - 1,000,000 человек": (500_000, 1_000_000),
                "Более 1,000,000 человек": (1_000_000, float('inf'))
            }

            selected_population_ranges = st.multiselect(
                "Население (жители):",
                options=list(population_ranges.keys()),
                help="Можно выбрать несколько диапазонов",
                key="population_filter"
            )
        else:
            selected_population_ranges = []
            population_ranges = {}

    # Определяем, какие регионы использовать для поиска
    regions_to_search = []
    if selected_regions:
        regions_to_search = selected_regions
    elif selected_districts:
        for district in selected_districts:
            regions_to_search.extend(FEDERAL_DISTRICTS[district])

    # Очищаем превью если все фильтры сняты
    if not regions_to_search and not selected_cities and not selected_timezones and not selected_population_ranges:
        if 'regions_cities_df' in st.session_state:
            del st.session_state.regions_cities_df

    # Функция для фильтрации по населению
    def filter_by_population(df, selected_ranges, ranges_dict):
        """Фильтрует DataFrame по выбранным диапазонам населения"""
        if not selected_ranges or df.empty or 'Население' not in df.columns:
            return df

        # Создаем маску для фильтрации
        mask = pd.Series([False] * len(df), index=df.index)
        for range_name in selected_ranges:
            min_pop, max_pop = ranges_dict[range_name]
            mask |= (df['Население'] >= min_pop) & (df['Население'] < max_pop)

        return df[mask]

    # КНОПКИ ДЕЙСТВИЙ
    col_btn1, col_btn2, col_btn3 = st.columns(3)

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
                    # Очищаем старые результаты
                    if 'city_df' in st.session_state:
                        del st.session_state.city_df
                    if 'timezones_df' in st.session_state:
                        del st.session_state.timezones_df
                    # Получаем список городов по регионам
                    result_df = get_cities_by_regions(hh_areas, regions_to_search)
                    # Применяем фильтр по населению
                    result_df = filter_by_population(result_df, selected_population_ranges, population_ranges)
                    # Сохраняем новый результат
                    st.session_state.regions_cities_df = result_df

    with col_btn2:
        # Кнопка для выбранных городов
        if selected_cities:
            # Информация о выборе
            if len(selected_cities) == 1:
                st.info(f"📍 Выбран город: **{selected_cities[0]}**")
            else:
                st.info(f"📍 Выбрано городов: **{len(selected_cities)}**")

            if st.button(f"🔍 Получить информацию о {'городе' if len(selected_cities) == 1 else 'городах'}", type="primary", use_container_width=True):
                with st.spinner(f"Получаю информацию о {len(selected_cities)} {'городе' if len(selected_cities) == 1 else 'городах'}..."):
                    # Очищаем старые результаты
                    if 'city_df' in st.session_state:
                        del st.session_state.city_df
                    if 'timezones_df' in st.session_state:
                        del st.session_state.timezones_df
                    # Фильтруем данные по выбранным городам
                    city_df = all_cities_full[all_cities_full['Город'].isin(selected_cities)].copy()
                    # Применяем фильтр по населению
                    city_df = filter_by_population(city_df, selected_population_ranges, population_ranges)
                    # Сохраняем в общий результат
                    if not city_df.empty:
                        st.session_state.regions_cities_df = city_df
        # Кнопка для фильтра по населению (если выбрано только население)
        elif selected_population_ranges and not regions_to_search and not selected_timezones:
            # Информация о выборе
            if len(selected_population_ranges) == 1:
                st.info(f"👥 Выбран диапазон: **{selected_population_ranges[0]}**")
            else:
                st.info(f"👥 Выбрано диапазонов: **{len(selected_population_ranges)}**")

            if st.button("🔍 Получить список городов по населению", type="primary", use_container_width=True):
                with st.spinner("Фильтрую по населению..."):
                    # Очищаем старые результаты
                    if 'city_df' in st.session_state:
                        del st.session_state.city_df
                    if 'timezones_df' in st.session_state:
                        del st.session_state.timezones_df
                    # Берем все города и фильтруем по населению
                    result_df = all_cities_full.copy()
                    result_df = filter_by_population(result_df, selected_population_ranges, population_ranges)
                    # Сохраняем результат
                    if not result_df.empty:
                        st.session_state.regions_cities_df = result_df

    with col_btn3:
        # Кнопка для выгрузки по часовым поясам
        if selected_timezones:
            # Информация о выборе
            if len(selected_timezones) == 1:
                st.info(f"🕐 Выбран часовой пояс: **{selected_timezones[0]}**")
            else:
                st.info(f"🕐 Выбрано часовых поясов: **{len(selected_timezones)}**")

            # Формируем текст для кнопки
            if len(selected_timezones) == 1:
                button_text = f"🔍 Получить список городов по UTC"
            else:
                button_text = f"🔍 Получить список городов"

            if st.button(button_text, type="primary", use_container_width=True):
                with st.spinner(f"Фильтрую по выбранным часовым поясам..."):
                    # Очищаем старые результаты
                    if 'city_df' in st.session_state:
                        del st.session_state.city_df
                    if 'timezones_df' in st.session_state:
                        del st.session_state.timezones_df
                    # Фильтруем города по выбранным часовым поясам
                    filtered_df = all_cities_full[all_cities_full['UTC'].isin(selected_timezones)].copy()
                    # Применяем фильтр по населению
                    filtered_df = filter_by_population(filtered_df, selected_population_ranges, population_ranges)
                    # Сохраняем в общий результат
                    if not filtered_df.empty:
                        st.session_state.regions_cities_df = filtered_df

    # ОТОБРАЖЕНИЕ РЕЗУЛЬТАТОВ (ТАБЛИЦА ПРЕВЬЮ НА ПОЛНУЮ ШИРИНУ)
    # Единый блок для отображения результатов всех фильтров
    if 'regions_cities_df' in st.session_state and not st.session_state.regions_cities_df.empty:
        cities_df = st.session_state.regions_cities_df

        # Определяем количество найденных городов
        city_count = len(cities_df)

        # Универсальное сообщение
        if city_count == 1:
            st.success(f"✅ Найден **{city_count}** город")
        else:
            st.success(f"✅ Найдено **{city_count}** городов")

        # Показываем таблицу на полную ширину
        # Сортируем по населению по убыванию
        display_cities_df = cities_df.copy()
        if 'Население' in display_cities_df.columns:
            display_cities_df = display_cities_df.sort_values('Население', ascending=False)
        display_cities_df = display_cities_df.reset_index(drop=True)

        st.dataframe(display_cities_df, use_container_width=True, height=400, hide_index=True)

        # Кнопки для скачивания
        col1, col2 = st.columns(2)

        with col1:
            # Полный отчет
            output_full = io.BytesIO()
            with pd.ExcelWriter(output_full, engine='openpyxl') as writer:
                cities_df.to_excel(writer, index=False, sheet_name='Города')
            output_full.seek(0)

            st.download_button(
                label=f"📥 Скачать полный отчет ({city_count} городов)" if city_count > 1 else f"📥 Скачать полный отчет ({city_count} город)",
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
                label=f"📤 Для публикатора ({city_count} городов)" if city_count > 1 else f"📤 Для публикатора ({city_count} город)",
                data=output_publisher,
                file_name="cities_for_publisher.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="download_regions_publisher"
            )

# ============================================
# БЛОК: ОБЪЕДИНИТЕЛЬ ФАЙЛОВ
# ============================================
st.markdown('<div id="объединитель-файлов"></div>', unsafe_allow_html=True)
st.header("🔗 Объединитель файлов")

st.markdown("""
Загрузите несколько файлов с одинаковыми столбцами. Инструмент объединит их в один файл.
Полные дубликаты будут выделены оранжевым цветом и размещены вначале.
""")

uploaded_files = st.file_uploader(
    "Загрузите файлы для объединения",
    type=['xlsx', 'xls', 'xlsm', 'xlsb', 'csv'],
    accept_multiple_files=True,
    key="file_merger_uploader",
    help="Можно загрузить несколько файлов Excel (xlsx, xls, xlsm, xlsb) или CSV с одинаковыми столбцами"
)

if uploaded_files:
    try:
        with st.spinner("Обрабатываем файлы..."):
            # Читаем все файлы
            all_dataframes = []
            for uploaded_file in uploaded_files:
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                all_dataframes.append(df)
                st.success(f"✅ Загружен: {uploaded_file.name} ({len(df)} строк)")

            # Объединяем все файлы
            merged_df = pd.concat(all_dataframes, ignore_index=True)

            # Находим полные дубликаты
            duplicates_mask = merged_df.duplicated(keep=False)
            duplicates = merged_df[duplicates_mask].copy()
            non_duplicates = merged_df[~duplicates_mask].copy()

            # Создаем итоговый DataFrame: сначала дубликаты, затем остальные
            final_df = pd.concat([duplicates, non_duplicates], ignore_index=True)

            # Статистика
            total_rows = len(merged_df)
            duplicate_rows = len(duplicates)
            unique_rows = len(non_duplicates)

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Всего строк", total_rows)
            with col2:
                st.metric("Дубликаты", duplicate_rows)
            with col3:
                st.metric("Уникальные", unique_rows)

            if duplicate_rows > 0:
                st.warning(f"⚠️ Найдено {duplicate_rows} дубликатов. Они будут выделены оранжевым цветом в скачанном файле и размещены вначале.")
            else:
                st.success("✅ Дубликаты не найдены!")

            st.markdown("### 👀 Превью объединенного файла")
            st.info(f"ℹ️ Первые {duplicate_rows} строк - дубликаты (будут выделены оранжевым в Excel)")
            st.dataframe(final_df, use_container_width=True, height=400)

            # Кнопка скачивания
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                final_df.to_excel(writer, index=False, sheet_name='Объединенные данные')

                # Применяем оранжевый цвет к дубликатам в Excel
                workbook = writer.book
                worksheet = writer.sheets['Объединенные данные']

                from openpyxl.styles import PatternFill
                orange_fill = PatternFill(start_color='FFA500', end_color='FFA500', fill_type='solid')

                # Выделяем дубликаты (начиная со строки 2, т.к. строка 1 - заголовок)
                for row_idx in range(2, duplicate_rows + 2):
                    for col_idx in range(1, len(final_df.columns) + 1):
                        worksheet.cell(row=row_idx, column=col_idx).fill = orange_fill

            output.seek(0)

            st.download_button(
                label=f"📥 Скачать объединенный файл ({total_rows} строк)",
                data=output,
                file_name=f"merged_file_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="download_merged_file"
            )

    except Exception as e:
        st.error(f"❌ Ошибка при обработке файлов: {str(e)}")
        st.info("Убедитесь, что все файлы имеют одинаковые столбцы.")

st.markdown("---")

# =====================================================
# Раздел: Сверки с клиентами
# =====================================================
st.markdown('<div id="сверки-с-клиентами"></div>', unsafe_allow_html=True)
st.header("🔄 Сверки с клиентами")

st.markdown("""
Инструменты для сверки данных с различными клиентами. Выберите нужную сверку ниже.
""")

# Открываем контейнер для стилизации блоков кода
st.markdown('<div class="matrix-code-section">', unsafe_allow_html=True)

# Яндекс.Еда - активная сверка
with st.expander("Яндекс.Еда", expanded=False):
    st.markdown("""
    ### Инструкция по запуску

    **ВАЖНО!** Скрипт нужно запускать в Google Colab в строгой последовательности:

    1. **Сначала запустите код установки библиотек** (Блок 1 ниже)
    2. **Дождитесь завершения установки**
    3. **Только потом запускайте основной код** (Блок 2 ниже)

    #### Ожидаемые файлы:
    - **"ООО Хэдхантер Биллинг....."** - отчет биллинг
    - **"Отчет-по-откликам-по-проектам-работодателя-"** - отчет внутренний hh
    - **"Leads_"** - лиды из ЛК Я.Еды
    """)

    st.markdown("---")

    # Блок 1: Установка библиотек
    st.markdown("### Блок 1: Установка библиотек")
    st.markdown("**Запустите этот код ПЕРВЫМ в отдельной ячейке Google Colab:**")

    libs_code = """!pip install pandas openpyxl fuzzywuzzy python-Levenshtein"""

    st.code(libs_code, language="python")

    st.markdown("---")

    # Блок 2: Основной код
    st.markdown("### Блок 2: Основной код сверки")
    st.markdown("**Запустите этот код ВТОРЫМ после установки библиотек:**")

    st.info("**Кнопка копирования** в правом верхнем углу блока кода скопирует **весь код целиком**")

    # Читаем основной код из файла
    try:
        with open("yaedamatch", "r", encoding="utf-8") as f:
            full_code = f.read()

        # Извлекаем код начиная с импорта библиотек
        main_code_start = full_code.find("# ============================================\n# ИМПОРТ БИБЛИОТЕК")
        if main_code_start != -1:
            main_code = full_code[main_code_start:]
        else:
            main_code = full_code

        # Отображаем код
        with st.expander("Показать код", expanded=False):
            st.code(main_code, language="python", line_numbers=False)

    except FileNotFoundError:
        st.error("Файл yaedamatch не найден. Обратитесь к администратору.")

# Остальные клиенты - в разработке
with st.expander("Пятерочка", expanded=False):
    st.info("Сверка для этого клиента находится в разработке")

with st.expander("Ростелеком", expanded=False):
    st.info("Сверка для этого клиента находится в разработке")

with st.expander("Ингосстрах", expanded=False):
    st.info("Сверка для этого клиента находится в разработке")

# Закрываем контейнер matrix-code-section
st.markdown('</div>', unsafe_allow_html=True)

st.markdown("---")
st.markdown(
    "Сделано с ❤️ | Данные из API HH.ru",
    unsafe_allow_html=True
)
