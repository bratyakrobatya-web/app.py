"""
Модуль утилит экспорта для VR Мультитул

Этот модуль содержит функции для:
- Создания Excel файлов в памяти (BytesIO)
- Создания ZIP архивов
- Экспорта данных в различных форматах
"""

from typing import Dict, Optional, List
import pandas as pd
import io
import zipfile


def create_excel_buffer(
    df: pd.DataFrame,
    sheet_name: str = 'Sheet1',
    include_index: bool = False,
    include_header: bool = True
) -> io.BytesIO:
    """
    Создает Excel файл в памяти (BytesIO) из DataFrame

    Args:
        df: DataFrame для экспорта
        sheet_name: Название листа Excel (по умолчанию 'Sheet1')
        include_index: Включать ли индекс DataFrame (по умолчанию False)
        include_header: Включать ли заголовки столбцов (по умолчанию True)

    Returns:
        io.BytesIO: Буфер с Excel файлом, готовый для скачивания

    Examples:
        >>> df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
        >>> buffer = create_excel_buffer(df, sheet_name='Data')
        >>> # buffer готов для st.download_button
    """
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(
            writer,
            index=include_index,
            header=include_header,
            sheet_name=sheet_name
        )
    buffer.seek(0)
    return buffer


def create_publisher_excel(df: pd.DataFrame, city_column: str = 'Город') -> io.BytesIO:
    """
    Создает Excel файл в формате для публикатора (только столбец с городами, без заголовка)

    Args:
        df: DataFrame с данными городов
        city_column: Название столбца с городами (по умолчанию 'Город')

    Returns:
        io.BytesIO: Буфер с Excel файлом для публикатора

    Examples:
        >>> df = pd.DataFrame({'Город': ['Москва', 'Санкт-Петербург'], 'ID HH': ['1', '2']})
        >>> buffer = create_publisher_excel(df)
        >>> # Excel содержит только столбец с городами, без заголовка
    """
    publisher_df = pd.DataFrame({city_column: df[city_column]})
    return create_excel_buffer(
        publisher_df,
        sheet_name='Гео',
        include_index=False,
        include_header=False
    )


def create_full_report_excel(df: pd.DataFrame, sheet_name: str = 'Города') -> io.BytesIO:
    """
    Создает полный Excel отчет со всеми столбцами и заголовками

    Args:
        df: DataFrame с полными данными
        sheet_name: Название листа Excel (по умолчанию 'Города')

    Returns:
        io.BytesIO: Буфер с полным Excel отчетом

    Examples:
        >>> df = pd.DataFrame({
        ...     'Город': ['Москва', 'Санкт-Петербург'],
        ...     'ID HH': ['1', '2'],
        ...     'Регион': ['Москва', 'Санкт-Петербург']
        ... })
        >>> buffer = create_full_report_excel(df)
        >>> # Excel содержит все столбцы с заголовками
    """
    return create_excel_buffer(
        df,
        sheet_name=sheet_name,
        include_index=False,
        include_header=True
    )


def create_zip_archive(files_dict: Dict[str, bytes]) -> io.BytesIO:
    """
    Создает ZIP архив из словаря файлов

    Args:
        files_dict: Словарь {имя_файла: содержимое_файла_в_байтах}

    Returns:
        io.BytesIO: Буфер с ZIP архивом

    Examples:
        >>> files = {
        ...     'file1.xlsx': b'excel_data_here',
        ...     'file2.xlsx': b'more_excel_data'
        ... }
        >>> zip_buffer = create_zip_archive(files)
        >>> # zip_buffer готов для скачивания
    """
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for filename, content in files_dict.items():
            zip_file.writestr(filename, content)
    zip_buffer.seek(0)
    return zip_buffer


def create_result_excel(
    df: pd.DataFrame,
    sheet_name: str = 'Результат'
) -> io.BytesIO:
    """
    Создает Excel файл с результатами сопоставления/вакансий

    Используется для экспорта результатов обработки после сопоставления городов
    или обработки вакансий.

    Args:
        df: DataFrame с результатами
        sheet_name: Название листа Excel (по умолчанию 'Результат')

    Returns:
        io.BytesIO: Буфер с Excel файлом результатов

    Examples:
        >>> result_df = pd.DataFrame({
        ...     'Исходное название': ['Москва', 'Спб'],
        ...     'Итоговое гео': ['Москва', 'Санкт-Петербург'],
        ...     'Статус': ['✅ Точное', '⚠️ Похожее']
        ... })
        >>> buffer = create_result_excel(result_df)
    """
    return create_excel_buffer(
        df,
        sheet_name=sheet_name,
        include_index=False,
        include_header=True
    )


def prepare_file_for_archive(
    df: pd.DataFrame,
    filename: str,
    sheet_name: str = 'Sheet1',
    include_header: bool = True
) -> Dict[str, bytes]:
    """
    Подготавливает файл для добавления в архив

    Args:
        df: DataFrame для экспорта
        filename: Имя файла в архиве
        sheet_name: Название листа Excel
        include_header: Включать ли заголовки

    Returns:
        Dict[str, bytes]: Словарь {filename: file_content}

    Examples:
        >>> df = pd.DataFrame({'A': [1, 2, 3]})
        >>> file_dict = prepare_file_for_archive(df, 'data.xlsx', 'Data')
        >>> # Готово для передачи в create_zip_archive
    """
    buffer = create_excel_buffer(
        df,
        sheet_name=sheet_name,
        include_index=False,
        include_header=include_header
    )
    return {filename: buffer.getvalue()}


def create_multiple_sheets_excel(
    dataframes_dict: Dict[str, pd.DataFrame],
    include_index: bool = False,
    include_header: bool = True
) -> io.BytesIO:
    """
    Создает Excel файл с несколькими листами

    Args:
        dataframes_dict: Словарь {название_листа: DataFrame}
        include_index: Включать ли индекс DataFrame
        include_header: Включать ли заголовки столбцов

    Returns:
        io.BytesIO: Буфер с многолистовым Excel файлом

    Examples:
        >>> df1 = pd.DataFrame({'A': [1, 2]})
        >>> df2 = pd.DataFrame({'B': [3, 4]})
        >>> sheets = {'Sheet1': df1, 'Sheet2': df2}
        >>> buffer = create_multiple_sheets_excel(sheets)
        >>> # Excel файл с двумя листами
    """
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        for sheet_name, df in dataframes_dict.items():
            df.to_excel(
                writer,
                index=include_index,
                header=include_header,
                sheet_name=sheet_name
            )
    buffer.seek(0)
    return buffer
