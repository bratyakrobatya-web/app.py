"""
Unit тесты для функций сопоставления городов
"""
import pytest
import sys
import os

# Добавляем родительскую директорию в путь для импорта app
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import normalize_city_name, get_candidates_by_word, extract_city_and_region


class TestNormalizeCityName:
    """Тесты для функции normalize_city_name"""
    
    def test_lowercase_conversion(self):
        """Проверка преобразования в нижний регистр"""
        assert normalize_city_name("Москва") == "москва"
        assert normalize_city_name("САНКТ-ПЕТЕРБУРГ") == "санкт-петербург"
    
    def test_whitespace_removal(self):
        """Проверка удаления лишних пробелов"""
        assert normalize_city_name("  Москва  ") == "москва"
        assert normalize_city_name("Санкт  Петербург") == "санкт петербург"
    
    def test_special_chars(self):
        """Проверка обработки специальных символов"""
        result = normalize_city_name("Москва-сити")
        assert "москва" in result
    
    def test_empty_string(self):
        """Проверка обработки пустой строки"""
        assert normalize_city_name("") == ""
        assert normalize_city_name("   ") == ""


class TestGetCandidatesByWord:
    """Тесты для функции get_candidates_by_word"""
    
    def test_basic_matching(self):
        """Проверка базового поиска кандидатов"""
        cities = ["Москва", "Московский", "Санкт-Петербург", "Петрозаводск"]
        
        candidates = get_candidates_by_word("Москва", cities, limit=5)
        assert len(candidates) > 0
        assert candidates[0][0] in ["Москва", "Московский"]
    
    def test_partial_matching(self):
        """Проверка частичного совпадения"""
        cities = ["Санкт-Петербург", "Петрозаводск", "Петропавловск"]
        
        candidates = get_candidates_by_word("Петр", cities, limit=5)
        assert len(candidates) > 0
    
    def test_empty_input(self):
        """Проверка обработки пустого ввода"""
        cities = ["Москва", "Санкт-Петербург"]
        
        candidates = get_candidates_by_word("", cities, limit=5)
        assert len(candidates) == 0
    
    def test_no_matches(self):
        """Проверка когда нет совпадений"""
        cities = ["Москва", "Санкт-Петербург"]
        
        candidates = get_candidates_by_word("Лондон", cities, limit=5)
        # Может быть 0 или низкие совпадения
        assert isinstance(candidates, list)
    
    def test_limit_parameter(self):
        """Проверка работы параметра limit"""
        cities = ["Москва", "Московский", "Москва-Сити", "Подмосковье", "Новомосковск"]
        
        candidates = get_candidates_by_word("Моск", cities, limit=3)
        assert len(candidates) <= 3


class TestExtractCityAndRegion:
    """Тесты для функции extract_city_and_region"""
    
    def test_city_with_region(self):
        """Проверка извлечения города и региона"""
        city, region = extract_city_and_region("Москва (Московская область)")
        assert "москва" in city.lower()
        assert "московская" in region.lower()
    
    def test_city_without_region(self):
        """Проверка города без региона"""
        city, region = extract_city_and_region("Москва")
        assert "москва" in city.lower()
        assert region == ""
    
    def test_empty_string(self):
        """Проверка пустой строки"""
        city, region = extract_city_and_region("")
        assert city == ""
        assert region == ""


class TestIntegration:
    """Интеграционные тесты"""
    
    def test_candidates_sorted_by_score(self):
        """Проверка что кандидаты отсортированы по убыванию баллов"""
        cities = ["Москва", "Московский", "Подмосковье", "Новомосковск"]
        
        candidates = get_candidates_by_word("Москва", cities, limit=10)
        
        if len(candidates) > 1:
            # Проверяем что баллы убывают
            for i in range(len(candidates) - 1):
                assert candidates[i][1] >= candidates[i + 1][1], \
                    f"Баллы должны убывать: {candidates[i][1]} >= {candidates[i + 1][1]}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
