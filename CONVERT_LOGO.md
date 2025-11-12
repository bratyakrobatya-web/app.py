# Конвертация логотипа

Файл `min-hh-red.eps` необходимо конвертировать в PNG для отображения в веб-приложении.

## Способы конвертации:

### 1. С помощью ImageMagick:
```bash
convert -density 300 min-hh-red.eps -resize 600x min-hh-red.png
```

### 2. С помощью Ghostscript:
```bash
gs -dSAFER -dBATCH -dNOPAUSE -dEPSCrop -r300 -sDEVICE=pngalpha -sOutputFile=min-hh-red.png min-hh-red.eps
```

### 3. Онлайн-сервисы:
- https://convertio.co/eps-png/
- https://cloudconvert.com/eps-to-png

После конвертации поместите файл `min-hh-red.png` в корневую директорию проекта.
