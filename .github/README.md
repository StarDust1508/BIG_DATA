# .github/

Папка для GitHub-специфичных файлов:

- `social-preview.svg` — превью репозитория (1280×640). Установите в Settings → Social preview.

## Как установить social preview

1. Откройте https://github.com/StarDust1508/BIG_DATA/settings
2. Найдите секцию **Social preview** (под Repository description).
3. Загрузите файл `.github/social-preview.svg` (или сконвертированный в PNG).
4. Сохраните.

Теперь когда кто-то делится ссылкой на репо в Telegram / Slack / LinkedIn — будет красивая карточка.

## Конвертация SVG → PNG (если GitHub не примет SVG)

```bash
# macOS / Linux через rsvg-convert
brew install librsvg
rsvg-convert -w 1280 -h 640 .github/social-preview.svg -o .github/social-preview.png

# Или через ImageMagick
brew install imagemagick
convert -density 300 -background none .github/social-preview.svg .github/social-preview.png
```

GitHub принимает PNG / JPG до 1 МБ.
