# Icu4j filter plugin for Embulk

Icu4j filter plugin for Embulk.
see. http://site.icu-project.org/

## Overview

* **Plugin type**: filter

## Configuration

- **key_names**: target key names. (list, required)
- **keep_input**: keep input columns. (bool, default: `true`)
- **settings**: settings. (list, required)
    - **suffix**: output column name suffix. if null overwrite column. (string, default: null)
    - **transliterators**: transliterator IDS(comma separated). see http://hondou.homedns.org/pukiwiki/pukiwiki.php?Java%20ICU4J. (string)
    - **case**: upper or lower (string, default: null)

## Example normalize NFKC

```yaml
filters:
  - type: icu4j
    key_names:
      - title
    settings:
      - { transliterators: 'Any-NFKC', case: upper }
```

## Example

```yaml
filters:
  - type: icu4j
    keep_input: false
    key_names:
      - catchcopy
    settings:
      - { suffix: _katakana, transliterators: 'Katakana-Hiragana,Fullwidth-Halfwidth', case: upper }
      - { transliterators: 'Katakana-Hiragana', case: lower }
      - { suffix: _romaji_lower, transliterators: 'Katakana-Hiragana,Hiragana-Latin', case: lower }
```

### input

```json
{
    "catchcopy" : "ホゲホゲ"
}
```

As below

```json
{
    "catchcopy" : "ほげほげ",
    "catchcopy_katakana" : "ﾎｹﾞﾎｹﾞ",
    "catchcopy_romaji_lower" : "hogehoge"
}
```

## transliterator rules
see. http://hondou.homedns.org/pukiwiki/pukiwiki.php?Java%20ICU4J

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
