# TODO — kolejność pracy

Każdy etap kończy się małym, działającym rezultatem. Nie zaczynaj pobierania
pełnej historii, dopóki pojedynczy przedział czasu nie przejdzie całej ścieżki.

Szczegółowe kryteria ukończenia znajdują się w [TICKETS.md](TICKETS.md).

## Etap 0 — decyzje

- [x] Wybierz pierwszą parę (`BTC-USD`).
- [x] Wybierz pierwszą granulację (np. 1 godzina).
- [x] Ustal częstotliwość DAG-a zgodną z granulacją.
- [x] Narysuj ręcznie jeden przykładowy przedział danych Airflow.

## Etap 1 — środowisko

- [x] Wykonaj TICKET-001: minimalny Docker Compose.
- [x] Wykonaj TICKET-002: schematy i migracje PostgreSQL.
- [x] Potwierdź, że Airflow łączy się z bazą danych rynkowych.

## Etap 2 — najcieńszy działający pipeline

- [x] Wykonaj TICKET-003: poznanie i przetestowanie odpowiedzi Coinbase.
- [x] Wykonaj TICKET-004: zapis Bronze dla jednego przedziału.
- [x] Wykonaj TICKET-005: transformacja Bronze → Silver.
- [x] Wykonaj TICKET-006: model Gold.
- [x] Wykonaj TICKET-007: połączenie kroków w DAG.

## Etap 3 — prezentacja

- [x] Wykonaj TICKET-008: prosty wykres świec z Gold.
- [ ] Sprawdź pusty zakres, jeden rekord i zwykły zakres danych.

## Etap 4 — niezawodność

- [ ] Wykonaj TICKET-009: testy, retry i jakość danych.
- [ ] Uruchom ten sam przedział dwa razy i potwierdź brak duplikatów.
- [ ] Wykonaj mały backfill, np. siedem dni.
- [x] Opisz uruchomienie projektu od pustego środowiska.

## Definicja ukończenia MVP

- [x] Jedno polecenie uruchamia kontenery lokalnie po skonfigurowaniu `.env`.
- [x] DAG pobiera zamknięte świece `BTC-USD` dla jednego interwału.
- [x] Dane przechodzą przez Bronze, Silver i Gold.
- [x] Ponowne uruchomienie nie tworzy duplikatów.
- [x] Błąd API jest widoczny w Airflow i podlega ograniczonemu retry.
- [x] Interfejs pokazuje wykres świec z danych Gold.
- [x] Testy sprawdzają główną transformację oraz klucz unikalny.
