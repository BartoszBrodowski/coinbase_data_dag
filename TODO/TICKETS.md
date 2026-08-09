# Bilety implementacyjne

## TICKET-001 — Lokalne środowisko Docker

**Cel:** uruchomić Airflow i PostgreSQL bez budowania pipeline'u.

- [x] Zdefiniuj usługi PostgreSQL, Airflow API Server, scheduler i DAG Processor.
- [x] W jednym kontenerze PostgreSQL utwórz osobne bazy: metadane Airflow i `market_data`.
- [x] Dodaj healthchecki i trwałe wolumeny.
- [x] Dodaj `.env.example`, ale nie commituj prawdziwych haseł.
- [x] Zaloguj się do UI Airflow i uruchom pusty testowy DAG ręcznie.

**Gotowe, gdy:** po restarcie kontenerów UI działa, a dane PostgreSQL nie znikają.

## TICKET-002 — Schemat bazy i migracje

**Cel:** utworzyć schematy `bronze`, `silver` i `gold` w bazie `market_data`.

- [x] Opisz kolumny tabeli surowych odpowiedzi Bronze.
- [x] Opisz typowany model świecy Silver.
- [x] Opisz stabilny model odczytowy Gold.
- [x] Dodaj klucz unikalny dla produktu, granulacji i czasu świecy.
- [x] Zdecyduj, jak uruchamiane i wersjonowane będą migracje.

**Gotowe, gdy:** pustą bazę można odtworzyć w powtarzalny sposób.

## TICKET-003 — Rozpoznanie API Coinbase

**Cel:** zrozumieć kontrakt źródła przed pisaniem DAG-a.

- [x] Przeczytaj aktualną dokumentację endpointu świec.
- [x] Zanotuj limit rekordów, dozwolone granulacje i zachowanie granic czasu.
- [x] Zapisz małą przykładową odpowiedź jako fixture testowy.
- [x] Sprawdź odpowiedź dla błędnego produktu i pustego zakresu.

**Gotowe, gdy:** potrafisz ręcznie wyjaśnić każde pole odpowiedzi i ograniczenia endpointu.

## TICKET-004 — Ingest do Bronze

**Cel:** zachować surową odpowiedź i kontekst pobrania.

- [x] Pobierz tylko jeden mały, jawny przedział czasu.
- [ ] Ustaw timeout oraz ograniczone retry.
- [x] Zapisz payload i metadane zapytania w jednej transakcji.
- [x] Loguj zakres, produkt oraz liczbę rekordów; nie loguj sekretów.
- [x] Zdefiniuj bezpieczne zachowanie przy ponownym uruchomieniu.

**Gotowe, gdy:** można powtórzyć pobranie i ustalić, z jakiego zapytania pochodzi payload.

## TICKET-005 — Transformacja Bronze → Silver

**Cel:** uzyskać poprawne, typowane i unikalne świece.

- [x] Zamień czas na UTC oraz ceny i wolumen na typ numeryczny.
- [x] Sprawdź wymagane pola i sensowne zależności OHLC.
- [x] Usuń duplikaty według klucza świecy.
- [x] Zapisuj przez upsert.
- [x] Dodaj test jednostkowy z fixture bez sieci i Airflow.

**Gotowe, gdy:** dwukrotne przetworzenie payloadu daje jeden zestaw rekordów.

## TICKET-006 — Warstwa Gold

**Cel:** dostarczyć prosty i stabilny kontrakt dla interfejsu.

- [ ] Ustal minimalny zestaw pól potrzebny wykresowi świec.
- [ ] Zapewnij szybkie filtrowanie po produkcie, granulacji i czasie.
- [ ] Wybierz tabelę lub widok i zapisz uzasadnienie decyzji.
- [ ] Dodaj test liczby rekordów oraz unikalności.

**Gotowe, gdy:** pojedyncze zapytanie zwraca posortowane dane do wykresu.

## TICKET-007 — DAG Airflow

**Cel:** połączyć istniejące, testowalne kroki w powtarzalny pipeline.

- [ ] Zdefiniuj harmonogram, `start_date`, `catchup` i retry.
- [ ] Użyj przedziału danych przekazanego przez Airflow.
- [ ] Ułóż zależności: ingest → Silver → Gold → test jakości.
- [ ] Przekazuj między zadaniami małe metadane, a nie całe payloady.
- [ ] Przetestuj ręczny run i ponowienie nieudanego zadania.

**Gotowe, gdy:** widok Grid pokazuje udany pełny przebieg dla jednego przedziału.

## TICKET-008 — Wykres świec

**Cel:** wyświetlić dane Gold bez łączenia interfejsu z pipeline'em.

- [ ] Utwórz minimalny ekran Streamlit z wykresem Plotly.
- [ ] Dodaj wybór produktu i zakresu czasu dopiero po działaniu wersji stałej.
- [ ] Ogranicz liczbę pobieranych rekordów.
- [ ] Obsłuż pusty wynik czytelnym komunikatem.
- [ ] Użyj użytkownika bazy z prawem tylko do odczytu Gold.

**Gotowe, gdy:** wykres działa także wtedy, gdy Airflow jest chwilowo zatrzymany.

## TICKET-009 — Jakość i eksploatacja

**Cel:** wykrywać problemy, zanim trafią na wykres.

- [ ] Sprawdzaj unikalność, wartości null, kolejność OHLC i luki czasowe.
- [ ] Rozróżnij błędy chwilowe API od błędów danych.
- [ ] Dodaj test integracyjny z PostgreSQL.
- [ ] Zmierz liczbę rekordów pobranych, odrzuconych i zapisanych.
- [ ] Opisz procedurę małego backfillu i ponowienia zadania.

**Gotowe, gdy:** celowo uszkodzony fixture powoduje czytelny błąd odpowiedniego zadania.
