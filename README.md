# Spring Kafka MVP - Sistem za ubradu porudžbina

Ovo je jednostavan, asinhroni event-driven sistem izgrađen pomoću **Spring Boot**-a i **Apache Kafka** brokera. Projekat simulira osnovni tok između kreiranja porudžbine i provere stanja na lageru.

---

## Ključne Karakteristike & Arhitektura

* **In-Memory Stanje (Bez Baze):** Radi maksimalne jednostavnosti i fokusa na same asinkrone mehanizme, sistem koristi heš mape umesto perzistentne baze podataka.
* **Konkurentnost i Bezbednost Niti:** Zaštita od race condition-a pod velikim opterećenjem rešena je na nivou memorije pomoću atomičnih operacija (putIfAbsent i computeIfPresent)
* **Idempotentnost:** Sistem sprečava duplu obradu istih poruka proverom i skladištenjem orderId-a u memorijski keš pre nego što započne izmenu stanja.
* **Otpornost na Greške & DLQ (Dead Letter Queue):** Implementirana je neblokirajuća strategija ponovnih pokušaja. Ako obrada ne uspe usled tehničke greške, poruka prolazi kroz retry faze i na kraju završava u DLT handleru, bez blokiranja ostalih poruka na Kafkinom topiku.

---

## Pokretanje Infrastrukture (Docker)

Za testiranje aplikacije lokalno, unutar projekta se nalazi `docker-compose.yml` fajl koji podiže kompletan Kafka klaster (Zookeeper i Kafka broker).

```bash
docker-compose up -d
```
---

## Testiranje Aplikacije

Najlakši način za testiranje aplikaciju i proveru ponašanja u runtime-u je putem sledećih `curl` komandi:

### 1. Uspešno slanje regularne porudžbine (Happy Path)
Ova komanda šalje ispravnu porudžbinu, smanjuje lager za `item-1` i označava porudžbinu kao uspešnu.
```bash
curl -X POST http://localhost:8080/orders -H "Content-Type: application/json" -d "{\"orderId\": \"1\", \"itemId\": \"item-1\", \"quantity\": 1}"
```
### 2. Simulacija mrežnog prekida (Retry & Dead Letter Queue)
Slanjem artikla pod nazivom timeout-item konzumer namerno baca izuzetak. Možeš pratiti konzolu kako aplikacija pokušava retry sa zadrškom, a zatim trajno prebacuje poruku u DLQ.
```bash
curl -X POST http://localhost:8080/orders -H "Content-Type: application/json" -d "{\"orderId\": \"2\", \"itemId\": \"timeout-item\", \"quantity\": 1}"
```
### 3. Stress-Test (Visoka konkurentnost)
Generiše 100 porudžbina asinhrono u pozadini. Koristi se za proveru stabilnosti, brzine obrade konzumera i dokaz da nema preklapanja u stanju lagera pod naletom poruka.
```bash
curl -X POST http://localhost:8080/orders/stress-test?count=100 -H "Content-Type: application/json"
```
