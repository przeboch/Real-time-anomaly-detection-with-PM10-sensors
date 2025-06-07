# 📡 Wykrywanie anomalii z czujników PM10 (Kafka + Spark + Jupyter)

Ten projekt przetwarza dane PM10 w czasie rzeczywistym z użyciem Apache Kafka i Spark Structured Streaming.

---

## 🔧 Środowisko developerskie

Środowisko zostało pobrane z repozytorium:  
👉 [https://github.com/sebkaz/jupyterlab-project](https://github.com/sebkaz/jupyterlab-project)

### 📦 Zawartość środowiska

- **Zookeeper**
- **Apache Kafka Broker** (z katalogiem `kafka` do obsługi z terminala)
- **MongoDB**
- **JupyterLab** z preinstalowanym środowiskiem Apache Spark

---

## ▶️ Uruchomienie

Aby uruchomić środowisko, wykonaj:

```bash
cd jupyterlab-project
docker compose up
