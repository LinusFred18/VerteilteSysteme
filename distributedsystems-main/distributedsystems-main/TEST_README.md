# Test System Documentation

## Überblick

Das Test-System besteht aus einem TestClient, der automatisch verschiedene Aufgabentypen an das verteilte System sendet und die Ergebnisse protokolliert.

## Verwendung

### 1. System Startup (Wichtig!)

Zuerst muss das Kernsystem gestartet werden:

```bash
./manage.sh stop && ./manage.sh start-core --build
```

### 2. Worker starten

Anschließend können beliebig viele Worker der verschiedenen Typen gestartet werden:

```bash
# Beispiele für das Starten einzelner Worker:
./manage.sh start-workers reverse 1   # Startet 1 reverse-Worker
./manage.sh start-workers sum 1       # Startet 1 sum-Worker
./manage.sh start-workers hash 1      # Startet 1 hash-Worker
./manage.sh start-workers upper 1     # Startet 1 upper-Worker
./manage.sh start-workers wait 1      # Startet 1 wait-Worker
./manage.sh start-workers length 1    # Startet 1 length-Worker
./manage.sh start-workers average 1   # Startet 1 average-Worker
./manage.sh start-workers lower 1     # Startet 1 lower-Worker
./manage.sh start-workers prime 1     # Startet 1 prime-Worker

# Die Anzahl der Worker kann beliebig angepasst werden, z.B.:
./manage.sh start-workers reverse 3   # Startet 3 reverse-Worker
```

### 3. Überwachung der Tests

- Beobachten der Konsolen-Ausgabe
- Zugriff auf das Monitoring-Dashboard unter http://localhost:8080/stats

## Komponenten

### TestClient

- Sendet alle 30 Sekunden eine zufällige Aufgabe an den Dispatcher
- Unterstützt alle verfügbaren Aufgabentypen:
  - reverse (Text umkehren)
  - sum (Summe von Zahlen)
  - hash (Text hashen)
  - upper (Text in Großbuchstaben)
  - length (Textlänge)
  - average (Durchschnitt von Zahlen)
  - prime (Primzahlprüfung)
  - lower (Text in Kleinbuchstaben)
  - wait (Verzögerung)

## Fehlerbehandlung

Der TestClient behandelt verschiedene Fehlersituationen:

- Nicht verfügbare Worker
- Zeitüberschreitungen bei der Aufgabenverarbeitung
- Fehlgeschlagene Aufgaben
- Verbindungsprobleme zum Dispatcher

## Interpretation der Ergebnisse

- Erfolgreiche Tests zeigen den Status "COMPLETED"
- Fehlgeschlagene Tests zeigen den Status "FAILED" mit einer Fehlermeldung
- Die Verarbeitungszeit wird für jede Aufgabe protokolliert
- Das Monitoring-Dashboard zeigt die Gesamtstatistik der Worker
