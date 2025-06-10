# Verteiltes System - Dokumentation

## Projekt Übersicht

Taskgrid+ ist ein verteiltes System zum übernehmen von Aufgaben über Workern. Die Dokumentation hängt anbei als pdf.

Info: je nach Mac Docker Version: "docker compose" statt "docker-compose"

## Tech Stack

- **Programmiersprache**: Python
- **RPC Framework**: gRPC
- **Container**: Docker & Docker Compose
- **Netzwerk**: Custom Docker Network (taskgrid)
- **Testing**: Automatisierter Test-Client

## System-Komponenten

### 1. Nameservice

- Zentrales Registry-System für alle Services
- Verwaltet die Registrierung und Verfügbarkeit aller Dienste
- Port: 50051

### 2. Dispatcher

- Verteilt Aufgaben an verfügbare Worker
- Koordiniert die Lastverteilung
- Port: 50052

### 3. Worker

Führen verschiedene Aufgabentypen aus:

- reverse (Text umkehren)
- sum (Summe von Zahlen)
- hash (Text hashen)
- upper (Text in Großbuchstaben)
- length (Textlänge)
- average (Durchschnitt von Zahlen)
- prime (Primzahlprüfung)
- lower (Text in Kleinbuchstaben)
- wait (Verzögerung)

### 4. Monitoring

- Echtzeit-Überwachung aller System-Komponenten
- Web-Dashboard unter http://localhost:8080/stats
- gRPC Interface auf Port 50053

### 5. Test-Client

- Sendet alle 30 Sekunden automatisch Testaufgaben
- Protokolliert Ergebnisse und Verarbeitungszeiten
- Behandelt verschiedene Fehlersituationen

## Systemvoraussetzungen

### Allgemein

- Docker & Docker Compose
- Mindestens 4GB RAM für Docker
- Terminal/Command Line Zugriff

### Mac-spezifisch

- Docker Desktop für Mac
- Settings → Resources → Memory: Min. 4GB

### Linux-spezifisch

- Native Docker Installation
- Docker Compose Installation
- Systemd oder alternatives Init-System

## Setup-Varianten

### 1. Mit Management-Script (empfohlen)

```bash
# System starten
./manage.sh stop && ./manage.sh start-core --build

# Worker starten (Beispiele)
./manage.sh start-workers reverse 1   # Startet 1 reverse-Worker
./manage.sh start-workers sum 3       # Startet 3 sum-Worker
./manage.sh start-workers hash 1      # Startet 1 hash-Worker
./manage.sh start-workers upper 2     # Startet 2 upper-Worker
./manage.sh start-workers wait 1      # Startet 1 wait-Worker
./manage.sh start-workers length 1    # Startet 1 length-Worker
./manage.sh start-workers average 1   # Startet 1 average-Worker
./manage.sh start-workers lower 2     # Startet 2 lower-Worker
./manage.sh start-workers prime 1     # Startet 1 prime-Worker

# System stoppen
./manage.sh stop
```

### 2. Nur Docker (Alternative)

```bash
# Netzwerk erstellen (optional: Nicht bei Mac ausführen)
docker network create taskgrid

# Core Services starten
docker-compose -f docker-compose.core.yml up -d --build

# Worker starten (Beispiel: 3 reverse-Worker) + weitere docker nach wahl
docker-compose -f docker-compose.workers.yml up -d --build --scale worker-reverse=1 worker-reverse
docker-compose -f docker-compose.workers.yml up -d --build --scale worker-sum=1 worker-sum
docker-compose -f docker-compose.workers.yml up -d --build --scale worker-hash=1 worker-hash
docker-compose -f docker-compose.workers.yml up -d --build --scale worker-upper=1 worker-upper
docker-compose -f docker-compose.workers.yml up -d --build --scale worker-wait=1 worker-wait
docker-compose -f docker-compose.workers.yml up -d --build --scale worker-length=1 worker-length
docker-compose -f docker-compose.workers.yml up -d --build --scale worker-average=1 worker-average
docker-compose -f docker-compose.workers.yml up -d --build --scale worker-prime=1 worker-prime
docker-compose -f docker-compose.workers.yml up -d --build --scale worker-lower=1 worker-lower

# Von jedem einen starten
docker-compose -f docker-compose.workers.yml up -d --build \
    --scale worker-reverse=1 \
    --scale worker-sum=1 \
    --scale worker-hash=1 \
    --scale worker-upper=1 \
    --scale worker-wait=1 \
    --scale worker-length=1 \
    --scale worker-average=1 \
    --scale worker-prime=1 \
    --scale worker-lower=1

# System stoppen
docker-compose -f docker-compose.workers.yml down
docker-compose -f docker-compose.core.yml down

# Netzwerk removen
docker network rm taskgrid
```

## Mac vs. Linux Unterschiede

### Mac-spezifische Besonderheiten

- Verwendet Docker Desktop
- Ressourcen müssen explizit zugewiesen werden

### Linux-spezifische Besonderheiten

- Native Docker-Performance
- Direkter Zugriff auf System-Ressourcen

## Monitoring & Debugging

### Dashboard

- URL: http://localhost:8080/stats
- Zeigt Echtzeit-Statistiken
- Worker-Status und -Auslastung
- Aufgaben-Verteilung und -Status
- Zeigt ob Worker aktiv sind bzw. ob sie gerade arbeiten
- pending tasks und details der tasks auch sichtbar

### Logs & Debugging

```bash
# Core Services Logs
docker-compose -f docker-compose.core.yml logs -f

# Worker Logs
docker logs <container-id>

# Container Status
docker ps
```

## Fehlerbehandlung

### Häufige Probleme & Lösungen

1. Netzwerk-Probleme:

```bash
# Komplette Bereinigung
docker-compose down --remove-orphans
docker network prune -f
```

2. Port-Konflikte:

```bash
# Ports prüfen
lsof -i :<port>
# Prozess beenden
kill -9 <PID>
```

3. Speicherprobleme:

- Docker Desktop Ressourcen erhöhen
- Nicht benötigte Container/Images entfernen:

```bash
docker system prune -a
```

## Interpretation der Ergebnisse

- **COMPLETED**: Erfolgreich abgeschlossene Aufgaben
- **FAILED**: Fehlgeschlagene Aufgaben mit Fehlermeldung
- Verarbeitungszeiten werden protokolliert
- Statistiken im Monitoring-Dashboard

## Architektur

Das System folgt einer verteilten Microservice-Architektur:

1. Nameservice als zentrales Registry
2. Worker registrieren sich beim Nameservice
3. Dispatcher verteilt Aufgaben basierend auf Verfügbarkeit
4. Monitoring überwacht alle Komponenten
5. Test-Client simuliert Benutzeranfragen

---

© 2024 DHBW - Entwickelt von Linus, David, Matthias, Milan
