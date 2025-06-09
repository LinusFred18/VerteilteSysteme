# VerteilteSysteme

1. Bevor das Projekt gestartet werden kann, muss der Docker-Daemon gestartet 
werden 
2. Anschließend muss man im Projektordner in das entsprechende Verzeichnis 
wechseln (Docker-compose.yml muss sich darin befinden): 
./VerteilteSysteme/ 
3. Die Anwendung kann mit folgendem Befehl gestartet werden: 
docker-compose up --build 
4. Um das Monitoring aufzurufen und die verschiedenen Prozesse der 
Anwendung zu überwachen kann man auf folgenden Link gehen: 
http://localhost:8080/stats  
5. Die Anwendung kann mit STRG+C im Terminal beendet werden.  
Seite | T 
6. Um den Container korrekt herunterzufahren, kann man folgenden Befehl 
verwenden: 
docker-compose down 