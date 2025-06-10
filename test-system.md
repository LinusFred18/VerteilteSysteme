./manage.sh start-workers reverse 1
./manage.sh start-workers sum 1
./manage.sh start-workers hash 1
./manage.sh start-workers upper 1
./manage.sh start-workers wait 1
./manage.sh start-workers length 1
./manage.sh start-workers average 1
./manage.sh start-workers lower 1
./manage.sh start-workers prime 1

oder mehrere:
./manage.sh start-workers length 2 --build

./manage.sh stop && ./manage.sh start-core --build
