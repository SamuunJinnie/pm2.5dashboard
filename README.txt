// clear volume docker (if need) && clear image (if need)
0. docker volume rm $(docker volume ls -q)
   docker rmi -f $(docker images -aq)
// build image postgres with inital commnad in setup.sql
1. docker-compose build --no-cache postgres
// build other image and run
2. docker-compose up -d


docker volume rm $(docker volume ls -q)
docker rmi -f $(docker images -aq)
docker-compose build --no-cache postgres
docker-compose up -d