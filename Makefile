
test:
	docker rm -f pstore-test ;\
		docker run --name pstore-test -d -p 5432:5432 postgres:10 &&\
		sleep 3 &&\
		docker exec pstore-test psql -U postgres -c 'create database store;' &&\
		go test -v ./...
