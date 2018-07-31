docker-build:
	docker-compose build

docker-test: | docker-build
	docker-compose up --abort-on-container-exit --exit-code-from radical
