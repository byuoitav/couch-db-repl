ORG=byuoitav
NAME=$(shell basename "$(PWD)")
BRANCH=$(shell git rev-parse --abbrev-ref HEAD)

UNAME=$(shell echo $(DOCKER_USERNAME))
PASSW=$(shell echo $(DOCKER_PASSWORD))

all:
	docker build -t $(ORG)/$(NAME):$(BRANCH) -f Dockerfile-ARM .
	docker login -u $(UNAME) -p $(PASSW)
	docker push $(ORG)/$(NAME):$(BRANCH)
