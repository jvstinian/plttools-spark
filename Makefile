.PHONY: build clean fix lint setup test unit

build:
	@sbt assembly

clean:
	@sbt clean
	@rm -rf ./target
	@rm -rf ./project/target

fix:
	@sbt scalafixAll
	@sbt scalafmtAll

lint:
	@sbt "scalafixAll --check"
	@sbt scalafmtCheckAll

setup:

test: clean build lint
	@make unit

unit:
	@sbt test

build-docker:
	@docker build -t plttools-spark:latest -f Dockerfile .

run-docker:
	@docker run -t plttools-spark:latest run

test-docker:
	@docker run -t plttools-spark:latest test

