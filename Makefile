# set env var
.EXPORT_ALL_VARIABLES:
SHELL:=/bin/bash
# Variables

########################################################
# Lint                          			 							  #
########################################################
.PHONY: go-lint
go-lint:
	golangci-lint run

########################################################
# Test                         			 							    #
########################################################
.PHONY: go-test
go-test:
	go test -v -cover -count=1 ./... 

########################################################
# Run                        			 							       #
########################################################
.PHONY: go-run
go-run:
	go run main.go