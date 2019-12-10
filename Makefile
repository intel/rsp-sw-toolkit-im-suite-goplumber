# Apache v2 license
#  Copyright (C) <2019> Intel Corporation
#
#  SPDX-License-Identifier: Apache-2.0
#

.PHONY: build docker run

build:
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo ./cmd/goplumber

docker:
	docker build -t my/goplumber .

run:
	docker run -it \
		-v $$(PWD)/testdata:/config \
		--net edgex-network \
		my/goplumber -config /config/plumber.json
