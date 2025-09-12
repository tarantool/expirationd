# This way everything works as expected ever for
# `make -C /path/to/project` or
# `make -f /path/to/project/Makefile`.
MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJECT_DIR := $(patsubst %/,%,$(dir $(MAKEFILE_PATH)))
LUACOV_REPORT := $(PROJECT_DIR)/luacov.report.out
LUACOV_STATS := $(PROJECT_DIR)/luacov.stats.out
METRICS ?= $(METRIC_VERSION)
CARTRIDGE ?= $(CARTRIDGE_VERSION)

SHELL := $(shell which bash) # Required for brace expansion used in a clean target.
SEED ?= $(shell /bin/bash -c "echo $$RANDOM")

SHELL := /bin/bash

ARCH := $(shell uname -m)
PLATFORM ?= $(shell uname -s | tr [:upper:] [:lower:])
ifeq ($(PLATFORM), darwin)
    PLATFORM := macos
endif

PWD := $(shell pwd)

TARANTOOL_BUNDLE_PATH ?= enterprise/dev/$(PLATFORM)/$(ARCH)/2.11/tarantool-enterprise-sdk-gc64-2.11.5-0-g74b51db7f-r662.$(PLATFORM).$(ARCH).tar.gz

VERSION_BRANCH := $(shell git branch --show-current)
VERSION := $(shell git describe --tags --long | cut -d'-' -f1-2)
ROCK_FILENAME := expirationd-${VERSION}.all.rock


S3_ENDPOINT_URL ?= https://hb.bizmrg.com

all: test

# The template (ldoc.tpl) is written using tarantool specific
# functions like string.split(), string.endswith(), so we run
# ldoc using tarantool.
apidoc:
	ldoc -c $(PROJECT_DIR)/doc/ldoc/config.ld \
             -d $(PROJECT_DIR)/doc/apidoc/ expirationd/

check: luacheck

luacheck:
	luacheck --config .luacheckrc --codes .

.PHONY: test
test:
	.rocks/bin/luatest -v --coverage --shuffle all:${SEED}

$(LUACOV_STATS): test

coverage: $(LUACOV_STATS)
	sed -i -e 's@'"$$(realpath .)"'/@@' $(LUACOV_STATS)
	cd $(PROJECT_DIR) && luacov expirationd/*.lua
	grep -A999 '^Summary' $(LUACOV_REPORT)

coveralls: $(LUACOV_STATS)
	echo "Send code coverage data to the coveralls.io service"
	luacov-coveralls --include ^expirationd --verbose --repo-token ${GITHUB_TOKEN}

deps:
	tt rocks install luatest 1.0.1
	tt rocks install luacheck 0.26.0
	tt rocks install luacov 0.13.0-1
	tt rocks install ldoc --server=https://tarantool.github.io/LDoc/
	tt rocks install luacov-coveralls 0.2.3-1 --server=http://luarocks.org
	tt rocks make

deps-full: deps
	tt rocks install cartridge 2.16.3
ifneq ($(strip $(METRICS)),)
	tt rocks install metrics $(METRICS)
endif

.SILENT: sdk
sdk: ## Download and Install Tarantool SDK
	echo Download and Install Tarantool SDK on $(ARCH)
	aws --endpoint-url "$(S3_ENDPOINT_URL)" s3 cp "s3://packages/$(TARANTOOL_BUNDLE_PATH)" ./sdk.tar.gz \
	&& mkdir -p sdk \
	&& tar -xzvf ./sdk.tar.gz -C sdk --strip 1 \
	&& rm -f ./sdk.tar.gz \
	&& chmod 644 sdk/rocks/*

.rocks: sdk ## Install Rocks
	source sdk/env.sh \
	&& tt rocks install vshard 0.1.36 \
	&& tt rocks install luatest 1.0.1 \
	&& tt rocks install luacov 0.13.0 \
	&& tt rocks install luacov-reporters 0.1.0 \
	&& tt rocks install luacheck 0.26.0
ifneq ($(strip $(METRICS)),)
	source sdk/env.sh && tt rocks install metrics $(METRICS)
endif
ifneq ($(strip $(CARTRIDGE)),)
	source sdk/env.sh && tt rocks install cartridge $(CARTRIDGE)
endif

install-debug-helper:
	source sdk/env.sh && tt rocks install https://raw.githubusercontent.com/a1div0/lua-debug-helper/main/lua-debug-helper-1.0.2-1.rockspec

.PHONY: bootstrap
bootstrap: .rocks ## Installs all dependencies

lint:
	source sdk/env.sh && .rocks/bin/luacheck .
