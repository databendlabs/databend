node_version:=$(shell node -v)
npm_version:=$(shell npm -v)
timeStamp:=$(shell date +%Y%m%d%H%M%S)


.PHONY: install build test clean

show:
		@ echo Timestamp: "$(timeStamp)"
		@ echo Node Version: $(node_version)
		@ echo npm_version: $(npm_version)

install:
		@ npm install
		@ pip install .
		@ pip install -r requirements.txt
		@ pip install \
			mkdocs-minify-plugin \
			mkdocs-redirects

build:
		@ echo "building in production mode"
		@ npm run build

deploy:
		@ echo "deploy to production"
		@ mkdocs gh-deploy --force

run:	build
		@ mkdocs serve

clean:
		@ echo "cleaning the dist directory"
		@ rm -rf dist
		@ rm -rf dist.tar.gz

INFO := @bash -c '\
  printf $(YELLOW); \
  echo "=> $$1"; \
  printf $(NC)' SOME_VALUE
