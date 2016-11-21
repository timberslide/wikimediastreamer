all:
	docker build -t timberslide/wikimediastreamer .

push:
	docker push timberslide/wikimediastreamer
