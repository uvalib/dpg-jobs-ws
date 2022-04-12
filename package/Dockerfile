#
# build the target application
#
FROM public.ecr.aws/docker/library/golang:1.18-alpine3.15 as builder
RUN apk update && apk upgrade && apk add --no-cache make npm yarn

WORKDIR /build
COPY go.mod go.sum Makefile ./
COPY cmd ./cmd
COPY templates ./templates
COPY assets ./assets
RUN make linux

#
# build the target container
#
FROM public.ecr.aws/debian/debian:buster-slim

# update the packages
RUN apt-get -y update && apt-get -y upgrade && apt-get -y install bash tzdata ca-certificates curl exiftool imagemagick

# Create the run user and group
RUN groupadd -g 16339 lb-digiserv && useradd -u 118698 -g lb-digiserv libsnlocal

# set the timezone appropriatly
ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Specify home
ENV APP_HOME /dpg-jobs-ws
WORKDIR $APP_HOME

# Create necessary directories
RUN mkdir -p $APP_HOME $APP_HOME/bin $APP_HOME/scripts
RUN chown -R libsnlocal $APP_HOME && chgrp -R lb-digiserv $APP_HOME

# port and run command
EXPOSE 8080
CMD scripts/entry.sh

# Move in necessary assets
COPY package/data/container_bash_profile /home/libsnlocal/.profile
COPY package/scripts/entry.sh $APP_HOME/scripts/entry.sh
COPY package/bin/linux/kdu_compress /usr/local/bin/kdu_compress
COPY package/bin/linux/libkdu_v63R.so /usr/lib/libkdu_v63R.so
RUN ldconfig -v
COPY --from=builder /build/bin/dpg-jobs-ws.linux $APP_HOME/bin/dpg-jobs-ws
COPY --from=builder /build/bin/templates $APP_HOME/bin/templates
COPY --from=builder /build/bin/assets $APP_HOME/bin/assets

# Ensure permissions are correct
RUN chown libsnlocal:lb-digiserv /home/libsnlocal/.profile $APP_HOME/scripts/entry.sh $APP_HOME/bin/dpg-jobs-ws && chmod 755 /home/libsnlocal/.profile $APP_HOME/scripts/entry.sh $APP_HOME/bin/dpg-jobs-ws
RUN chown -R libsnlocal:lb-digiserv $APP_HOME/bin/assets $APP_HOME/bin/templates && chmod -R 755 $APP_HOME/bin/assets $APP_HOME/bin/templates
RUN chown -R libsnlocal:lb-digiserv $APP_HOME/bin && chmod -R 755 $APP_HOME/bin

# Add the build tag
ARG BUILD_TAG
RUN test -n "$BUILD_TAG" && touch $APP_HOME/buildtag.build-$BUILD_TAG || touch $APP_HOME/buildtag.build-0

# Specify the user
USER libsnlocal

#
# end of file
#
