#
# build the target application
#
FROM public.ecr.aws/docker/library/golang:1.18-alpine3.15 as builder
RUN apk update && apk upgrade && apk add --no-cache make npm yarn

WORKDIR /build
COPY go.mod go.sum Makefile ./
COPY cmd ./cmd
RUN make linux

#
# build the target container
#
FROM public.ecr.aws/docker/library/alpine:3.15

# update the packages
RUN apk update && apk upgrade && apk add bash tzdata ca-certificates curl exiftool imagemagick && rm -rf /var/cache/apk/*

# Create the run user and group
RUN addgroup --gid 16339 lb-digiserv && adduser --uid 118698 libsnlocal -G lb-digiserv -D

# set the timezone appropriatly
ENV TZ=UTC
RUN cp /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

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
COPY --from=builder /build/bin/imagingsvc.linux $APP_HOME/bin/imagingsvc
COPY --from=builder /build/bin/public $APP_HOME/bin/public
COPY --from=builder /build/bin/data $APP_HOME/bin/data

# Ensure permissions are correct
RUN chown libsnlocal:lb-digiserv /home/libsnlocal/.profile $APP_HOME/scripts/entry.sh $APP_HOME/bin/imagingsvc && chmod 755 /home/libsnlocal/.profile $APP_HOME/scripts/entry.sh $APP_HOME/bin/imagingsvc
RUN chown -R libsnlocal:lb-digiserv $APP_HOME/bin/public $APP_HOME/bin/data && chmod -R 755 $APP_HOME/bin/public $APP_HOME/bin/data

# Add the build tag
ARG BUILD_TAG
RUN test -n "$BUILD_TAG" && touch $APP_HOME/buildtag.build-$BUILD_TAG || touch $APP_HOME/buildtag.build-0

# Specify the user
USER libsnlocal

#
# end of file
#
