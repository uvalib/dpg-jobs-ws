#
# build the target application
#
FROM public.ecr.aws/docker/library/golang:1.21.0-alpine3.18 as builder
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
FROM public.ecr.aws/docker/library/alpine:3.18

# update the packages
RUN apk update && apk upgrade && apk add bash tzdata ca-certificates curl exiftool wget && rm -rf /var/cache/apk/*

# image magick support
RUN apk add fftw-double-libs fontconfig freetype ghostscript ghostscript-fonts lcms2 libbz2 libgcc libgomp libheif libjxl libltdl libraw libx11 libxext libxml2 openjpeg pango tiff zlib
COPY distro/bin/magick /usr/local/bin
RUN ln -s /usr/local/bin/magick /usr/local/bin/convert && ln -s /usr/local/bin/magick /usr/local/bin/identify
COPY distro/etc/ /usr/local/etc

# rclone support
RUN cd /tmp && wget https://downloads.rclone.org/rclone-current-linux-amd64.zip && unzip rclone-current-linux-amd64.zip && cp rclone-v*-linux-amd64/rclone /usr/local/bin && rm -fr rclone*

# Create the run user and group
RUN addgroup --gid 10708 libr-snlocal && addgroup --gid 16339 lb-digiserv && adduser --uid 118698 libsnlocal -G lb-digiserv -D && addgroup libsnlocal libr-snlocal

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
COPY package/data/rclone.conf /usr/local/etc/rclone.conf
COPY package/scripts/entry.sh $APP_HOME/scripts/entry.sh

# layer build assets
COPY --from=builder /build/bin/dpg-jobs-ws.linux $APP_HOME/bin/dpg-jobs-ws
COPY --from=builder /build/bin/templates $APP_HOME/bin/templates
COPY --from=builder /build/bin/assets $APP_HOME/bin/assets

# Ensure permissions are correct
RUN chown libsnlocal:lb-digiserv /home/libsnlocal/.profile $APP_HOME/scripts/entry.sh $APP_HOME/bin/dpg-jobs-ws && chmod 755 /home/libsnlocal/.profile $APP_HOME/scripts/entry.sh $APP_HOME/bin/dpg-jobs-ws
RUN chown -R libsnlocal:lb-digiserv $APP_HOME/bin/assets $APP_HOME/bin/templates && chmod -R 755 $APP_HOME/bin/assets $APP_HOME/bin/templates
RUN chown -R libsnlocal:lb-digiserv $APP_HOME/bin && chmod -R 755 $APP_HOME/bin

# cos decrypting rclone config needs to write here
RUN chmod 777 /usr/local/etc

# Add the build tag
ARG BUILD_TAG
RUN test -n "$BUILD_TAG" && touch $APP_HOME/buildtag.build-$BUILD_TAG || touch $APP_HOME/buildtag.build-0

# Specify the user
USER libsnlocal

#
# end of file
#
