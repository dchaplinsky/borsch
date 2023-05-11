FROM python:3.6.9-alpine3.10

ARG root=/app
ARG user=app

WORKDIR ${root}

RUN /usr/sbin/adduser -D -h ${root} ${user}
COPY ./requirements.txt ${root}/requirements.txt
RUN apk add --no-cache su-exec postgresql-libs \
    && apk add --no-cache --virtual .build-deps zlib-dev postgresql-dev build-base \
    && PREFIX=/usr/local pip install -r  ${root}/requirements.txt \
    && runDeps="$( \
        scanelf --needed --nobanner --format '%n#p' --recursive /usr/local \
            | tr ',' '\n' \
            | sort -u \
            | awk 'system("[ -e /usr/local/lib" $1 " ]") == 0 { next } { print "so:" $1 }' \
        )" \
        apk add --no-cache --virtual .app-rundeps $runDeps \
    && apk del .build-deps
COPY . ${root}
RUN python3 -m compileall ${root}

VOLUME [ "/static", "${root}/keys" ]
EXPOSE 5000
ENV user=${user}
CMD [ "sh", "-c", "cp -a static/* /static/ && su-exec ${user} gunicorn --bind 0.0.0.0:5000 app:app" ]
