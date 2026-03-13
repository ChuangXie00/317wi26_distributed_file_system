FROM nginx:alpine

RUN apk add --no-cache python3

WORKDIR /opt/meta-entry

COPY deploy/meta-entry/leader_controller.py /opt/meta-entry/leader_controller.py
COPY deploy/meta-entry/nginx.conf /etc/nginx/nginx.conf

RUN mkdir -p /etc/nginx/conf.d /var/lib/meta-entry/debug \
    && printf '%s\n' 'set $meta_upstream http://meta-01:8000;' > /etc/nginx/conf.d/meta_upstream.conf \
    && printf '%s\n' 'set $meta_leader_id meta-01;' >> /etc/nginx/conf.d/meta_upstream.conf \
    && printf '%s\n' '{"status":"bootstrapping"}' > /var/lib/meta-entry/debug/entry_status.json

CMD ["sh", "-c", "python3 /opt/meta-entry/leader_controller.py --once || true; python3 /opt/meta-entry/leader_controller.py & exec nginx -g 'daemon off;'"]
