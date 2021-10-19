# Telegraf Service
#
# This Dockerfile is used to create images for the
# Telegraf service monitoring the FDB cluster used
# by AE.

FROM pexae.azurecr.io/ae-lookup-deploy:1.0_fdb6.3

RUN mkdir -p /opt/ae

WORKDIR /opt/ae
COPY telegraf telegraf
ENTRYPOINT ./telegraf
