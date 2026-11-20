FROM node:24-alpine
WORKDIR /app
RUN mkdir -p /data && chown node:node /data
COPY queue.mjs cli.mjs ./
ENV COURIER_DB=/data/courier.sqlite
USER node
ENTRYPOINT ["node", "cli.mjs"]
CMD ["work"]
