FROM node:18.14.0

WORKDIR /app
COPY ./protos /app/protos
COPY ./src /app/src
COPY ./package.json /app/package.json
RUN npm install

ENTRYPOINT [ "node", "src/main.js" ]