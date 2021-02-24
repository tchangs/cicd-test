FROM node:latest

WORKDIR /website

COPY package*.json ./

RUN npm install --force

COPY . .

CMD [ "npm", "start" ]
