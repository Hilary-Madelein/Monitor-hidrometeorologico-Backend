FROM node:18-alpine3.14

WORKDIR /app 

COPY package*.json .

RUN npm install

COPY . .

EXPOSE 9006

CMD ["npm", "start"]
