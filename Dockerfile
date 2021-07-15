FROM node:15
WORKDIR /app
COPY package.json /app
RUN npm install
COPY . /app
EXPOSE 3002
CMD node app.js
EXPOSE 5000
