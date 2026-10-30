FROM node:24-alpine AS build
WORKDIR /console
COPY console/package.json console/package-lock.json ./
RUN npm ci
COPY console/ ./
RUN npm run build

FROM nginx:stable-alpine
ENV API_UPSTREAM=http://anyseek:8080
COPY --from=build /console/dist /usr/share/nginx/html
COPY console/nginx.conf.template /etc/nginx/templates/default.conf.template
EXPOSE 80
