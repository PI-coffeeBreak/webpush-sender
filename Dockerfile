FROM node:22.14.0-alpine3.21

# Use dumb-init to handle signals properly (install as root)
RUN apk add --no-cache dumb-init

WORKDIR /usr/src/app

COPY package*.json ./

RUN npm install

COPY . .

# Add a non-root user for security
RUN addgroup -g 1001 -S nodejs
RUN adduser -S nodejs -u 1001

# Change ownership of the working directory
RUN chown -R nodejs:nodejs /usr/src/app
USER nodejs

EXPOSE 3000

ENTRYPOINT ["dumb-init", "--"]
CMD ["node", "index.js"]
