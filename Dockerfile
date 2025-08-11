# Custom Dockerfile to ensure Chrome deps are present for Puppeteer/whatsapp-web.js
FROM apify/actor-node:20

# Install Chrome and required dependencies
RUN apt-get update -y && apt-get install -y \
    chromium \
    chromium-sandbox \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

COPY . ./
RUN npm ci --omit=dev --omit=optional || npm install --only=prod --no-optional && (npm list || true)

CMD [ "node", "src/main.js" ]


