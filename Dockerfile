# Ensure Chrome/Chromium is available for Puppeteer/whatsapp-web.js
FROM apify/actor-node:20

# Install Chromium and dependencies - Apify uses Alpine Linux
RUN echo "http://dl-cdn.alpinelinux.org/alpine/edge/main" > /etc/apk/repositories \
    && echo "http://dl-cdn.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories \
    && echo "http://dl-cdn.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories \
    && apk --no-cache update \
    && apk --no-cache upgrade \
    && apk add --no-cache \
      chromium \
      chromium-chromedriver \
      nss \
      freetype \
      freetype-dev \
      harfbuzz \
      ca-certificates \
      ttf-freefont \
      font-noto-emoji \
      font-noto \
      font-noto-cjk

# Set Puppeteer to use installed Chromium
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium-browser
ENV CHROME_BIN=/usr/bin/chromium-browser
ENV CHROME_PATH=/usr/bin/chromium-browser

# Copy application
COPY package*.json ./
RUN npm ci --omit=dev --omit=optional || npm install --only=prod --no-optional
COPY . ./

# Ensure the executable path is correct
RUN which chromium-browser || which chromium || echo "Warning: Chromium not found in PATH"

CMD [ "node", "src/main.js" ]