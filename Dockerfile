FROM mcr.microsoft.com/playwright:v1.44.0-jammy

WORKDIR /app

# Install the exact dependency versions recorded in the lockfile.
COPY package.json package-lock.json ./
RUN npm ci

# Copy application source.
COPY index.js ./src/index.js
COPY scrape-orchestration.js ./src/scrape-orchestration.js
COPY monitoring-input.js ./src/monitoring-input.js

# Expose port
EXPOSE 3000

# Start
CMD ["node", "src/index.js"]
