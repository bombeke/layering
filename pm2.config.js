module.exports = {
    apps: [
      {
        name: "dev-app",
        script: "pnpm dev:app",
        instances: 5, // Number of instances to run (e.g., scale to multiple CPUs)
        autorestart: true, // Restart on crash or failure
        watch: false, // Enable to restart on file changes
        max_memory_restart: "1G", // Restart if memory usage exceeds 1GB
        env: {
          NODE_ENV: "development",
        },
        env_production: {
          NODE_ENV: "production",
        },
      }
    ],
  };
  