import { cpus } from "os";
const totalCPUs = cpus().length;
const instances = Math.max(1, totalCPUs - 3);

module.exports = {
    apps: [
      {
        name: "search-index-app",
        script: "pnpm",
        args: "run dev:app",
        instances: instances, // Number of instances to run (e.g., scale to multiple CPUs)
        exec_mode: "fork",
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
  