import { cpus } from "os";
const totalCPUs = cpus().length;
const instances = Math.max(1, totalCPUs - 3);

export const apps = [
    {
        name: "search-prod-index",
        script: "dist/server.js",
        instances: instances,
        exec_mode: "cluster",
        watch: false,
        autorestart: true,
        max_memory_restart: "1000M",
        env: {
            NODE_ENV: "development",
        },
        env_production: {
            NODE_ENV: "production",
        }
    }
];
  