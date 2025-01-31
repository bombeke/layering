module.exports = {
    apps: [
      {
        name: "dev-watch",
        script: "tsx",
        args: "watch src/index.ts",
        interpreter: "node",
      },
      {
        name: "dev-app",
        script: "tsx",
        args: "src/index.ts",
        interpreter: "node",
      }
    ],
  };
  