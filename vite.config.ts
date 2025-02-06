import { defineConfig } from "vite";

export default defineConfig({
  build: {
    lib: {
      entry: "src/index.ts",
      formats: ["es"], 
    },
    target: "node16", 
    outDir: "dist", 
    minify: false, 
    sourcemap: true, 
    emptyOutDir: true
  },
});
