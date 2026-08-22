import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: ['./src/__tests__/setup.ts'],
  },
  server: {
    proxy: {
      '/recommend': 'http://localhost:8000',
      '/simulate':  'http://localhost:8000',
      '/events':    'http://localhost:8000',
      '/movies':    'http://localhost:8000',
      '/genres':    'http://localhost:8000',
      '/stats':     'http://localhost:8000',
      '/health':    'http://localhost:8000',
      '/ws':        { target: 'ws://localhost:8000', ws: true },
    }
  }
})
