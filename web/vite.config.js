import { defineConfig } from 'vite'
import { createHtmlPlugin } from 'vite-plugin-html'
import react from '@vitejs/plugin-react'
import svgr from 'vite-plugin-svgr'
export default defineConfig({
    root: 'src',
    build: {
        // Relative to the root
        outDir: '../dist',
        emptyOutDir: true,
        chunkSizeWarningLimit: 3000,
    },
    publicDir: 'static',
    server: {
        proxy: {
            '/openapi.json': 'http://127.0.0.1:8000/',
            '/graphql': 'http://127.0.0.1:8000/',
        },
    },
    plugins: [
        createHtmlPlugin({
            inject: {
                data: {
                    PUBLIC_URL: '/',
                },
            },
        }),
        react({
            // Use React plugin in all *.jsx and *.tsx files
            include: '**/*.{jsx,tsx}',
        }),
        svgr(),
    ],
    define: {
        PROJECT_GROUPS: JSON.stringify(process.env.PROJECT_GROUPS) || '[]',
    },
})
