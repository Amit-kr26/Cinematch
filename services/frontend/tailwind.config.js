/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        // Cinematic near-black surface ramp (kept under the `navy` name so existing
        // components restyle automatically).
        navy: {
          950: '#050505',
          900: '#0d0d0f',
          800: '#161618',
          700: '#202024',
          600: '#2c2c31',
          500: '#3f3f46',
        },
        // Cinema red primary accent
        accent: {
          DEFAULT: '#E50914',
          light:   '#F6121D',
          dark:    '#B81D24',
        },
        gold: '#F59E0B',
      },
      fontFamily: {
        display: ['Outfit', 'system-ui', 'sans-serif'],
        sans:    ['Manrope', 'system-ui', 'sans-serif'],
      },
      keyframes: {
        fadeSlideUp: {
          '0%':   { opacity: '0', transform: 'translateY(16px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
        scaleIn: {
          '0%':   { opacity: '0', transform: 'scale(0.96)' },
          '100%': { opacity: '1', transform: 'scale(1)' },
        },
        shimmer: {
          '0%':   { backgroundPosition: '-400px 0' },
          '100%': { backgroundPosition: '400px 0' },
        },
      },
      animation: {
        'fade-slide-up': 'fadeSlideUp 0.4s ease-out forwards',
        'scale-in':      'scaleIn 0.25s ease-out forwards',
        shimmer:         'shimmer 1.4s infinite linear',
      },
    },
  },
  plugins: [],
}
