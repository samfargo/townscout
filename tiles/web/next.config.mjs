// Next.js configuration for the vicinity frontend.
/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  experimental: {
    typedRoutes: true
  },
  env: {
    // Default API URL for development (can be overridden by .env.local)
    NEXT_PUBLIC_API_URL: process.env.NEXT_PUBLIC_API_URL || 'http://localhost:5173'
  }
};

export default nextConfig;
