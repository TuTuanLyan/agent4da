/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // The image is consumed as a server inside docker-compose. We keep the
  // standalone output so the production image stays small.
  output: "standalone",
  experimental: {
    // Phase 1 keeps the app simple; advanced features land later.
  },
  env: {
    NEXT_PUBLIC_API_BASE_URL: process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8083",
  },
};

export default nextConfig;
