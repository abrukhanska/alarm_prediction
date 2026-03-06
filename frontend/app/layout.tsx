import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "AEGIS — Air Event Guardian & Intelligence System",
  description: "Real-time alarm prediction dashboard for Ukraine",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="bg-[#0a0f1a] text-[#e2e8f0] min-h-screen">
        {children}
      </body>
    </html>
  );
}
