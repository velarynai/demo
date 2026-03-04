import type { Metadata } from 'next';
import { Space_Grotesk, Sora } from 'next/font/google';
import './globals.css';

const spaceGrotesk = Space_Grotesk({
  subsets: ['latin'],
  variable: '--font-space-grotesk',
});

const sora = Sora({
  subsets: ['latin'],
  variable: '--font-sora',
});

export const metadata: Metadata = {
  title: 'Ragora Demo Studio',
  description: 'Stylish multi-collection chat demo powered by Ragora',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className={`${spaceGrotesk.variable} ${sora.variable} bg-slate-100 text-slate-900 antialiased`}>
        {children}
      </body>
    </html>
  );
}
