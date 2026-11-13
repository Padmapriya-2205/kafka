import pino from "pino";

export function createLogger(service: string): pino.Logger {
  const isProduction = process.env.NODE_ENV === "production";

  return pino({
    base: { service },
    level: process.env.LOG_LEVEL ?? "info",
    ...(isProduction ? {} : {
      transport: {
        target: "pino-pretty",
        options: {
          colorize: true,
          translateTime: "SYS:standard",
          ignore: "pid,hostname"
        }
      }
    })
  });
}
