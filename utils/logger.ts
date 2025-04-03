import { v4 as uuidv4 } from "uuid";

export enum LogLevel {
    DEBUG = "DEBUG",
    INFO = "INFO",
    WARN = "WARN",
    ERROR = "ERROR",
}

interface LogContext {
    requestId?: string;
    userId?: string;
    nodeId?: string;
    relationId?: string;
    transactionId?: string;
    duration?: number;
    [key: string]: unknown;
}

class Logger {
    private static instance: Logger;
    private requestId: string;

    private constructor() {
        this.requestId = uuidv4();
    }

    public static getInstance(): Logger {
        if (!Logger.instance) {
            Logger.instance = new Logger();
        }
        return Logger.instance;
    }

    private formatMessage(
        level: LogLevel,
        message: string,
        context: LogContext = {}
    ): string {
        const timestamp = new Date().toISOString();
        const baseContext = {
            timestamp,
            level,
            requestId: this.requestId,
            ...context,
        };

        return JSON.stringify({
            ...baseContext,
            message,
        });
    }

    public debug(message: string, context: LogContext = {}): void {
        console.debug(this.formatMessage(LogLevel.DEBUG, message, context));
    }

    public info(message: string, context: LogContext = {}): void {
        console.info(this.formatMessage(LogLevel.INFO, message, context));
    }

    public warn(message: string, context: LogContext = {}): void {
        console.warn(this.formatMessage(LogLevel.WARN, message, context));
    }

    public error(message: string, context: LogContext = {}): void {
        console.error(this.formatMessage(LogLevel.ERROR, message, context));
    }

    public setRequestId(requestId: string): void {
        this.requestId = requestId;
    }

    public withContext(context: LogContext): Logger {
        const newLogger = Logger.getInstance();
        newLogger.requestId = context.requestId || this.requestId;
        return newLogger;
    }
}

export const logger = Logger.getInstance();
