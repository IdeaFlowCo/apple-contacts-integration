import fs from "fs";
import path from "path";

const LOG_DIR = "logs";
const LOG_FILE = path.join(LOG_DIR, "contact-sync.log");

// Ensure logs directory exists
if (!fs.existsSync(LOG_DIR)) {
    fs.mkdirSync(LOG_DIR);
}

// Create a write stream for the log file
const logStream = fs.createWriteStream(LOG_FILE, { flags: "a" });

export const logger = {
    log: (message: string, data?: any) => {
        const timestamp = new Date().toISOString();
        const logMessage = `[${timestamp}] ${message}${
            data ? "\n" + JSON.stringify(data, null, 2) : ""
        }\n`;

        // Write to console
        console.log(logMessage);

        // Write to file
        logStream.write(logMessage);
    },

    error: (message: string, error?: any) => {
        const timestamp = new Date().toISOString();
        const errorMessage = `[${timestamp}] ERROR: ${message}${
            error ? "\n" + JSON.stringify(error, null, 2) : ""
        }\n`;

        // Write to console
        console.error(errorMessage);

        // Write to file
        logStream.write(errorMessage);
    },
};
