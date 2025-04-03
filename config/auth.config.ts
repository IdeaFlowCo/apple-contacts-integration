import { z, ZodError } from "zod";

const AuthConfigSchema = z.object({
    baseUrl: z.string().url(),
    baseNodeUrl: z.string().url(),
    auth0Domain: z.string(),
    auth0ClientId: z.string(),
    auth0ClientSecret: z.string(),
    auth0Audience: z.string().url(),
});

type AuthConfig = z.infer<typeof AuthConfigSchema>;

function validateConfig(config: unknown): AuthConfig {
    try {
        return AuthConfigSchema.parse(config);
    } catch (error) {
        if (error instanceof ZodError) {
            const missingFields = error.errors
                .map((err: z.ZodIssue) => err.path.join("."))
                .join(", ");
            throw new Error(
                `Invalid configuration. Missing or invalid fields: ${missingFields}`
            );
        }
        throw error;
    }
}

export const AUTH_CONFIG = validateConfig({
    baseUrl: process.env.MEW_BASE_URL || "https://mew-edge.ideaflow.app/api",
    baseNodeUrl:
        process.env.MEW_BASE_NODE_URL || "https://mew-edge.ideaflow.app/",
    auth0Domain:
        process.env.MEW_AUTH0_DOMAIN || "ideaflow-mew-dev.us.auth0.com",
    auth0ClientId:
        process.env.MEW_AUTH0_CLIENT_ID || "zbhouY8SmHtIIJSjt1gu8TR3FgMsgo3J",
    auth0ClientSecret:
        process.env.MEW_AUTH0_CLIENT_SECRET ||
        "x0SAiFCCMwfgNEzU29KFh3TR4sTWuQVDqrRwBWCe0KsbA7WEd-1Ypatb47LCQ_Xb",
    auth0Audience:
        process.env.MEW_AUTH0_AUDIENCE ||
        "https://ideaflow-mew-dev.us.auth0.com/api/v2/",
});
