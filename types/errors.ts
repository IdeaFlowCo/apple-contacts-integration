export class MewAPIError extends Error {
    constructor(
        message: string,
        public readonly statusCode?: number,
        public readonly responseData?: unknown
    ) {
        super(message);
        this.name = "MewAPIError";
    }
}

export class AuthenticationError extends MewAPIError {
    constructor(message: string, statusCode?: number, responseData?: unknown) {
        super(message, statusCode, responseData);
        this.name = "AuthenticationError";
    }
}

export class NodeOperationError extends MewAPIError {
    constructor(
        message: string,
        public readonly nodeId: string,
        statusCode?: number,
        responseData?: unknown
    ) {
        super(message, statusCode, responseData);
        this.name = "NodeOperationError";
    }
}

export class RelationOperationError extends MewAPIError {
    constructor(
        message: string,
        public readonly relationId: string,
        statusCode?: number,
        responseData?: unknown
    ) {
        super(message, statusCode, responseData);
        this.name = "RelationOperationError";
    }
}

export class BatchOperationError extends MewAPIError {
    constructor(
        message: string,
        public readonly transactionId: string,
        public readonly failedOperations: unknown[],
        statusCode?: number,
        responseData?: unknown
    ) {
        super(message, statusCode, responseData);
        this.name = "BatchOperationError";
    }
}
