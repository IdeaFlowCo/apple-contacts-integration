interface QueuedRequest<T> {
    operation: () => Promise<T>;
    resolve: (value: T) => void;
    reject: (error: Error) => void;
    timestamp: number;
}

export class RequestQueue {
    private queue: QueuedRequest<unknown>[] = [];
    private isProcessing: boolean = false;
    private readonly batchSize: number;
    private readonly maxDelay: number;
    private readonly rateLimit: number; // requests per second
    private lastRequestTime: number = 0;

    constructor(
        batchSize: number = 10,
        maxDelay: number = 100, // ms
        rateLimit: number = 50 // requests per second
    ) {
        this.batchSize = batchSize;
        this.maxDelay = maxDelay;
        this.rateLimit = rateLimit;
    }

    public async enqueue<T>(operation: () => Promise<T>): Promise<T> {
        return new Promise<T>((resolve, reject) => {
            const request: QueuedRequest<T> = {
                operation,
                resolve,
                reject,
                timestamp: Date.now(),
            };
            this.queue.push(request as QueuedRequest<unknown>);

            if (!this.isProcessing) {
                this.processQueue();
            }
        });
    }

    private async processQueue(): Promise<void> {
        if (this.isProcessing || this.queue.length === 0) {
            return;
        }

        this.isProcessing = true;

        try {
            while (this.queue.length > 0) {
                const batch = this.queue.splice(0, this.batchSize);
                const startTime = Date.now();

                // Process batch
                await Promise.all(
                    batch.map(async (request) => {
                        try {
                            // Rate limiting
                            const now = Date.now();
                            const timeSinceLastRequest =
                                now - this.lastRequestTime;
                            const minTimeBetweenRequests =
                                1000 / this.rateLimit;

                            if (timeSinceLastRequest < minTimeBetweenRequests) {
                                await new Promise((resolve) =>
                                    setTimeout(
                                        resolve,
                                        minTimeBetweenRequests -
                                            timeSinceLastRequest
                                    )
                                );
                            }

                            const result = await request.operation();
                            this.lastRequestTime = Date.now();
                            (request as QueuedRequest<unknown>).resolve(result);
                        } catch (error) {
                            request.reject(error as Error);
                        }
                    })
                );

                // Wait if needed to respect maxDelay
                const elapsed = Date.now() - startTime;
                if (elapsed < this.maxDelay) {
                    await new Promise((resolve) =>
                        setTimeout(resolve, this.maxDelay - elapsed)
                    );
                }
            }
        } finally {
            this.isProcessing = false;
        }
    }

    public getQueueSize(): number {
        return this.queue.length;
    }

    public clear(): void {
        this.queue.forEach((request) => {
            request.reject(new Error("Queue cleared"));
        });
        this.queue = [];
    }
}
