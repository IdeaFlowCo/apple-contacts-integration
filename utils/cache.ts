interface CacheEntry<T> {
    value: T;
    timestamp: number;
}

export class Cache<T> {
    private cache: Map<string, CacheEntry<T>>;
    private readonly ttl: number; // Time to live in milliseconds
    private readonly maxSize: number;

    constructor(ttl: number = 5 * 60 * 1000, maxSize: number = 1000) {
        this.cache = new Map();
        this.ttl = ttl;
        this.maxSize = maxSize;
    }

    public set(key: string, value: T): void {
        if (this.cache.size >= this.maxSize) {
            this.evictOldest();
        }

        this.cache.set(key, {
            value,
            timestamp: Date.now(),
        });
    }

    public get(key: string): T | null {
        const entry = this.cache.get(key);
        if (!entry) return null;

        if (Date.now() - entry.timestamp > this.ttl) {
            this.cache.delete(key);
            return null;
        }

        return entry.value;
    }

    public delete(key: string): void {
        this.cache.delete(key);
    }

    public clear(): void {
        this.cache.clear();
    }

    private evictOldest(): void {
        let oldestKey: string | null = null;
        let oldestTimestamp = Infinity;

        for (const [key, entry] of this.cache.entries()) {
            if (entry.timestamp < oldestTimestamp) {
                oldestTimestamp = entry.timestamp;
                oldestKey = key;
            }
        }

        if (oldestKey) {
            this.cache.delete(oldestKey);
        }
    }
}
