export const AUTH_CONFIG = {
    baseUrl: "https://mew-edge.ideaflow.app/api",
    baseNodeUrl: "https://mew-edge.ideaflow.app/",
    auth0Domain: "ideaflow-mew-dev.us.auth0.com",
    auth0ClientId: "zbhouY8SmHtIIJSjt1gu8TR3FgMsgo3J",
    auth0ClientSecret:
        "x0SAiFCCMwfgNEzU29KFh3TR4sTWuQVDqrRwBWCe0KsbA7WEd-1Ypatb47LCQ_Xb",
    auth0Audience: "https://ideaflow-mew-dev.us.auth0.com/api/v2/",
};

/* MewService.ts - Tailored for our conversation integration project */

import fetch from "node-fetch";
import crypto from "crypto";
import { logger } from "./utils/logger.js";
import { Cache } from "./utils/cache.js";
import { RequestQueue } from "./utils/requestQueue.js";
import {
    MewAPIError,
    AuthenticationError,
    NodeOperationError,
} from "./types/errors.js";

interface AuthTokenResponse {
    access_token: string;
    // Add other expected properties if needed, e.g., expires_in, token_type
}

export interface ConversationNode {
    id: string;
    parentNodeId: string;
    text: string;
    createdAt: string;
}

/**
 * Represents the types of content a Mew node can primarily consist of.
 * Used to structure the `content` array for API calls.
 */
export enum NodeContentType {
    Text = "text",
    Replacement = "replacement",
    Mention = "mention",
}

/** Represents data needed for a replacement-type node. */
export interface ReplacementNodeData {
    referenceNodeId: string;
    referenceCanonicalRelationId: string;
}

export interface MentionData {
    preMentionText: string;
    postMentionText: string;
    mentionNodeId: string;
}

/** Union type representing the simplified input for node content creation. */
export type NodeContent =
    | { type: NodeContentType.Text; text: string }
    | {
          type: NodeContentType.Replacement;
          replacementNodeData: ReplacementNodeData;
      }
    | { type: NodeContentType.Mention; mentionData: MentionData };

/**
 * Utility function to format various input content types into the
 * structured array format expected by the Mew API's `node.content` field.
 * @param content The input content (string, object, or already formatted array).
 * @returns {object[]} The formatted content array.
 */
export function createNodeContent(content: any) {
    // If content is already in the correct format, return it
    if (Array.isArray(content)) {
        return content;
    }

    // Handle our NodeContent type
    if (content.type === NodeContentType.Text) {
        return [{ type: "text", value: content.text, styles: 0 }];
    } else if (content.type === "text" && content.text) {
        // Handle the format coming from mewClipper
        return [{ type: "text", value: content.text, styles: 0 }];
    } else if (content.type === NodeContentType.Mention) {
        return [
            {
                type: "text",
                value: content.mentionData.preMentionText,
                styles: 0,
            },
            {
                type: "mention",
                value: content.mentionData.mentionNodeId,
                mentionTrigger: "@",
            },
            {
                type: "text",
                value: content.mentionData.postMentionText,
                styles: 0,
            },
        ];
    } else if (content.type === NodeContentType.Replacement) {
        return [{ type: "text", value: "replacement", styles: 0 }];
    }

    // Default case
    return [{ type: "text", value: "", styles: 0 }];
}

/**
 * Provides methods for interacting with the Mew API.
 * Handles authentication, node creation/updates, relation management, and data fetching.
 */
export class MewAPI {
    private baseUrl: string;
    private baseNodeUrl: string;
    private token: string;
    private currentUserRootNodeId: string;
    private authorId: string;
    private tokenCache: Cache<string>;
    private requestQueue: RequestQueue;

    /** Initializes the API service with base URLs and empty IDs. */
    constructor() {
        // Use the base URL from our AUTH_CONFIG
        this.baseUrl = AUTH_CONFIG.baseUrl;
        this.baseNodeUrl = AUTH_CONFIG.baseNodeUrl;
        this.token = "";
        this.currentUserRootNodeId = "";
        this.authorId = "";
        this.tokenCache = new Cache<string>(4 * 60 * 1000); // 4 minutes TTL for tokens
        this.requestQueue = new RequestQueue(10, 100, 50); // 10 batch size, 100ms max delay, 50 req/s rate limit
    }

    /**
     * Sets the User's Root Node ID for the current session.
     * This ID is typically parsed from the user root URL and used for constructing URLs.
     * @param userRootNodeId The Mew User Root Node ID (e.g., user-root-id-google-oauth2|...).
     */
    public setCurrentUserRootNodeId(userRootNodeId: string): void {
        this.currentUserRootNodeId = userRootNodeId;
    }

    /**
     * Sets the Author ID for the current session.
     * This ID is the actual user identifier (e.g., google-oauth2|...) used for ownership.
     * @param authorId The Mew Author ID.
     */
    public setAuthorId(authorId: string): void {
        if (!authorId) {
            console.warn(
                "[MewAPI] setAuthorId called with empty or invalid ID."
            );
        }
        this.authorId = authorId;
    }

    /**
     * Gets the currently set User Root Node ID.
     * @returns An object containing the current user root node ID, or empty if not set.
     */
    public getCurrentUserRootNodeInfo(): { id: string } {
        return { id: this.currentUserRootNodeId };
    }

    /**
     * Gets the currently set Author ID.
     * @returns {string} The author ID, or empty if not set.
     */
    public getAuthorId(): string {
        return this.authorId;
    }

    /** Generates a UUID v4. */
    private uuid(): string {
        return crypto.randomUUID();
    }

    /**
     * Retrieves or refreshes the Auth0 access token using client credentials.
     * Stores the token internally for subsequent API calls.
     * @returns {Promise<string>} The fetched access token.
     * @throws {Error} If authentication fails.
     */
    async getAccessToken(): Promise<string> {
        const cachedToken = this.tokenCache.get("auth_token");
        if (cachedToken) {
            return cachedToken;
        }

        try {
            const response = await fetch(
                `https://${AUTH_CONFIG.auth0Domain}/oauth/token`,
                {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify({
                        client_id: AUTH_CONFIG.auth0ClientId,
                        client_secret: AUTH_CONFIG.auth0ClientSecret,
                        audience: AUTH_CONFIG.auth0Audience,
                        grant_type: "client_credentials",
                    }),
                }
            );

            if (!response.ok) {
                throw new AuthenticationError(
                    `Auth failed: ${response.statusText}`,
                    response.status,
                    await response.text()
                );
            }

            const data = (await response.json()) as AuthTokenResponse;
            this.token = data.access_token;
            this.tokenCache.set("auth_token", this.token);
            return this.token;
        } catch (error) {
            if (error instanceof AuthenticationError) {
                throw error;
            }
            throw new AuthenticationError(
                `Failed to get access token: ${
                    error instanceof Error ? error.message : "Unknown error"
                }`
            );
        }
    }

    /**
     * Updates an existing Mew node with the provided partial data.
     * Fetches the existing node to construct the `oldProps` for the transaction.
     * @param nodeId The ID of the node to update.
     * @param updates An object containing the properties to update.
     * @throws {Error} If the node is not found or the update API call fails.
     */
    async updateNode(
        nodeId: string,
        updates: Partial<GraphNode>
    ): Promise<void> {
        const startTime = Date.now();
        try {
            const token = await this.getAccessToken();
            const transactionId = this.uuid();
            const timestamp = Date.now();

            const layerData = await this.getLayerData([nodeId]);
            const existingNode = layerData.data.nodesById[nodeId] as GraphNode;

            if (!existingNode) {
                throw new NodeOperationError(
                    `Node with ID ${nodeId} not found.`,
                    nodeId
                );
            }

            const updatePayload = {
                operation: "updateNode",
                oldProps: {
                    ...existingNode,
                    content: createNodeContent(existingNode.content),
                    updatedAt: existingNode.updatedAt,
                },
                newProps: {
                    ...existingNode,
                    ...updates,
                    content: updates.content
                        ? createNodeContent(updates.content)
                        : createNodeContent(existingNode.content),
                    id: nodeId,
                    authorId: existingNode.authorId,
                    createdAt: existingNode.createdAt,
                    updatedAt: timestamp,
                },
            };

            const payload = {
                clientId: AUTH_CONFIG.auth0ClientId,
                userId: this.authorId,
                transactionId: transactionId,
                updates: [updatePayload],
            };

            const response = await this.requestQueue.enqueue(() =>
                fetch(`${this.baseUrl}/sync`, {
                    method: "POST",
                    headers: {
                        Authorization: `Bearer ${token}`,
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify(payload),
                })
            );

            if (!response.ok) {
                const responseText = await response.text();
                throw new NodeOperationError(
                    `Failed to update node ${nodeId}: ${response.statusText}`,
                    nodeId,
                    response.status,
                    responseText
                );
            }

            logger.log("Node updated successfully", {
                nodeId,
                duration: Date.now() - startTime,
            });
        } catch (error) {
            logger.error("Failed to update node", {
                nodeId,
                duration: Date.now() - startTime,
                error: error instanceof Error ? error.message : "Unknown error",
            });
            throw error;
        }
    }

    /**
     * Deletes a Mew node.
     * Note: Currently only deletes the node itself. Associated relations might need explicit deletion.
     * @param nodeId The ID of the node to delete.
     * @throws {Error} If the deletion API call fails.
     */
    async deleteNode(nodeId: string): Promise<void> {
        const token = await this.getAccessToken();
        const transactionId = this.uuid();

        // Fetch the node data primarily to ensure it exists and potentially
        // get oldProps if needed by the API, though delete often doesn't require oldProps.
        const layerData = await this.getLayerData([nodeId]);
        const existingNode = layerData.data.nodesById[nodeId] as GraphNode;

        if (!existingNode) {
            console.warn(
                `[MewAPI] Node with ID ${nodeId} not found for deletion. Skipping.`
            );
            return;
        }

        const deletePayload = {
            operation: "deleteNode",
            node: {
                // Typically, only the ID is strictly required for deletion
                id: nodeId,
            },
        };

        // TODO: Investigate if related relations (parent-child, type)
        // and relation list entries need explicit deletion operations.
        // For now, we only delete the node itself.

        const payload = {
            clientId: AUTH_CONFIG.auth0ClientId,
            userId: this.authorId,
            transactionId: transactionId,
            updates: [deletePayload],
        };

        const txResponse = await fetch(`${this.baseUrl}/sync`, {
            method: "POST",
            headers: {
                Authorization: `Bearer ${token}`,
                "Content-Type": "application/json",
            },
            body: JSON.stringify(payload),
        });

        if (!txResponse.ok) {
            const responseText = await txResponse.text();
            const errMsg = `Failed to delete node ${nodeId}: Status ${txResponse.status} ${txResponse.statusText}. Response: ${responseText}`;
            console.error(errMsg);
            console.error("Request payload was:", payload);
            throw new Error(errMsg);
        }

        console.log(`Node ${nodeId} deleted successfully.`);
    }

    /**
     * Adds a single Mew node, optionally linking it as a child of a parent
     * and adding a type relation based on a label.
     * Constructs and executes a single transaction for all related operations.
     * @param input Object containing node details (content, parentId, label, etc.).
     * @returns {Promise<object>} An object containing IDs of the created node and relations.
     * @throws {Error} If the API call fails.
     */
    async addNode(input: {
        content: any;
        parentNodeId?: string;
        relationLabel?: string;
        isChecked?: boolean;
        authorId?: string;
        isPublic?: boolean;
    }): Promise<{
        newNodeId: string;
        newRelationLabelNodeId: string;
        parentChildRelationId: string;
        referenceNodeId: string;
        referenceCanonicalRelationId: string;
        isChecked?: boolean;
    }> {
        const {
            content,
            parentNodeId,
            relationLabel,
            isChecked,
            authorId,
            isPublic = true,
        } = input;
        const nodeContent = createNodeContent(content);
        const usedAuthorId = authorId ?? this.authorId;
        const newNodeId = this.uuid();
        const parentChildRelationId = this.uuid();
        const transactionId = this.uuid();
        const timestamp = Date.now();
        let relationLabelNodeId = "";

        const updates: any[] = [];

        // Step 1: Add the new node.
        updates.push({
            operation: "addNode",
            node: {
                version: 1,
                id: newNodeId,
                authorId: usedAuthorId,
                createdAt: timestamp,
                updatedAt: timestamp,
                content: nodeContent,
                isPublic: isPublic,
                isNewRelatedObjectsPublic: false,
                canonicalRelationId: parentNodeId
                    ? parentChildRelationId
                    : null,
                isChecked: isChecked ?? null,
            },
        });

        // Step 2: If a parent is provided, establish the child relation.
        if (parentNodeId) {
            updates.push({
                operation: "addRelation",
                relation: {
                    version: 1,
                    id: parentChildRelationId,
                    authorId: usedAuthorId,
                    createdAt: timestamp,
                    updatedAt: timestamp,
                    fromId: parentNodeId,
                    toId: newNodeId,
                    relationTypeId: "child",
                    isPublic: isPublic,
                    canonicalRelationId: null,
                },
                fromPos: { int: timestamp, frac: "a0" },
                toPos: { int: timestamp, frac: "a0" },
            });
            // Single updateRelationList operation for the parent-child relation
            updates.push({
                operation: "updateRelationList",
                relationId: parentChildRelationId,
                oldPosition: null,
                newPosition: { int: timestamp, frac: "a0" },
                authorId: usedAuthorId,
                type: "all",
                oldIsPublic: isPublic,
                newIsPublic: isPublic,
                nodeId: parentNodeId,
                relatedNodeId: newNodeId,
            });
        }

        // Step 3: Optionally create a relation label node if a relationLabel is provided.
        if (relationLabel) {
            relationLabelNodeId = this.uuid();
            updates.push({
                operation: "addNode",
                node: {
                    version: 1,
                    id: relationLabelNodeId,
                    authorId: usedAuthorId,
                    createdAt: timestamp,
                    updatedAt: timestamp,
                    content: [
                        { type: "text", value: relationLabel, styles: 0 },
                    ],
                    isPublic: isPublic,
                    isNewRelatedObjectsPublic: false,
                    canonicalRelationId: null,
                    isChecked: null,
                },
            });
            const newRelationTypeId = this.uuid();
            updates.push({
                operation: "addRelation",
                relation: {
                    version: 1,
                    id: newRelationTypeId,
                    authorId: usedAuthorId,
                    createdAt: timestamp,
                    updatedAt: timestamp,
                    fromId: parentChildRelationId,
                    toId: relationLabelNodeId,
                    relationTypeId: "__type__",
                    isPublic: isPublic,
                    canonicalRelationId: null,
                },
                fromPos: { int: timestamp, frac: "a0" },
                toPos: { int: timestamp, frac: "a0" },
            });
            updates.push({
                operation: "updateRelationList",
                relationId: newRelationTypeId,
                oldPosition: null,
                newPosition: { int: timestamp, frac: "a0" },
                authorId: usedAuthorId,
                type: "all",
                oldIsPublic: isPublic,
                newIsPublic: isPublic,
                nodeId: parentChildRelationId,
                relatedNodeId: relationLabelNodeId,
            });
            // Update the original child relation to reference the new relation type
            updates.push({
                operation: "updateRelation",
                oldProps: {
                    version: 1,
                    id: parentChildRelationId,
                    authorId: usedAuthorId,
                    createdAt: timestamp,
                    updatedAt: timestamp,
                    fromId: parentNodeId,
                    toId: newNodeId,
                    relationTypeId: "child",
                    isPublic: isPublic,
                    canonicalRelationId: null,
                },
                newProps: {
                    version: 1,
                    id: parentChildRelationId,
                    authorId: usedAuthorId,
                    createdAt: timestamp,
                    updatedAt: timestamp,
                    fromId: parentNodeId,
                    toId: newNodeId,
                    relationTypeId: "child",
                    isPublic: isPublic,
                    canonicalRelationId: newRelationTypeId,
                },
            });
        }

        // Step 4: If the content type is Replacement, update the parent-child relation accordingly.
        if (content?.type === "Replacement" && content.replacementNodeData) {
            updates.push({
                operation: "updateRelation",
                oldProps: {
                    version: 1,
                    id: parentChildRelationId,
                    authorId: usedAuthorId,
                    createdAt: timestamp,
                    updatedAt: timestamp,
                    fromId: parentNodeId,
                    toId: newNodeId,
                    relationTypeId: "child",
                    isPublic: isPublic,
                    canonicalRelationId: null,
                },
                newProps: {
                    version: 1,
                    id: parentChildRelationId,
                    authorId: usedAuthorId,
                    createdAt: timestamp,
                    updatedAt: timestamp,
                    fromId: parentNodeId,
                    toId: content.replacementNodeData.referenceNodeId,
                    relationTypeId: "child",
                    isPublic: isPublic,
                    canonicalRelationId:
                        content.replacementNodeData
                            .referenceCanonicalRelationId,
                },
            });
            updates.push({
                operation: "updateRelationList",
                relationId: parentChildRelationId,
                oldPosition: null,
                newPosition: { int: timestamp, frac: "a0" },
                authorId: usedAuthorId,
                type: "all",
                oldIsPublic: isPublic,
                newIsPublic: isPublic,
                nodeId: parentNodeId,
                relatedNodeId: content.replacementNodeData.referenceNodeId,
            });
        }

        // Step 5: Execute one transaction with all updates.
        const token = await this.getAccessToken();
        const payload = {
            clientId: AUTH_CONFIG.auth0ClientId,
            userId: usedAuthorId,
            transactionId: transactionId,
            updates: updates,
        };

        const txResponse = await this.requestQueue.enqueue(() =>
            fetch(`${this.baseUrl}/sync`, {
                method: "POST",
                headers: {
                    Authorization: `Bearer ${token}`,
                    "Content-Type": "application/json",
                },
                body: JSON.stringify(payload),
            })
        );

        if (!txResponse.ok) {
            const responseText = await txResponse.text();
            const errMsg = `Failed to add node: Status ${txResponse.status} ${txResponse.statusText}. Response: ${responseText}`;
            console.error(errMsg);
            console.error("Request payload was:", payload);
            throw new Error(errMsg);
        }

        if (txResponse.ok && isChecked) {
            // Optionally update the node's isChecked status.
            // await this.updateNode(newNodeId, { isChecked: true });
        }

        return {
            newNodeId,
            newRelationLabelNodeId: relationLabelNodeId,
            parentChildRelationId,
            referenceNodeId:
                content?.type === "Replacement" && content.replacementNodeData
                    ? content.replacementNodeData.referenceNodeId
                    : "",
            referenceCanonicalRelationId:
                content?.type === "Replacement" && content.replacementNodeData
                    ? content.replacementNodeData.referenceCanonicalRelationId
                    : "",
            isChecked: isChecked ?? undefined,
        };
    }

    /**
     * Fetches the base synchronization data for the user.
     * (Currently unused in the contacts sync script).
     * @returns {Promise<any>} The sync data payload.
     * @throws {Error} If the API call fails.
     */
    async syncData(): Promise<any> {
        const token = await this.getAccessToken();
        const response = await fetch(`${this.baseUrl}/sync`, {
            method: "GET",
            headers: {
                Authorization: `Bearer ${token}`,
                "Content-Type": "application/json",
            },
        });
        if (!response.ok) {
            throw new Error(`Failed to sync data: ${response.statusText}`);
        }
        return response.json();
    }

    /**
     * Fetches detailed data for a list of specified object IDs (nodes or relations).
     * @param objectIds An array of node or relation IDs.
     * @returns {Promise<any>} The layer data payload containing details about the requested objects and related entities.
     * @throws {Error} If the API call fails.
     */
    async getLayerData(objectIds: string[]): Promise<SyncResponse> {
        const startTime = Date.now();
        try {
            const token = await this.getAccessToken();
            const response = await this.requestQueue.enqueue(() =>
                fetch(`${this.baseUrl}/layer`, {
                    method: "POST",
                    headers: {
                        Authorization: `Bearer ${token}`,
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify({ objectIds }),
                })
            );

            if (!response.ok) {
                throw new MewAPIError(
                    `Failed to fetch layer data: ${response.statusText}`,
                    response.status,
                    await response.text()
                );
            }

            const layerData = (await response.json()) as SyncResponse;
            logger.log("Layer data fetched successfully", {
                objectIds,
                duration: Date.now() - startTime,
            });
            return layerData;
        } catch (error) {
            logger.error("Failed to fetch layer data", {
                objectIds,
                duration: Date.now() - startTime,
                error: error instanceof Error ? error.message : "Unknown error",
            });
            throw error;
        }
    }

    /**
     * Finds the first child node under a given parent that has an exact text match.
     * Useful for finding nodes like the "My Contacts" folder by name.
     * @param params Object containing parentNodeId and nodeText to search for.
     * @returns {Promise<GraphNode | undefined>} The matching GraphNode or undefined if not found.
     */
    async findNodeByText({
        parentNodeId,
        nodeText,
    }: {
        parentNodeId: string;
        nodeText: string;
    }) {
        const { parentNode, childNodes } = await this.getChildNodes({
            parentNodeId,
        });
        logger.log(
            "[MewService] findNodeByText: searching for exact text match:",
            nodeText
        );
        logger.log("[MewService] findNodeByText: child nodes content:", {
            count: childNodes.length,
            nodes: childNodes
                .filter((node) => node)
                .map((node) => ({
                    id: node.id,
                    textValue: getNodeTextContent(node) ?? "[No text content]",
                })),
        });

        const node = childNodes.find(
            (node) =>
                node &&
                node.content &&
                node.content.length > 0 &&
                node.content[0].value === nodeText
        );

        logger.log("[MewService] findNodeByText: found node:", {
            searchedFor: nodeText,
            foundNodeText: node ? getNodeTextContent(node) : "[Not found]",
            foundNodeId: node?.id ?? null,
        });

        return node;
    }

    /**
     * Retrieves the direct child nodes of a given parent node.
     * Fetches layer data for the parent and filters relations to find children.
     * @param params Object containing parentNodeId.
     * @returns {Promise<{ parentNode: GraphNode; childNodes: GraphNode[] }>} An object containing the parent node data
     *          and an array of its direct child nodes.
     */
    async getChildNodes({
        parentNodeId,
    }: {
        parentNodeId: string;
    }): Promise<{ parentNode: GraphNode; childNodes: GraphNode[] }> {
        const layerData = await this.getLayerData([parentNodeId]);

        // Extra logging for the root node case
        // if (parentNodeId.startsWith('user-root-id')) {
        //     logger.log("[MewService] getChildNodes: Raw relationsById for root node", {
        //         parentNodeId: parentNodeId,
        //         relations: layerData.data.relationsById
        //     });
        // }

        const parentNode = layerData.data.nodesById[parentNodeId];

        const childRelations = Object.values(
            layerData.data.relationsById
        ).filter(
            (relation): relation is Relation =>
                relation !== null &&
                typeof relation === "object" &&
                "fromId" in relation &&
                "toId" in relation &&
                "relationTypeId" in relation &&
                relation.fromId === parentNodeId &&
                relation.relationTypeId === "child"
        );

        const childNodes = childRelations.map((relation) => {
            const nodeData = layerData.data.nodesById[relation.toId];
            return nodeData;
        });

        return {
            parentNode,
            childNodes,
        };
    }

    /**
     * Constructs the web URL for a given Mew node ID.
     * @param nodeId The Mew Node ID.
     * @returns {string} The full URL to view the node in the Mew web interface.
     */
    getNodeUrl(nodeId: string): string {
        // Note: This URL structure might be specific to the environment/user setup.
        // It seems tailored to construct a URL based on the current user's structure.
        // Consider if a simpler `${this.baseNodeUrl}n/${nodeId}` format might be more general.
        // return `${this.baseNodeUrl}n/${nodeId}`; // Simpler alternative?

        // Current implementation assumes a specific path structure including the user ID
        if (!this.currentUserRootNodeId) {
            console.warn(
                "[MewAPI] getNodeUrl called before currentUserRootNodeId is set. URL might be incorrect."
            );
            // Fallback or throw error? For now, construct a potentially incomplete URL.
            return `${this.baseNodeUrl}g/all/global-root-to-users/all/users-to-user-relation-id-unknown/user-root-id-unknown`;
        }
        // Construct the URL using the known base and the user ID pattern
        // This assumes the user ID correctly represents the 'google-oauth2|...' part.
        return `${this.baseNodeUrl}g/all/global-root-to-users/all/users-to-user-relation-id-${this.currentUserRootNodeId}/user-root-id-${this.currentUserRootNodeId}`;
        // Original hardcoded example: return `${this.baseNodeUrl}g/all/global-root-to-users/all/users-to-user-relation-id/${nodeId}`;
        // This original example seems incorrect as it used the target nodeId in the user path part.
    }

    /**
     * Adds multiple contact nodes and their appleContactId properties in a single transaction.
     * Designed to efficiently create multiple contacts retrieved from an external source.
     * @param contactsToAdd Array of objects, each containing the identifier (Apple ID) and display name for a contact.
     * @param parentNodeId The Mew Node ID of the parent folder (e.g., "My Contacts") where contacts will be created.
     * @param isPublic Optional boolean flag to set the privacy of created nodes/relations. Defaults to true (public).
     * @returns {Promise<Map<string, string>>} A Map where keys are Apple Identifiers and values are the newly created Mew Node IDs.
     * @throws {Error} If the batch API call fails.
     */
    async batchAddContacts(
        contactsToAdd: AppleContact[],
        parentNodeId: string,
        isPublic: boolean = true
    ): Promise<Map<string, string>> {
        console.log(
            `[MewAPI] Starting batchAddContacts with ${contactsToAdd.length} contacts. isPublic=${isPublic}`
        );
        console.log(`[MewAPI] Parent node ID: ${parentNodeId}`);

        const token = await this.getAccessToken();
        console.log(
            `[MewAPI] Token retrieved successfully: ${token ? "Yes" : "No"}`
        );

        const transactionId = this.uuid();
        const timestamp = Date.now();
        const authorIdForBatch = this.authorId;
        if (!authorIdForBatch) {
            console.error(
                "[MewAPI] batchAddContacts called before authorId is set. Aborting."
            );
            throw new Error("Author ID not set in MewAPI");
        }
        console.log(`[MewAPI] Author ID for batch: ${authorIdForBatch}`);

        const updates: any[] = [];
        const createdContactsMap = new Map<string, string>();

        console.log(
            `[MewAPI] Preparing batch add for ${contactsToAdd.length} contacts under parent ${parentNodeId}`
        );

        for (const contact of contactsToAdd) {
            console.log(`[MewAPI] Processing contact: ${contact.identifier}`);
            const newNodeId = this.uuid();
            const parentChildRelationId = this.uuid();

            // Reconstruct the display name assignment carefully
            let calculatedName = `${contact.givenName || ""} ${
                contact.familyName || ""
            }`.trim();
            const displayName =
                calculatedName || contact.organizationName || "Unnamed Contact";
            console.log(`[MewAPI] Contact display name: ${displayName}`);

            createdContactsMap.set(contact.identifier, newNodeId);

            // 1. Add the main contact node
            const nodeContent = createNodeContent({
                type: NodeContentType.Text,
                text: displayName,
            });
            console.log(`[MewAPI] Node content created:`, nodeContent);

            updates.push({
                operation: "addNode",
                node: {
                    version: 1,
                    id: newNodeId,
                    authorId: authorIdForBatch,
                    createdAt: timestamp,
                    updatedAt: timestamp,
                    content: nodeContent,
                    isPublic: isPublic,
                    isNewRelatedObjectsPublic: false,
                    canonicalRelationId: parentChildRelationId,
                    isChecked: null,
                },
            });

            // 2. Add the parent-child relation
            const relation = {
                operation: "addRelation",
                relation: {
                    version: 1,
                    id: parentChildRelationId,
                    authorId: authorIdForBatch,
                    createdAt: timestamp,
                    updatedAt: timestamp,
                    fromId: parentNodeId,
                    toId: newNodeId,
                    relationTypeId: "child",
                    isPublic: isPublic,
                    canonicalRelationId: null,
                },
                fromPos: {
                    int: timestamp,
                    frac: crypto.randomBytes(4).toString("hex"),
                },
                toPos: {
                    int: timestamp,
                    frac: crypto.randomBytes(4).toString("hex"),
                },
            };
            console.log(`[MewAPI] Parent-child relation:`, relation);
            updates.push(relation);

            updates.push({
                operation: "updateRelationList",
                relationId: parentChildRelationId,
                oldPosition: null,
                newPosition: {
                    int: timestamp,
                    frac: crypto.randomBytes(4).toString("hex"),
                },
                authorId: authorIdForBatch,
                type: "all",
                oldIsPublic: isPublic,
                newIsPublic: isPublic,
                nodeId: parentNodeId,
                relatedNodeId: newNodeId,
            });

            // --- 3. Add the appleContactId property node and its relations ---
            const appleIdOps = this.generatePropertyOperations(
                newNodeId,
                "appleContactId",
                contact.identifier,
                authorIdForBatch,
                timestamp,
                isPublic
            );
            console.log(
                `[MewAPI] Generated ${appleIdOps.length} property operations for appleContactId`
            );
            updates.push(...appleIdOps);

            // --- 4. Add other properties (Phone, Email, Org, Note) ---
            const propertiesToSync = [
                { key: "phoneNumbers", baseLabel: "phone", array: true },
                { key: "emailAddresses", baseLabel: "email", array: true },
                {
                    key: "organizationName",
                    baseLabel: "organization",
                    array: false,
                },
                { key: "note", baseLabel: "note", array: false },
            ];

            for (const propInfo of propertiesToSync) {
                const data = contact[propInfo.key as keyof AppleContact];
                console.log(
                    `[MewAPI] Processing property ${propInfo.key}:`,
                    data
                );

                if (propInfo.array && Array.isArray(data)) {
                    const items = data as {
                        label?: string | null;
                        value: string;
                    }[];
                    for (const item of items) {
                        if (item.value) {
                            const sanitizedLabel = item.label
                                ? item.label
                                      .replace(/[^a-zA-Z0-9]/g, "_")
                                      .toLowerCase()
                                : "";
                            const relationLabel = sanitizedLabel
                                ? `${propInfo.baseLabel}_${sanitizedLabel}`
                                : propInfo.baseLabel;
                            const propOps = this.generatePropertyOperations(
                                newNodeId,
                                relationLabel,
                                item.value,
                                authorIdForBatch,
                                timestamp,
                                isPublic
                            );
                            console.log(
                                `[MewAPI] Generated ${propOps.length} property operations for ${relationLabel}`
                            );
                            updates.push(...propOps);
                        }
                    }
                } else if (
                    !propInfo.array &&
                    typeof data === "string" &&
                    data
                ) {
                    const propOps = this.generatePropertyOperations(
                        newNodeId,
                        propInfo.baseLabel,
                        data,
                        authorIdForBatch,
                        timestamp,
                        isPublic
                    );
                    console.log(
                        `[MewAPI] Generated ${propOps.length} property operations for ${propInfo.baseLabel}`
                    );
                    updates.push(...propOps);
                }
            }
        }

        console.log(`[MewAPI] Total operations generated: ${updates.length}`);

        if (updates.length === 0) {
            console.log("[MewAPI] No operations to send, returning empty map");
            return createdContactsMap;
        }

        // Execute the single transaction with all updates
        console.log(
            `[MewAPI] Sending batch transaction with ${updates.length} operations...`
        );
        const payload = {
            clientId: AUTH_CONFIG.auth0ClientId,
            userId: authorIdForBatch,
            transactionId: transactionId,
            updates: updates,
        };

        try {
            const txResponse = await this.requestQueue.enqueue(() =>
                fetch(`${this.baseUrl}/sync`, {
                    method: "POST",
                    headers: {
                        Authorization: `Bearer ${token}`,
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify(payload),
                })
            );

            console.log(`[MewAPI] API Response Status: ${txResponse.status}`);

            if (!txResponse.ok) {
                const responseText = await txResponse.text();
                console.error(`[MewAPI] API Error Response:`, responseText);
                throw new Error(
                    `Failed batch add contacts: Status ${txResponse.status} ${txResponse.statusText}. Response: ${responseText}`
                );
            }

            const responseJson = await txResponse.json();
            console.log(
                `[MewAPI] Batch add transaction successful. Created ${createdContactsMap.size} contacts`
            );
            return createdContactsMap;
        } catch (error) {
            console.error("[MewAPI] Error during batch add:", error);
            throw error;
        }
    }

    /**
     * Sends a batch of pre-constructed update operations to the /sync endpoint.
     * @param operations An array of operation objects (e.g., addNode, updateNode, deleteNode, addRelation).
     * @param transactionId Optional transaction ID. If not provided, a new one is generated.
     * @throws {Error} If the API call fails.
     */
    async sendBatchOperations(
        operations: any[],
        transactionId: string = this.uuid()
    ): Promise<any> {
        // Return the response JSON
        if (!operations || operations.length === 0) {
            console.log("[MewAPI] No operations provided for batch send.");
            return Promise.resolve({}); // Nothing to do
        }

        const token = await this.getAccessToken();
        const authorIdForBatch = this.authorId;
        if (!authorIdForBatch) {
            console.error(
                "[MewAPI] sendBatchOperations called before authorId is set. Aborting."
            );
            throw new Error("Author ID not set in MewAPI");
        }

        console.log(
            `[MewAPI] Sending batch transaction ${transactionId} with ${operations.length} operations using authorId: ${authorIdForBatch}...`
        );
        const payload = {
            clientId: AUTH_CONFIG.auth0ClientId,
            userId: authorIdForBatch,
            transactionId: transactionId,
            updates: operations, // Send the provided operations array
        };

        try {
            const txResponse = await this.requestQueue.enqueue(() =>
                fetch(`${this.baseUrl}/sync`, {
                    method: "POST",
                    headers: {
                        Authorization: `Bearer ${token}`,
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify(payload),
                })
            );

            if (!txResponse.ok) {
                const responseText = await txResponse.text();
                let detail = responseText;
                try {
                    const errorJson = JSON.parse(responseText);
                    detail = errorJson.detail || responseText;
                } catch (e) {
                    /* Ignore parsing error */
                }
                const errMsg = `Failed batch operations (TxID: ${transactionId}): Status ${txResponse.status} ${txResponse.statusText}. Detail: ${detail}`;
                console.error(errMsg);
                // console.error("Request payload was:", JSON.stringify(payload, null, 2)); // Careful logging large payloads
                throw new Error(errMsg);
            }

            const responseJson = await txResponse.json();
            console.log(
                `[MewAPI] Batch transaction ${transactionId} successful.`
                // Optionally log: Response:`, responseJson
            );
            return responseJson;
        } catch (error) {
            console.error(
                `[MewAPI] Error during batch send fetch operation (TxID: ${transactionId}):`,
                error
            );
            throw error;
        }
    }

    // --- Helper Operation Generators ---

    /**
     * Generates the Mew API operations needed to add a single property
     * (value node, relation, label node, type relation) to a parent node.
     * NOTE: Creates a new label node defensively.
     * @returns An array of Mew API operation objects.
     */
    public generatePropertyOperations(
        parentNodeId: string,
        relationLabel: string,
        value: string,
        authorIdToUse: string,
        timestamp: number,
        isPublic: boolean
    ): any[] {
        const updates: any[] = [];
        const propertyNodeId = this.uuid();
        const propertyTypeRelationId = this.uuid();
        const propertyRelationId = this.uuid();
        const labelNodeId = this.uuid(); // Create label node defensively
        const fracSuffix = crypto.randomBytes(4).toString("hex"); // Unique suffix for ordering

        // a. Add the node holding the property value
        updates.push({
            operation: "addNode",
            node: {
                version: 1,
                id: propertyNodeId,
                authorId: authorIdToUse,
                createdAt: timestamp,
                updatedAt: timestamp,
                content: createNodeContent({
                    type: NodeContentType.Text,
                    text: value,
                }),
                isPublic: isPublic,
                isNewRelatedObjectsPublic: false,
                canonicalRelationId: propertyRelationId,
                isChecked: null,
            },
        });

        // b. Add the relation for the property (Parent Node -> Value Node)
        updates.push({
            operation: "addRelation",
            relation: {
                version: 1,
                id: propertyRelationId,
                authorId: authorIdToUse,
                createdAt: timestamp,
                updatedAt: timestamp,
                fromId: parentNodeId,
                toId: propertyNodeId,
                relationTypeId: "child", // Generic type
                isPublic: isPublic,
                canonicalRelationId: propertyTypeRelationId, // Link to type relation
            },
            fromPos: { int: timestamp, frac: `a${fracSuffix}` },
            toPos: { int: timestamp, frac: `b${fracSuffix}` },
        });
        updates.push({
            operation: "updateRelationList",
            relationId: propertyRelationId,
            oldPosition: null,
            newPosition: { int: timestamp, frac: "a0" },
            authorId: authorIdToUse,
            type: "all",
            oldIsPublic: isPublic,
            newIsPublic: isPublic,
            nodeId: parentNodeId,
            relatedNodeId: propertyNodeId,
        });

        // c. Add the label node (e.g., "email_home")
        updates.push({
            operation: "addNode",
            node: {
                version: 1,
                id: labelNodeId,
                authorId: authorIdToUse,
                createdAt: timestamp,
                updatedAt: timestamp,
                content: createNodeContent({
                    type: NodeContentType.Text,
                    text: relationLabel,
                }),
                isPublic: isPublic,
                isNewRelatedObjectsPublic: false,
                canonicalRelationId: null,
                isChecked: null,
            },
        });

        // d. Add the __type__ relation (Property Relation -> Label Node)
        updates.push({
            operation: "addRelation",
            relation: {
                version: 1,
                id: propertyTypeRelationId,
                authorId: authorIdToUse,
                createdAt: timestamp,
                updatedAt: timestamp,
                fromId: propertyRelationId,
                toId: labelNodeId,
                relationTypeId: "__type__",
                isPublic: isPublic,
                canonicalRelationId: null,
            },
            fromPos: { int: timestamp, frac: `c${fracSuffix}` },
            toPos: { int: timestamp, frac: `d${fracSuffix}` },
        });
        updates.push({
            operation: "updateRelationList",
            relationId: propertyTypeRelationId,
            oldPosition: null,
            newPosition: { int: timestamp, frac: "c0" },
            authorId: authorIdToUse,
            type: "all",
            oldIsPublic: isPublic,
            newIsPublic: isPublic,
            nodeId: propertyRelationId,
            relatedNodeId: labelNodeId,
        });

        // Note: Step 3e (updateRelation for propertyRelationId) from previous logic is removed,
        // as we now set canonicalRelationId directly during the creation of propertyRelationId in step 3b.

        return updates;
    }

    /**
     * Generates the Mew API operation needed to update the content of an existing node.
     * Requires fetching the node's existing state to provide oldProps.
     * @returns A single updateNode operation object, or null if node data cannot be fetched.
     */
    public async _generateUpdateNodeContentOperation(
        nodeId: string,
        newTextValue: string,
        timestamp: number = Date.now()
    ): Promise<any | null> {
        // Fetch existing node data - Required for oldProps
        // Consider batching fetches if called frequently in a loop outside a batch transaction
        let existingNode: GraphNode;
        try {
            const layerData = await this.getLayerData([nodeId]);
            existingNode = layerData.data.nodesById[nodeId];
            if (!existingNode) {
                console.warn(
                    `[MewAPI._generateUpdate] Node ${nodeId} not found, cannot generate update op.`
                );
                return null;
            }
        } catch (fetchError) {
            console.error(
                `[MewAPI._generateUpdate] Error fetching node ${nodeId} for update:`,
                fetchError
            );
            return null;
        }

        const newContent = createNodeContent({
            type: "text",
            text: newTextValue,
        });

        // Avoid generating an update if content hasn't actually changed
        // Note: This requires getNodeTextContent or similar logic if content structure varies
        const existingText = getNodeTextContent(existingNode);
        if (existingText === newTextValue) {
            // console.log(`[MewAPI._generateUpdate] Content for node ${nodeId} unchanged, skipping update op.`);
            return null; // No operation needed
        }

        return {
            operation: "updateNode",
            oldProps: {
                ...existingNode,
                content: createNodeContent(existingNode.content), // Ensure old content is formatted
                updatedAt: existingNode.updatedAt, // Use existing timestamp for oldProps
            },
            newProps: {
                ...existingNode,
                content: newContent,
                updatedAt: timestamp, // Update timestamp for newProps
            },
        };
    }

    /**
     * Generates the Mew API operation needed to delete a node.
     * @returns A single deleteNode operation object.
     */
    public _generateDeleteNodeOperation(nodeId: string): any {
        // TODO: Enhance to also generate operations to delete related relations?
        // This is complex as it requires knowing which relations should be cleaned up.
        return {
            operation: "deleteNode",
            node: { id: nodeId },
        };
    }
}

/**
 * Parses the Mew User ID from a user's root node URL.
 * Validates the URL format specific to the mew-edge.ideaflow.app structure.
 * Handles URL decoding, including pipe characters (%7C).
 * @param url The user root URL string.
 * @returns {string} The extracted Mew User ID.
 * @throws {Error} If the URL format is invalid.
 */
export const parseUserRootNodeIdFromUrl = (url: string): string => {
    const regex =
        /^https?:\/\/mew-edge\.ideaflow\.app\/g\/all\/global-root-to-users\/all\/users-to-user-relation-id-[^\/]+\/user-root-id-[^\/]+$/;
    if (!regex.test(url)) {
        throw new Error("Invalid user node URL format");
    }
    const urlParts = url.split("/");
    const lastPart = urlParts[urlParts.length - 1];

    // First handle any raw %7C or %7c that might be in the string
    let decoded = lastPart.replace(/%7C/gi, "|");
    // Then do a full URL decode to handle any other encoded characters
    decoded = decodeURIComponent(decoded);
    // Finally ensure any remaining encoded pipes are handled
    decoded = decoded.replace(/%7C/gi, "|");

    return decoded;
};

/** Represents the structure of a Mew Graph Node based on observed API responses. */
export interface GraphNode {
    version: number;
    id: string;
    authorId: string;
    createdAt: string;
    updatedAt: string;
    content: ContentBlock[];
    isPublic: boolean;
    isNewRelatedObjectsPublic: boolean;
    relationId: string | null;
    canonicalRelationId: string | null;
    isChecked: boolean | null;
}

/** Represents a single block of content within a node's `content` array. */
export interface ContentBlock {
    type: "text" | "mention"; // Could be expanded if there are other types
    value: string;
}

/** Represents the structure of a Mew Relation based on observed API responses. */
export interface Relation {
    id: string;
    version: number;
    authorId: string;
    createdAt: number;
    updatedAt: number;
    fromId: string;
    toId: string;
    relationTypeId: string;
    isPublic: boolean;
    canonicalRelationId: string | null;
}

/** Defines the expected structure for user data within API responses. */
interface User {
    id: string;
    username: string;
    email: string;
}

/** Defines the overall structure of the data returned by the /sync or /layer endpoints. */
interface SyncResponse {
    data: {
        usersById: {
            [key: string]: User;
        };
        nodesById: {
            [key: string]: GraphNode;
        };
        relationsById: {
            [key: string]: Relation;
        };
    };
}

/** Defines the structure of the response from the Auth0 token endpoint. */
interface TokenData {
    access_token: string;
    expires_in: number;
    token_type: string;
}

/**
 * Represents the structure of contact data expected from the source (Apple Contacts).
 * This interface defines the input structure for methods like batchAddContacts.
 */
export interface AppleContact {
    identifier: string; // CNContactIdentifierKey
    givenName?: string | null; // CNContactGivenNameKey
    familyName?: string | null; // CNContactFamilyNameKey
    organizationName?: string | null; // CNContactOrganizationNameKey
    phoneNumbers?: { label?: string | null; value: string }[] | null; // CNContactPhoneNumbersKey
    emailAddresses?: { label?: string | null; value: string }[] | null; // CNContactEmailAddressesKey
    note?: string | null; // CNContactNoteKey
    properties: Array<{
        type: string;
        value: string;
    }>;
}

export interface MewContact {
    id: string;
    properties: Array<{
        id: string;
        type: string;
        value: string;
    }>;
}

export type Operation = {
    type: "add" | "update" | "delete";
    mewId: string;
    mewUserRootUrl: string;
    propertyId?: string;
    property?: {
        type: string;
        value: string;
    };
};

// --- Utility Functions (Internal to MewService) ---

/**
 * Extracts the text value from a simple Mew text node's content array.
 * @param node The Mew GraphNode to extract text from.
 * @returns {string | null} The text value or null if not found/applicable.
 */
export function getNodeTextContent(
    node: GraphNode | null | undefined
): string | null {
    if (
        node &&
        node.content &&
        node.content.length > 0 &&
        node.content[0].type === "text"
    ) {
        return node.content[0].value;
    }
    return null;
}

// --- End Utility Functions ---
