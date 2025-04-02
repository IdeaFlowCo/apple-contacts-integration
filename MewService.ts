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

export enum NodeContentType {
    Text = "text",
    Replacement = "replacement",
    Mention = "mention",
}

export interface ReplacementNodeData {
    referenceNodeId: string;
    referenceCanonicalRelationId: string;
}

export interface MentionData {
    preMentionText: string;
    postMentionText: string;
    mentionNodeId: string;
}

export type NodeContent =
    | { type: NodeContentType.Text; text: string }
    | {
          type: NodeContentType.Replacement;
          replacementNodeData: ReplacementNodeData;
      }
    | { type: NodeContentType.Mention; mentionData: MentionData };

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

export class MewAPI {
    private baseUrl: string;
    private baseNodeUrl: string;
    private token: string;
    private currentUserId: string;

    constructor() {
        // Use the base URL from our AUTH_CONFIG
        this.baseUrl = AUTH_CONFIG.baseUrl;
        this.baseNodeUrl = AUTH_CONFIG.baseNodeUrl;
        this.token = "";
        this.currentUserId = ""; // Will be set from user's root node URL
    }

    public setCurrentUserId(userId: string): void {
        this.currentUserId = userId;
    }

    public getCurrentUser(): { id: string } {
        return { id: this.currentUserId };
    }

    private uuid(): string {
        return crypto.randomUUID();
    }

    async getAccessToken(): Promise<string> {
        // Retrieve an access token using Auth0 credentials.
        try {
            const response = await fetch(
                `https://${AUTH_CONFIG.auth0Domain}/oauth/token`,
                {
                    method: "POST",
                    // mode: "cors", // Removed - not applicable for node-fetch
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
                throw new Error(`Auth failed: ${response.statusText}`);
            }

            const data = (await response.json()) as AuthTokenResponse; // Type assertion
            this.token = data.access_token;
        } catch (error: unknown) {
            throw error;
        }
        return this.token;
    }

    async updateNode(
        nodeId: string,
        updates: Partial<GraphNode>
    ): Promise<void> {
        const token = await this.getAccessToken();
        const transactionId = this.uuid();
        const timestamp = Date.now();
        const authorId = this.currentUserId; // Assume updates are made by the current user

        // Fetch the current node data to get its existing properties
        const layerData = await this.getLayerData([nodeId]);
        const existingNode = layerData.data.nodesById[nodeId] as GraphNode;

        if (!existingNode) {
            throw new Error(`Node with ID ${nodeId} not found.`);
        }

        // Prepare the update payload
        const updatePayload = {
            operation: "updateNode",
            oldProps: {
                ...existingNode,
                // Ensure content is in the expected format if not already
                content: createNodeContent(existingNode.content),
                updatedAt: existingNode.updatedAt, // Use existing timestamp
            },
            newProps: {
                ...existingNode,
                ...updates,
                // Ensure content is in the expected format
                content: updates.content
                    ? createNodeContent(updates.content)
                    : createNodeContent(existingNode.content),
                id: nodeId, // Ensure ID remains the same
                authorId: existingNode.authorId, // Keep original author
                createdAt: existingNode.createdAt, // Keep original creation time
                updatedAt: timestamp, // Update the timestamp
            },
        };

        const payload = {
            clientId: AUTH_CONFIG.auth0ClientId,
            userId: authorId,
            transactionId: transactionId,
            updates: [updatePayload],
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
            const errMsg = `Failed to update node ${nodeId}: Status ${txResponse.status} ${txResponse.statusText}. Response: ${responseText}`;
            console.error(errMsg);
            console.error("Request payload was:", payload);
            throw new Error(errMsg);
        }

        console.log(`Node ${nodeId} updated successfully.`);
    }

    async deleteNode(nodeId: string): Promise<void> {
        const token = await this.getAccessToken();
        const transactionId = this.uuid();
        const authorId = this.currentUserId;

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
            userId: authorId,
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

    async addNode(input: {
        content: any;
        parentNodeId?: string;
        relationLabel?: string;
        isChecked?: boolean;
        authorId?: string;
    }): Promise<{
        newNodeId: string;
        newRelationLabelNodeId: string;
        parentChildRelationId: string;
        referenceNodeId: string;
        referenceCanonicalRelationId: string;
        isChecked?: boolean;
    }> {
        const { content, parentNodeId, relationLabel, isChecked, authorId } =
            input;
        const nodeContent = createNodeContent(content);
        const usedAuthorId = authorId ?? this.currentUserId;
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
                isPublic: true,
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
                    isPublic: true,
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
                oldIsPublic: true,
                newIsPublic: true,
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
                    isPublic: true,
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
                    isPublic: true,
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
                oldIsPublic: true,
                newIsPublic: true,
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
                    isPublic: true,
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
                    isPublic: true,
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
                    isPublic: true,
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
                    isPublic: true,
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
                oldIsPublic: true,
                newIsPublic: true,
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

    async getLayerData(objectIds: string[]): Promise<any> {
        const token = await this.getAccessToken();
        const response = await fetch(`${this.baseUrl}/layer`, {
            method: "POST",
            headers: {
                Authorization: `Bearer ${token}`,
                "Content-Type": "application/json",
            },
            body: JSON.stringify({ objectIds }),
        });
        if (!response.ok) {
            throw new Error(
                `Failed to fetch layer data: ${response.statusText}`
            );
        }
        const layerData = await response.json();
        return layerData;
    }

    /**
     * Finds a node with exact text match under a parent node
     * @returns The matching node or undefined
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
        console.log(
            "findNodeByText: searching for exact text match:",
            nodeText
        );
        console.log(
            "findNodeByText: child nodes content:",
            childNodes
                .filter((node) => node)
                .map((node) => ({
                    id: node.id,
                    content: node.content,
                    textValue: node.content?.[0]?.value,
                }))
        );

        const node = childNodes.find(
            (node) =>
                node &&
                node.content &&
                node.content.length > 0 &&
                node.content[0].value === nodeText
        );

        console.log("findNodeByText: found node:", {
            searchedFor: nodeText,
            foundNodeContent: node?.content?.[0]?.value,
            node,
        });

        return node;
    }

    async getChildNodes({
        parentNodeId,
    }: {
        parentNodeId: string;
    }): Promise<{ parentNode: GraphNode; childNodes: GraphNode[] }> {
        const layerData = await this.getLayerData([parentNodeId]);

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

    getNodeUrl(nodeId: string): string {
        return `${this.baseUrl}/g/all/global-root-to-users/all/users-to-user-relation-id/${nodeId}`;
    }

    /**
     * Adds multiple contact nodes and their appleContactId properties in a single transaction.
     * @param contactsToAdd Array of objects containing data for contacts to add.
     * @param parentNodeId The Node ID of the parent folder (e.g., "My Contacts").
     * @returns A Map of Apple Identifier -> New Mew Node ID for the created contacts.
     */
    async batchAddContacts(
        contactsToAdd: {
            identifier: string; // Apple Contact Identifier
            displayName: string;
        }[],
        parentNodeId: string
    ): Promise<Map<string, string>> {
        const token = await this.getAccessToken();
        const transactionId = this.uuid();
        const timestamp = Date.now();
        const authorId = this.currentUserId;
        const updates: any[] = [];
        const createdContactsMap = new Map<string, string>();

        console.log(
            `[MewAPI] Preparing batch add for ${contactsToAdd.length} contacts under parent ${parentNodeId}`
        );

        for (const contact of contactsToAdd) {
            const newNodeId = this.uuid();
            const parentChildRelationId = this.uuid();
            const relationLabel = "appleContactId"; // Constant label
            const relationLabelValue = contact.identifier; // Apple ID as value

            createdContactsMap.set(contact.identifier, newNodeId);

            // 1. Add the main contact node
            updates.push({
                operation: "addNode",
                node: {
                    version: 1,
                    id: newNodeId,
                    authorId: authorId,
                    createdAt: timestamp,
                    updatedAt: timestamp,
                    content: createNodeContent({
                        type: NodeContentType.Text,
                        text: contact.displayName,
                    }),
                    isPublic: true,
                    isNewRelatedObjectsPublic: false,
                    canonicalRelationId: parentChildRelationId, // Link to parent relation
                    isChecked: null,
                },
            });

            // 2. Add the parent-child relation
            updates.push({
                operation: "addRelation",
                relation: {
                    version: 1,
                    id: parentChildRelationId,
                    authorId: authorId,
                    createdAt: timestamp,
                    updatedAt: timestamp,
                    fromId: parentNodeId,
                    toId: newNodeId,
                    relationTypeId: "child",
                    isPublic: true,
                    canonicalRelationId: null, // Will be updated later if label exists
                },
                fromPos: {
                    int: timestamp,
                    frac: crypto.randomBytes(4).toString("hex"),
                }, // Use unique frac for ordering
                toPos: {
                    int: timestamp,
                    frac: crypto.randomBytes(4).toString("hex"),
                },
            });
            updates.push({
                operation: "updateRelationList",
                relationId: parentChildRelationId,
                oldPosition: null,
                newPosition: {
                    int: timestamp,
                    frac: crypto.randomBytes(4).toString("hex"),
                },
                authorId: authorId,
                type: "all",
                oldIsPublic: true,
                newIsPublic: true,
                nodeId: parentNodeId,
                relatedNodeId: newNodeId,
            });

            // --- 3. Add the appleContactId property node and its relations ---
            const propertyNodeId = this.uuid();
            const propertyTypeRelationId = this.uuid(); // Relation between parent-child and label node
            const propertyRelationId = this.uuid(); // The actual relation storing the ID

            // 3a. Add the node holding the Apple ID value
            updates.push({
                operation: "addNode",
                node: {
                    version: 1,
                    id: propertyNodeId,
                    authorId: authorId,
                    createdAt: timestamp,
                    updatedAt: timestamp,
                    content: createNodeContent({
                        type: NodeContentType.Text,
                        text: relationLabelValue,
                    }),
                    isPublic: true,
                    isNewRelatedObjectsPublic: false,
                    canonicalRelationId: propertyRelationId, // Link to property relation
                    isChecked: null,
                },
            });

            // 3b. Add the relation for the property (Contact Node -> ID Value Node)
            updates.push({
                operation: "addRelation",
                relation: {
                    version: 1,
                    id: propertyRelationId,
                    authorId: authorId,
                    createdAt: timestamp,
                    updatedAt: timestamp,
                    fromId: newNodeId, // From the main contact node
                    toId: propertyNodeId, // To the ID value node
                    relationTypeId: "child", // Or a more specific type if available
                    isPublic: true,
                    canonicalRelationId: null, // Will be updated with type relation ID
                },
                fromPos: {
                    int: timestamp,
                    frac: crypto.randomBytes(4).toString("hex"),
                },
                toPos: {
                    int: timestamp,
                    frac: crypto.randomBytes(4).toString("hex"),
                },
            });
            updates.push({
                operation: "updateRelationList",
                relationId: propertyRelationId,
                oldPosition: null,
                newPosition: {
                    int: timestamp,
                    frac: crypto.randomBytes(4).toString("hex"),
                },
                authorId: authorId,
                type: "all",
                oldIsPublic: true,
                newIsPublic: true,
                nodeId: newNodeId,
                relatedNodeId: propertyNodeId,
            });

            // 3c. Add the label node ("appleContactId") - *Assume it might not exist, create defensively*
            // In a real system, you might fetch/cache common labels, but creating is safer for now.
            const labelNodeId = this.uuid();
            updates.push({
                operation: "addNode",
                node: {
                    version: 1,
                    id: labelNodeId,
                    authorId: authorId, // Or a system author
                    createdAt: timestamp,
                    updatedAt: timestamp,
                    content: createNodeContent({
                        type: NodeContentType.Text,
                        text: relationLabel,
                    }),
                    isPublic: true,
                    isNewRelatedObjectsPublic: false,
                    canonicalRelationId: null,
                    isChecked: null,
                },
            });

            // 3d. Add the __type__ relation (Property Relation -> Label Node)
            updates.push({
                operation: "addRelation",
                relation: {
                    version: 1,
                    id: propertyTypeRelationId,
                    authorId: authorId,
                    createdAt: timestamp,
                    updatedAt: timestamp,
                    fromId: propertyRelationId, // From the property relation
                    toId: labelNodeId, // To the label node
                    relationTypeId: "__type__",
                    isPublic: true,
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
            });
            updates.push({
                operation: "updateRelationList",
                relationId: propertyTypeRelationId,
                oldPosition: null,
                newPosition: {
                    int: timestamp,
                    frac: crypto.randomBytes(4).toString("hex"),
                },
                authorId: authorId,
                type: "all", // Check if this is correct for type relations
                oldIsPublic: true,
                newIsPublic: true,
                nodeId: propertyRelationId, // Check this
                relatedNodeId: labelNodeId,
            });

            // 3e. Update the property relation to point to its type relation
            updates.push({
                operation: "updateRelation",
                oldProps: {
                    // Need to reconstruct the state just added
                    version: 1,
                    id: propertyRelationId,
                    authorId: authorId,
                    createdAt: timestamp,
                    updatedAt: timestamp,
                    fromId: newNodeId,
                    toId: propertyNodeId,
                    relationTypeId: "child",
                    isPublic: true,
                    canonicalRelationId: null,
                },
                newProps: {
                    version: 1,
                    id: propertyRelationId,
                    authorId: authorId,
                    createdAt: timestamp,
                    updatedAt: timestamp,
                    fromId: newNodeId,
                    toId: propertyNodeId,
                    relationTypeId: "child",
                    isPublic: true,
                    canonicalRelationId: propertyTypeRelationId, // Link to the type relation
                },
            });

            // --- End appleContactId property ---
        }

        if (updates.length === 0) {
            console.log("[MewAPI] No contacts to batch add.");
            return createdContactsMap;
        }

        // Execute the single transaction with all updates
        console.log(
            `[MewAPI] Sending batch transaction with ${updates.length} operations...`
        );
        const payload = {
            clientId: AUTH_CONFIG.auth0ClientId,
            userId: authorId,
            transactionId: transactionId,
            updates: updates,
        };

        try {
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
                // Attempt to parse for more detailed error
                let detail = responseText;
                try {
                    const errorJson = JSON.parse(responseText);
                    detail = errorJson.detail || responseText;
                } catch (e) {
                    /* Ignore parsing error */
                }
                const errMsg = `Failed batch add contacts: Status ${txResponse.status} ${txResponse.statusText}. Detail: ${detail}`;
                console.error(errMsg);
                // console.error("Request payload was:", JSON.stringify(payload, null, 2)); // Careful logging large payloads
                throw new Error(errMsg);
            }

            const responseJson = await txResponse.json();
            console.log(
                `[MewAPI] Batch add transaction successful. Response:`,
                responseJson // Or summarize if too large
            );
            return createdContactsMap;
        } catch (error) {
            console.error(
                "[MewAPI] Error during batch add fetch operation:",
                error
            );
            // Depending on the error, we might want to clear the map or handle partially successful batches
            // For now, we'll throw, assuming the whole batch failed.
            throw error;
        }
    }
}

export const parseNodeIdFromUrl = (url: string): string => {
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

export interface ContentBlock {
    type: "text" | "mention"; // Could be expanded if there are other types
    value: string;
}

interface User {
    id: string;
    username: string;
    email: string;
}

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

interface TokenData {
    access_token: string;
    expires_in: number;
    token_type: string;
}
