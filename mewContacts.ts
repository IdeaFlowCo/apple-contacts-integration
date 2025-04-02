/* mewContacts.ts - Manages syncing Apple Contacts to Mew */

import { fileURLToPath } from "url";
import { resolve } from "path";
import { execSync } from "child_process"; // Added for executing Python script
import {
    MewAPI,
    parseNodeIdFromUrl,
    GraphNode,
    Relation,
    createNodeContent,
} from "./MewService.js";
// We might need a logger later, let's keep this import commented for now
// import { Logger } from "./utils/logger";

// const logger = new Logger("MewContacts");

export const mewApi = new MewAPI();

// Global variable to hold the user root URL provided via CLI argument
let userRootUrlGlobal: string | null = null;

// Helper to get the user ID - uses the globally set userRootUrl
async function getUserId(): Promise<string> {
    if (!userRootUrlGlobal) {
        throw new Error(
            "User root URL not provided. Pass it as a command-line argument."
        );
    }
    try {
        const rootNodeId = parseNodeIdFromUrl(userRootUrlGlobal);
        // Set the user ID in the MewAPI instance upon first retrieval
        mewApi.setCurrentUserId(rootNodeId);
        return rootNodeId;
    } catch (error) {
        console.error(
            "Failed to parse user root URL:",
            userRootUrlGlobal,
            error
        );
        throw new Error("Invalid user root URL format provided.");
    }
}

// Helper function to get the text content of a simple text node
function getNodeTextContent(node: GraphNode | null): string | null {
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

// Define the standard folder name for contacts
const myContactsFolderName = "My Contacts";

/**
 * Ensures that the "My Contacts" folder exists in Mew under the user's root node.
 * Finds the existing folder or creates a new one if it doesn't exist.
 * @returns The Node ID of the "My Contacts" folder.
 */
export async function ensureMyContactsFolder(): Promise<{
    folderId: string;
    created: boolean;
}> {
    console.log(`[MewContacts] Ensuring folder: ${myContactsFolderName}`);
    let rootNodeId;
    let created = false; // Flag to track if we created the folder

    try {
        rootNodeId = await getUserId(); // This also sets the userId in the mewApi instance
        console.log("[MewContacts] Using root node ID:", rootNodeId);
    } catch (error) {
        console.error("[MewContacts] Failed to get root node ID:", error);
        throw new Error(
            "Failed to get root node ID for ensuring contacts folder."
        );
    }

    // Look for existing "My Contacts" folder under the root node
    console.log(
        `[MewContacts] Searching for folder '${myContactsFolderName}' under root node '${rootNodeId}'`
    );
    const existingNode = await mewApi.findNodeByText({
        parentNodeId: rootNodeId,
        nodeText: myContactsFolderName,
    });

    if (existingNode) {
        console.log(
            `[MewContacts] Found existing '${myContactsFolderName}' folder with id:`,
            existingNode.id
        );
        return { folderId: existingNode.id, created: false }; // Return existing ID, created = false
    }

    // Create the "My Contacts" folder if it doesn't exist
    console.log(
        `[MewContacts] Creating '${myContactsFolderName}' folder under root node '${rootNodeId}'.`
    );
    try {
        const response = await mewApi.addNode({
            content: { type: "text", text: myContactsFolderName },
            parentNodeId: rootNodeId,
            authorId: rootNodeId, // Use rootNodeId as authorId since it's the user's space
        });

        const newContactsFolderId = response.newNodeId;
        console.log(
            `[MewContacts] '${myContactsFolderName}' folder created with id:`,
            newContactsFolderId
        );
        console.log(
            "[MewContacts] New folder node URL:",
            mewApi.getNodeUrl(newContactsFolderId)
        );
        created = true; // Set flag to true
        return { folderId: newContactsFolderId, created: true }; // Return new ID, created = true
    } catch (error) {
        console.error(
            `[MewContacts] Failed to create '${myContactsFolderName}' folder:`,
            error
        );
        throw new Error(`Failed to create '${myContactsFolderName}' folder.`);
    }
}

// Placeholder type for Apple Contact data (replace with actual structure)
interface AppleContact {
    identifier: string; // Unique ID from Apple Contacts
    givenName?: string;
    familyName?: string;
    organizationName?: string;
    phoneNumbers?: { label: string; value: string }[];
    emailAddresses?: { label: string; value: string }[];
    // Add other relevant fields: postalAddresses, birthday, note etc.
}

/**
 * Finds the Mew node corresponding to an Apple Contact identifier.
 * @param identifier The Apple Contact's unique identifier.
 * @param contactsFolderId The Node ID of the "My Contacts" folder.
 * @returns The GraphNode of the contact if found, otherwise null.
 */
async function findContactNodeByIdentifier(
    identifier: string,
    contactsFolderId: string
): Promise<GraphNode | null> {
    console.log(
        `[MewContacts] Searching for contact with Apple ID: ${identifier} under folder ${contactsFolderId}`
    );

    try {
        // 1. Get all child nodes (potential contact nodes) of contactsFolderId.
        const { childNodes: potentialContactNodes } =
            await mewApi.getChildNodes({ parentNodeId: contactsFolderId });

        // 2. For each potential contact node, check its children for the appleContactId relation.
        for (const contactNode of potentialContactNodes) {
            if (!contactNode) continue; // Skip if node data is missing

            // console.log(`[MewContacts] Checking potential contact node: ${contactNode.id}`); // Verbose logging

            // 2a. Get its relations and the layer data needed to check labels.
            const layerData = await mewApi.getLayerData([contactNode.id]); // Fetch data for this node and potentially its children/labels
            const relations = Object.values(
                layerData.data.relationsById
            ).filter(
                (rel): rel is Relation =>
                    rel !== null &&
                    typeof rel === "object" &&
                    "fromId" in rel &&
                    rel.fromId === contactNode.id
            );

            // 2b & 2c. Find the relation with label "appleContactId"
            const idRelation = relations.find((rel) => {
                const typeRelationId = rel.canonicalRelationId;
                if (typeRelationId) {
                    // Need to ensure the label node and its defining relation are in the layer data
                    // It's possible getLayerData([contactNode.id]) doesn't fetch label nodes two steps away
                    // A safer approach might be to fetch layer data for contactNode.id and all related node IDs (rel.toId)
                    const typeRelation =
                        layerData.data.relationsById[typeRelationId];
                    if (
                        typeRelation &&
                        typeRelation.relationTypeId === "__type__"
                    ) {
                        const labelNodeId = typeRelation.toId;
                        const labelNode = layerData.data.nodesById[labelNodeId];
                        const labelText = getNodeTextContent(labelNode);
                        // console.log(`[MewContacts] Node ${contactNode.id} has relation with label: ${labelText}`); // Verbose logging
                        return labelText === "appleContactId";
                    }
                }
                return false;
            });

            if (idRelation) {
                const idNodeId = idRelation.toId;
                const idNode = layerData.data.nodesById[idNodeId];

                // 2d. Compare the content value.
                const storedIdentifier = getNodeTextContent(idNode);
                // console.log(`[MewContacts] Found appleContactId node ${idNodeId} with value: ${storedIdentifier}`); // Verbose logging

                if (storedIdentifier === identifier) {
                    console.log(
                        `[MewContacts] Match found! Returning contact node: ${contactNode.id}`
                    );
                    return contactNode; // Found the matching contact node
                }
            }
            // else {
            // console.log(`[MewContacts] No appleContactId relation found for node ${contactNode.id}`); // Verbose logging
            // }
        }

        // 3. If no match found after checking all children.
        console.log(
            `[MewContacts] No contact node found with Apple ID: ${identifier}`
        );
        return null;
    } catch (error) {
        console.error(
            `[MewContacts] Error in findContactNodeByIdentifier for ID ${identifier}:`,
            error
        );
        return null; // Return null on error
    }
}

/**
 * Syncs a single Apple Contact to Mew.
 * Creates or updates the contact node and its properties under the "My Contacts" folder.
 * @param contactData The Apple Contact data.
 * @param contactsFolderId The Node ID of the "My Contacts" folder.
 * @param existingMewContactsMap Map of existing Apple identifiers to Mew Node IDs in the folder.
 */
export async function syncContactToMew(
    contactData: AppleContact,
    contactsFolderId: string,
    existingMewContactsMap: Map<string, string>
): Promise<void> {
    const contactDisplayName =
        `${contactData.givenName || ""} ${
            contactData.familyName || ""
        }`.trim() ||
        contactData.organizationName ||
        "Unnamed Contact";
    console.log(
        `[MewContacts] Syncing contact: ${contactDisplayName} (Apple ID: ${contactData.identifier})`
    );

    const authorId = await getUserId(); // Get authorId once

    try {
        // --- Optimization: Check existing map first ---
        let existingMewNodeId: string | undefined = existingMewContactsMap.get(
            contactData.identifier
        );
        let contactNode: GraphNode | null = null; // Keep this for potential update logic needing the node object
        let contactNodeId: string | null = null; // Declare contactNodeId here for broader scope

        if (existingMewNodeId) {
            console.log(
                `[MewContacts] Contact found in existing map: Apple ID ${contactData.identifier} -> Mew ID ${existingMewNodeId}`
            );
            // If we need the full node object for updates later, we might need a fetch here,
            // but for now, we only need the ID for the update check.
            contactNodeId = existingMewNodeId; // Assign the ID found in the map
            // TODO: If update logic needs the full node, fetch it: contactNode = await mewApi.getNode(existingMewNodeId);
        } else {
            // If not in the map, it needs to be created.
            // We don't need to call findContactNodeByIdentifier anymore.
            console.log(
                `[MewContacts] Contact Apple ID ${contactData.identifier} not found in existing map.`
            );
            // contactNodeId remains null
        }
        // --- End Optimization Check ---

        // Original logic adjusted:

        // If Mew node ID wasn't found in the map, create the node
        if (!contactNodeId) {
            // const creationReason = isNewFolder ? "(in new folder)" : "(not found)"; // Old reason
            const creationReason =
                existingMewNodeId === undefined
                    ? "(not in map)"
                    : "(logic error?)";
            console.log(
                `[MewContacts] Creating new node for contact ${creationReason}: ${contactDisplayName}`
            );
            const response = await mewApi.addNode({
                content: { type: "text", text: contactDisplayName },
                parentNodeId: contactsFolderId,
                authorId: authorId,
            });
            contactNodeId = response.newNodeId;
            console.log(`[MewContacts] Created contact node: ${contactNodeId}`);

            // Immediately add the identifier node using addOrUpdatePropertyNode
            // Non-null assertion ok here as we just created it
            await addOrUpdatePropertyNode({
                parentNodeId: contactNodeId!,
                relationLabel: "appleContactId", // Use specific, consistent label
                value: contactData.identifier,
                authorId: authorId,
            });
            console.log(
                `[MewContacts] Added appleContactId node for ${contactNodeId}`
            );
            // Note: Properties will be added/updated in the common section below
        } else {
            // If Mew node ID *was* found in the map, update its display name if changed.
            console.log(
                `[MewContacts] Checking for updates for existing contact node: ${contactNodeId}`
            );

            // !! IMPORTANT !! We currently don't have the full 'contactNode' object
            // because we skipped findContactNodeByIdentifier.
            // To check/update the name, we either need to:
            //   a) Fetch the node content here: const existingNodeData = await mewApi.getNode(contactNodeId); const currentName = getNodeTextContent(existingNodeData.node);
            //   b) Include node names in the initial batch fetch (more complex map structure needed)
            //   c) Assume the name doesn't need checking/updating if only syncing properties (simplest for now, but potentially incorrect)

            // For now, let's proceed without the name check/update to keep the optimization focused.
            // We will still update properties below.
            console.warn(
                `[MewContacts] Skipping name update check for existing contact ${contactNodeId} due to optimization.`
            );

            /* // Original name update logic (requires full node object):
            const currentName = getNodeTextContent(contactNode!); // Requires contactNode fetched earlier
            if (currentName !== contactDisplayName) {
                console.log(
                    `[MewContacts] Updating contact node ${contactNodeId} display name from '${currentName}' to '${contactDisplayName}'`
                );
                await mewApi.updateNode(contactNodeId, {
                    content: createNodeContent({
                        type: "text",
                        text: contactDisplayName,
                    }),
                });
            }
            */
        }

        // --- Common Section for Property Updates (Runs for both Create and Update) ---

        // Ensure we have a contactNodeId to proceed (should always be true here unless creation failed)
        if (!contactNodeId) {
            console.error(
                `[MewContacts] Failed to obtain a contactNodeId for Apple contact ${contactData.identifier}. Skipping property updates.`
            );
            return; // Exit sync for this contact if ID is missing
        }

        // 4. Add/Update property nodes.
        console.log(
            `[MewContacts] Updating properties for node: ${contactNodeId}`
        );

        // Define which properties to sync and their base labels
        // TODO: Add more properties like address, notes, birthday
        const propertiesToSync = [
            { key: "phoneNumbers", baseLabel: "phone", array: true },
            { key: "emailAddresses", baseLabel: "email", array: true },
            {
                key: "organizationName",
                baseLabel: "organization",
                array: false,
            },
        ];

        for (const propInfo of propertiesToSync) {
            const data = contactData[propInfo.key as keyof AppleContact];

            if (propInfo.array) {
                // Handle arrays like phoneNumbers, emailAddresses
                const items = (data || []) as {
                    label?: string;
                    value: string;
                }[]; // Default to empty array
                for (const item of items) {
                    if (item.value) {
                        // Ensure there's a value to sync
                        const sanitizedLabel = item.label
                            ? item.label
                                  .replace(/[^a-zA-Z0-9]/g, "_")
                                  .toLowerCase()
                            : "";
                        const relationLabel = sanitizedLabel
                            ? `${propInfo.baseLabel}_${sanitizedLabel}`
                            : propInfo.baseLabel;
                        await addOrUpdatePropertyNode({
                            parentNodeId: contactNodeId,
                            relationLabel: relationLabel,
                            value: item.value,
                            authorId: authorId,
                        });
                    }
                }
                // TODO: Handle deletion of properties that are no longer present in the source data
            } else if (typeof data === "string" && data) {
                // Handle simple string properties like organizationName
                await addOrUpdatePropertyNode({
                    parentNodeId: contactNodeId,
                    relationLabel: propInfo.baseLabel,
                    value: data,
                    authorId: authorId,
                });
                // TODO: Handle deletion if the string property is now empty/null in source
            }
            // TODO: Handle deletion if the property key itself is absent from contactData
        }

        console.log(
            `[MewContacts] Finished syncing contact: ${contactDisplayName}`
        );
    } catch (error) {
        console.error(
            `[MewContacts] Failed to sync contact ${contactDisplayName} (ID: ${contactData.identifier}):`,
            error
        );
        // Optionally re-throw or handle error appropriately
    }
}

/**
 * Helper function to add or update a property node (like phone, email) for a parent (contact) node.
 * Searches for an existing node with the same relationLabel under the parent.
 * If found and value differs, updates it. If not found, creates it.
 * @param params Information about the property node
 */
async function addOrUpdatePropertyNode(params: {
    parentNodeId: string; // Contact Node ID
    relationLabel: string;
    value: string;
    authorId: string;
}): Promise<void> {
    const { parentNodeId, relationLabel, value, authorId } = params;

    // console.log(`[MewContacts] Ensuring property '${relationLabel}' with value '${value}' for node ${parentNodeId}`); // Verbose

    try {
        // 1. Get children of parentNodeId and their relations/labels
        // Fetching layer data here again is inefficient. Ideally, fetch once in syncContactToMew
        // and pass relevant data down. For now, keeping it simple.
        const layerData = await mewApi.getLayerData([parentNodeId]);
        const relations = Object.values(layerData.data.relationsById).filter(
            (rel): rel is Relation =>
                rel !== null &&
                typeof rel === "object" &&
                "fromId" in rel &&
                rel.fromId === parentNodeId
        );

        let existingPropertyNode: GraphNode | null = null;

        // 2. Search for an existing node with the same relationLabel.
        for (const relation of relations) {
            const typeRelationId = relation.canonicalRelationId;
            if (typeRelationId) {
                // Again, potential issue: label node might not be in layerData if only parentNodeId was requested
                const typeRelation =
                    layerData.data.relationsById[typeRelationId];
                if (
                    typeRelation &&
                    typeRelation.relationTypeId === "__type__"
                ) {
                    const labelNodeId = typeRelation.toId;
                    const labelNode = layerData.data.nodesById[labelNodeId];
                    const labelText = getNodeTextContent(labelNode);

                    if (labelText === relationLabel) {
                        const propertyNodeId = relation.toId;
                        existingPropertyNode =
                            layerData.data.nodesById[propertyNodeId];
                        // console.log(`[MewContacts] Found existing property node ${existingPropertyNode?.id} for label '${relationLabel}'`); // Verbose
                        break;
                    }
                }
            }
        }

        // 3. If found, compare value and update if needed.
        if (existingPropertyNode) {
            const existingValue = getNodeTextContent(existingPropertyNode);
            if (existingValue !== value) {
                console.log(
                    `[MewContacts] Updating property node ${existingPropertyNode.id} ('${relationLabel}'). Old: '${existingValue}', New: '${value}'`
                );
                await mewApi.updateNode(existingPropertyNode.id, {
                    content: createNodeContent({ type: "text", text: value }),
                });
            }
            // else { // Verbose
            // console.log(`[MewContacts] Property node ${existingPropertyNode.id} ('${relationLabel}') already has correct value.`);
            // }
        }
        // 4. If not found, create a new node.
        else {
            console.log(
                `[MewContacts] Creating new property node ('${relationLabel}') with value '${value}'`
            );
            await mewApi.addNode({
                content: { type: "text", text: value },
                parentNodeId: parentNodeId,
                relationLabel: relationLabel,
                authorId: authorId,
            });
        }
    } catch (error) {
        console.error(
            `[MewContacts] Failed to add/update property node '${relationLabel}' for parent ${parentNodeId}:`,
            error
        );
        throw error; // Rethrow to signal failure up the chain
    }
}

/**
 * Fetches all child nodes of a given folder and builds a map of
 * Apple Contact Identifier -> Mew Node ID for efficient lookup.
 * @param contactsFolderId The Node ID of the "My Contacts" folder in Mew.
 * @returns A Map where keys are Apple Contact Identifiers and values are Mew Node IDs.
 */
async function fetchExistingContactIdentifiers(
    contactsFolderId: string
): Promise<Map<string, string>> {
    const existingContactsMap = new Map<string, string>();
    console.log(
        `[MewContacts] Getting child nodes for folder: ${contactsFolderId}`
    );

    // 1. Get all child nodes (potential contact nodes) of contactsFolderId.
    const { childNodes: potentialContactNodes } = await mewApi.getChildNodes({
        parentNodeId: contactsFolderId,
    });

    if (!potentialContactNodes || potentialContactNodes.length === 0) {
        console.log("[MewContacts] No existing child nodes found in folder.");
        return existingContactsMap; // Return empty map
    }

    const childNodeIds = potentialContactNodes
        .map((node) => node?.id)
        .filter((id): id is string => !!id);

    if (childNodeIds.length === 0) {
        console.log(
            "[MewContacts] Child nodes found but missing IDs. Returning empty map."
        );
        return existingContactsMap;
    }

    console.log(
        `[MewContacts] Found ${childNodeIds.length} child nodes. Fetching layer data...`
    );

    // 2. Fetch layer data for all children at once.
    // This should hopefully include relations and potentially the identifier nodes.
    const layerData = await mewApi.getLayerData(childNodeIds);

    // 3. Process the layer data to build the map.
    console.log("[MewContacts] Processing layer data to find identifiers...");
    for (const contactNodeId of childNodeIds) {
        const relations = Object.values(layerData.data.relationsById).filter(
            (rel): rel is Relation =>
                rel !== null &&
                typeof rel === "object" &&
                "fromId" in rel &&
                rel.fromId === contactNodeId
        );

        // Find the relation specifically labeled "appleContactId"
        let appleIdentifier: string | null = null;
        for (const relation of relations) {
            const typeRelationId = relation.canonicalRelationId;
            if (typeRelationId) {
                const typeRelation =
                    layerData.data.relationsById[typeRelationId];
                if (
                    typeRelation &&
                    typeRelation.relationTypeId === "__type__"
                ) {
                    const labelNodeId = typeRelation.toId;
                    const labelNode = layerData.data.nodesById[labelNodeId];
                    const labelText = getNodeTextContent(labelNode);

                    if (labelText === "appleContactId") {
                        // Found the relation. Now get the identifier node's content.
                        const idNodeId = relation.toId;
                        const idNode = layerData.data.nodesById[idNodeId];
                        appleIdentifier = getNodeTextContent(idNode);
                        break; // Found the identifier, no need to check other relations for this node
                    }
                }
            }
        }

        if (appleIdentifier) {
            // console.log(`[MewContacts] Mapping AppleID: ${appleIdentifier} -> MewID: ${contactNodeId}`); // Verbose
            existingContactsMap.set(appleIdentifier, contactNodeId);
        } else {
            // This might happen if a node in the folder is not a contact managed by this script
            // or if the appleContactId relation is missing.
            console.warn(
                `[MewContacts] Could not find appleContactId for Mew node ${contactNodeId} in folder ${contactsFolderId}`
            );
        }
    }

    console.log(
        `[MewContacts] Finished building map with ${existingContactsMap.size} identifiers.`
    );
    return existingContactsMap;
}

const BATCH_CHUNK_SIZE = 50; // Number of contacts to create per batch API call

// Main processing function
export async function processContacts(
    userRootUrl: string,
    contactsJsonString: string
): Promise<void> {
    console.log("[MewContacts] Starting contact processing...");
    userRootUrlGlobal = userRootUrl; // Set the global URL

    let contactsData: AppleContact[];
    try {
        contactsData = JSON.parse(contactsJsonString);
        if (!Array.isArray(contactsData)) {
            throw new Error("Parsed data is not an array.");
        }
        console.log(
            `[MewContacts] Successfully parsed ${contactsData.length} contacts.`
        );
    } catch (error) {
        console.error("[MewContacts] Failed to parse contacts JSON:", error);
        process.exit(1);
    }

    try {
        // Ensure the contacts folder exists and get its ID and creation status
        const { folderId: contactsFolderId, created: isNewFolder } =
            await ensureMyContactsFolder();

        if (!contactsFolderId) {
            throw new Error("Failed to find or create the contacts folder.");
        }

        console.log(
            `[MewContacts] Using contacts folder ID: ${contactsFolderId} (Newly Created: ${isNewFolder})`
        );

        // --- Optimization: Fetch existing contact identifiers in batch ---
        let existingMewContactsMap = new Map<string, string>(); // Map<appleIdentifier, mewNodeId>

        if (!isNewFolder) {
            console.log(
                `[MewContacts] Fetching existing contacts from folder ${contactsFolderId}...`
            );
            try {
                existingMewContactsMap = await fetchExistingContactIdentifiers(
                    contactsFolderId
                );
            } catch (error) {
                console.error(
                    `[MewContacts] Error fetching existing contact identifiers:`,
                    error,
                    "Proceeding with individual checks."
                );
                // Keep map empty on error
            }
            console.log(
                `[MewContacts] Found ${existingMewContactsMap.size} existing contacts in Mew folder.`
            );
        } else {
            console.log(
                "[MewContacts] Skipping fetch of existing contacts as folder is new."
            );
        }
        // --- End Optimization ---

        const contactsToCreate: { identifier: string; displayName: string }[] =
            [];
        const contactsToUpdate: { appleData: AppleContact; mewId: string }[] =
            [];

        // Separate contacts into create/update lists
        for (const contact of contactsData) {
            // Basic validation
            if (
                !contact ||
                typeof contact !== "object" ||
                !contact.identifier
            ) {
                console.warn(
                    "[MewContacts] Skipping invalid contact data:",
                    contact
                );
                continue;
            }

            const existingMewId = existingMewContactsMap.get(
                contact.identifier
            );
            if (existingMewId) {
                // Exists in Mew, needs update check
                contactsToUpdate.push({
                    appleData: contact,
                    mewId: existingMewId,
                });
            } else {
                // Does not exist in Mew, needs creation
                const contactDisplayName =
                    `${contact.givenName || ""} ${
                        contact.familyName || ""
                    }`.trim() ||
                    contact.organizationName ||
                    "Unnamed Contact";
                contactsToCreate.push({
                    identifier: contact.identifier,
                    displayName: contactDisplayName,
                });
            }
        }

        console.log(
            `[MewContacts] Planning: ${contactsToCreate.length} creates, ${contactsToUpdate.length} updates.`
        );

        // --- Chunked Batch Create ---
        if (contactsToCreate.length > 0) {
            console.log(
                `[MewContacts] Starting batch creation for ${contactsToCreate.length} contacts in chunks of ${BATCH_CHUNK_SIZE}...`
            );
            let createdCount = 0;
            for (
                let i = 0;
                i < contactsToCreate.length;
                i += BATCH_CHUNK_SIZE
            ) {
                const chunk = contactsToCreate.slice(i, i + BATCH_CHUNK_SIZE);
                console.log(
                    `[MewContacts] Processing chunk ${
                        i / BATCH_CHUNK_SIZE + 1
                    } / ${Math.ceil(
                        contactsToCreate.length / BATCH_CHUNK_SIZE
                    )} (Contacts ${i + 1} - ${i + chunk.length})`
                );

                try {
                    const createdMap = await mewApi.batchAddContacts(
                        chunk,
                        contactsFolderId
                    );
                    createdCount += createdMap.size;
                    console.log(
                        `[MewContacts] Chunk ${
                            i / BATCH_CHUNK_SIZE + 1
                        } successful. ${
                            createdMap.size
                        } contacts created in this chunk.`
                    );
                    // Optional: Short delay between chunks if needed?
                    // await new Promise(resolve => setTimeout(resolve, 200));
                } catch (error) {
                    console.error(
                        `[MewContacts] Batch creation failed for chunk starting at index ${i}:`,
                        error
                    );
                    // Decide how to proceed - skip remaining? Try sequential for this chunk?
                    // For now, we'll stop the entire sync if one chunk fails.
                    throw new Error(
                        `Batch creation failed on chunk ${
                            i / BATCH_CHUNK_SIZE + 1
                        }, stopping sync.`
                    );
                }
            }
            console.log(
                `[MewContacts] Batch creation phase completed. Total contacts created: ${createdCount}`
            );
        }
        // --- End Chunked Batch Create ---

        // --- Sequential Update ---
        console.log(
            `[MewContacts] Starting sequential check/update for ${contactsToUpdate.length} existing contacts...`
        );
        for (const updateInfo of contactsToUpdate) {
            // We need to call syncContactToMew, but it expects the map.
            // We can pass the original map, or an empty one if we know it exists.
            // Passing the original map is safer as syncContactToMew uses it.
            await syncContactToMew(
                updateInfo.appleData,
                contactsFolderId,
                existingMewContactsMap
            );
        }

        console.log("[MewContacts] Sync finished successfully.");
    } catch (error) {
        console.error("[MewContacts] Sync process failed:", error);
        process.exit(1);
    }
}

// Script execution entry point check using ES module standards
const currentFilePath = fileURLToPath(import.meta.url);
// Resolve the path provided by Node.js when executing the script
const scriptPath = resolve(process.argv[1]);

// Check if the current file path matches the executed script path
if (currentFilePath === scriptPath) {
    // --- Main script execution logic starts here ---
    const args = process.argv.slice(2);
    if (args.length !== 1) {
        console.error("Usage: node <script.js> <userRootUrl>");
        process.exit(1);
    }

    const userRootUrlArg = args[0];

    try {
        console.log("[MewContacts] Fetching contacts via Python script...");
        // Execute the Python script and capture stdout
        const pythonOutput = execSync("python3 get_contacts_json.py", {
            encoding: "utf8",
            // Increase maxBuffer if you have a very large number of contacts
            // maxBuffer: 1024 * 1024 * 10, // Example: 10MB
        });

        console.log("[MewContacts] Parsing fetched contacts...");
        // Parse the JSON output from the Python script
        const contactsData = JSON.parse(pythonOutput);

        if (!Array.isArray(contactsData)) {
            throw new Error("Python script did not return a JSON array.");
        }

        console.log(
            `[MewContacts] Received ${contactsData.length} contacts. Starting sync process...`
        );
        // Call the main processing function with the fetched data
        // Pass the raw JSON string, as processContacts handles parsing
        processContacts(userRootUrlArg, pythonOutput).catch((err) => {
            console.error(
                "[MewContacts] Unhandled error during contact processing:",
                err
            );
            process.exit(1);
        });
    } catch (error) {
        console.error(
            "[MewContacts] Failed to fetch or parse contacts from Python script:",
            error
        );
        // Check if it's a process error (e.g., python not found, script error)
        if (error instanceof Error && "stderr" in error) {
            console.error(
                "[MewContacts] Python stderr:",
                (error as any).stderr?.toString()
            );
        }
        process.exit(1);
    }

    // --- Main script execution logic ends here ---
}
