/* mewContacts.ts - Manages syncing Apple Contacts to Mew */

import { spawn } from "child_process"; // Changed from execSync to spawn
import {
    MewAPI,
    parseUserRootNodeIdFromUrl,
    getNodeTextContent,
    Relation,
    AppleContact,
    MewContact,
    Operation,
} from "./MewService.js"; // Assuming MewService exports necessary types
import { logger } from "./utils/logger.js";
import { fileURLToPath } from "url";
import { resolve } from "path";
import { setTimeout } from "timers";

// --- Type Definitions ---

/** Represents the structure of contact data expected from the Python script. */
// Moved to MewService.ts: export interface AppleContact { ... }

/**
 * Holds information about a contact already existing in Mew, including its ID
 * and a map of its properties fetched upfront.
 */
interface ExistingContactInfo {
    mewId: string; // The Mew Node ID of the main contact node
    name: string | null; // Pre-fetched display name of the contact node
    properties: Map<string, { propertyNodeId: string; value: string | null }>; // Map<RelationLabel, { PropertyNodeID, Value }>
}

// --- Globals & Constants ---

/** Mew API instance used throughout the script. */
export const mewApi = new MewAPI();

/** Global variable to hold the user root URL provided via CLI argument. Set in processContacts. */
let userRootUrlGlobal: string | null = null;

/** Defines the standard folder name under which contacts will be synced in Mew. */
const myContactsFolderName = "My Contacts";

/** Number of contacts to include in each batch creation request. */
const BATCH_CHUNK_SIZE = 50;

// --- Helper Functions ---

/**
 * Parses the globally set user root URL to extract the root node ID and author ID,
 * then sets them in the MewAPI instance.
 * @throws {Error} If the global user root URL is not set or invalid.
 */
function initializeApiConfigFromRootUrl(): void {
    // Renamed and modified
    if (!userRootUrlGlobal) {
        throw new Error(
            "User root URL not provided. Pass it as a command-line argument."
        );
    }
    try {
        const rootNodeId = parseUserRootNodeIdFromUrl(userRootUrlGlobal);

        // Extract the actual author ID (e.g., google-oauth2|...) from the rootNodeId
        const authorIdMatch = rootNodeId.match(/^(?:user-root-id-)?(.*)$/);
        if (!authorIdMatch || !authorIdMatch[1]) {
            throw new Error(
                "Could not extract author ID from root node ID: " + rootNodeId
            );
        }
        const authorId = authorIdMatch[1];

        logger.log("[MewContacts] Initializing MewAPI config:", {
            rootNodeId,
            authorId,
        });

        // Set both IDs in the MewAPI instance
        mewApi.setCurrentUserRootNodeId(rootNodeId);
        mewApi.setAuthorId(authorId);

        // No return value needed, just sets config
    } catch (error) {
        logger.error(
            `Failed to initialize API config from user root URL: ${userRootUrlGlobal}`,
            error
        );
        throw new Error(
            "Invalid user root URL format or failed to extract IDs."
        );
    }
}

/**
 * Ensures that the target folder (defined by `myContactsFolderName`) exists in Mew
 * under the user's root node. Finds the existing folder or creates a new one.
 * Assumes `initializeApiConfigFromRootUrl` has already been called.
 * @returns {Promise<{ folderId: string; created: boolean }>} An object containing the Node ID
 *          of the folder and a boolean indicating if it was newly created in this run.
 * @throws {Error} If unable to get user ID or create/find the folder.
 */
export async function ensureMyContactsFolder(): Promise<{
    folderId: string;
    created: boolean;
}> {
    logger.log(`[MewContacts] Ensuring folder: ${myContactsFolderName}`);
    let created = false; // Flag to track if we created the folder

    // Get the root node ID directly from the API instance (assuming it's set)
    const rootNodeId = mewApi.getCurrentUserRootNodeInfo().id;
    if (!rootNodeId) {
        throw new Error(
            "Root node ID not set in MewAPI. Ensure initializeApiConfigFromRootUrl was called."
        );
    }
    logger.log(
        "[MewContacts] Using root node ID from MewAPI config:",
        rootNodeId
    );

    // Look for existing "My Contacts" folder under the root node
    logger.log(
        `[MewContacts] Searching for folder '${myContactsFolderName}' under root node '${rootNodeId}'`
    );
    const existingNode = await mewApi.findNodeByText({
        parentNodeId: rootNodeId,
        nodeText: myContactsFolderName,
    });

    if (existingNode) {
        logger.log(
            `[MewContacts] Found existing '${myContactsFolderName}' folder with id:`,
            existingNode.id
        );
        return { folderId: existingNode.id, created: false }; // Return existing ID, created = false
    }

    // Create the "My Contacts" folder if it doesn't exist
    logger.log(
        `[MewContacts] Creating '${myContactsFolderName}' folder under root node '${rootNodeId}'.`
    );
    try {
        // addNode now uses the internal authorId set via setAuthorId
        const response = await mewApi.addNode({
            content: { type: "text", text: myContactsFolderName },
            parentNodeId: rootNodeId,
            // No need to specify authorId here, it uses the one set in the instance
        });

        const newContactsFolderId = response.newNodeId;
        logger.log(
            `[MewContacts] '${myContactsFolderName}' folder created with id:`,
            newContactsFolderId
        );
        logger.log(
            "[MewContacts] New folder node URL:",
            mewApi.getNodeUrl(newContactsFolderId)
        );
        created = true; // Set flag to true
        return { folderId: newContactsFolderId, created: true }; // Return new ID, created = true
    } catch (error) {
        logger.error(
            `[MewContacts] Failed to create '${myContactsFolderName}' folder:`,
            error
        );
        throw new Error(`Failed to create '${myContactsFolderName}' folder.`);
    }
}

/**
 * Fetches detailed information about contacts already existing in the Mew folder.
 * Retrieves child nodes, fetches their layer data (including properties),
 * and builds a map for efficient lookup and update checking.
 * @param contactsFolderId The Node ID of the parent contacts folder in Mew.
 * @returns {Promise<Map<string, ExistingContactInfo>>} A Map where keys are Apple Contact Identifiers
 *          and values contain the Mew ID, name, and a map of existing properties.
 */
async function fetchExistingContactInfo(
    contactsFolderId: string
): Promise<Map<string, ExistingContactInfo>> {
    const existingContactsMap = new Map<string, ExistingContactInfo>();
    logger.log(
        `[MewContacts] Fetching existing contact info from folder: ${contactsFolderId}`
    );

    // --- Step 1: Get Child Node IDs ---
    let childNodeIds: string[] = [];
    try {
        const { childNodes: potentialContactNodes } =
            await mewApi.getChildNodes({
                parentNodeId: contactsFolderId,
            });
        if (!potentialContactNodes || potentialContactNodes.length === 0) {
            logger.log(
                "[MewContacts] No existing child nodes found in folder."
            );
            return existingContactsMap; // Return empty map
        }
        childNodeIds = potentialContactNodes
            .map((node) => node?.id)
            .filter((id): id is string => !!id);

        if (childNodeIds.length === 0) {
            logger.log("[MewContacts] Child nodes found but missing IDs.");
            return existingContactsMap;
        }
        logger.log(`[MewContacts] Found ${childNodeIds.length} child nodes.`);
    } catch (error) {
        logger.error("[MewContacts] Error getting child nodes:", error);
        return existingContactsMap; // Return empty map on error
    }

    // --- Step 2: First Layer Fetch (Children + Direct Relations) ---
    logger.log(
        `[MewContacts] Fetching initial layer data for ${childNodeIds.length} nodes...`
    );
    let combinedLayerData: any = { data: { nodesById: {}, relationsById: {} } }; // Initialize structure
    let directRelationTargetIds = new Set<string>();
    let directCanonicalRelationIds = new Set<string>();

    try {
        const initialLayerData = await mewApi.getLayerData(childNodeIds);
        // Merge initial data
        Object.assign(
            combinedLayerData.data.nodesById,
            initialLayerData.data.nodesById
        );
        Object.assign(
            combinedLayerData.data.relationsById,
            initialLayerData.data.relationsById
        );

        // Find IDs needed for the second fetch
        for (const nodeId of childNodeIds) {
            const relations = Object.values(
                initialLayerData.data.relationsById
            ).filter(
                (rel: any): rel is Relation => rel && rel.fromId === nodeId
            );
            for (const rel of relations) {
                directRelationTargetIds.add(rel.toId); // Property value node IDs
                if (rel.canonicalRelationId) {
                    directCanonicalRelationIds.add(rel.canonicalRelationId); // Type relation IDs
                }
            }
        }
        logger.log(
            `[MewContacts] Initial fetch complete. Found ${directRelationTargetIds.size} direct targets and ${directCanonicalRelationIds.size} canonical relations.`
        );
    } catch (error) {
        logger.error("[MewContacts] Error fetching initial layer data:", error);
        // Proceed with potentially incomplete data? Or return error?
        // For now, log and continue, map might be incomplete.
    }

    // --- Step 3: Second Layer Fetch (Property Values, Type Relations, Labels) ---
    const secondFetchIds = [
        ...Array.from(directRelationTargetIds).filter(
            (id) => !combinedLayerData.data.nodesById[id]
        ), // Only fetch missing target nodes
        ...Array.from(directCanonicalRelationIds).filter(
            (id) => !combinedLayerData.data.relationsById[id]
        ), // Only fetch missing canonical relations
    ];
    let thirdFetchIds = new Set<string>(); // For label nodes from type relations

    if (secondFetchIds.length > 0) {
        logger.log(
            `[MewContacts] Fetching second layer data for ${secondFetchIds.length} missing related objects...`
        );
        try {
            const secondLayerData = await mewApi.getLayerData(secondFetchIds);
            // Merge second layer data
            Object.assign(
                combinedLayerData.data.nodesById,
                secondLayerData.data.nodesById
            );
            Object.assign(
                combinedLayerData.data.relationsById,
                secondLayerData.data.relationsById
            );
            logger.log("[MewContacts] Second fetch complete.");

            // Find label node IDs from newly fetched canonical relations
            for (const relId of directCanonicalRelationIds) {
                const typeRelation =
                    combinedLayerData.data.relationsById[relId];
                if (
                    typeRelation &&
                    typeRelation.relationTypeId === "__type__"
                ) {
                    if (!combinedLayerData.data.nodesById[typeRelation.toId]) {
                        thirdFetchIds.add(typeRelation.toId); // Label node IDs
                    }
                }
            }
        } catch (error) {
            logger.error(
                "[MewContacts] Error fetching second layer data:",
                error
            );
            // Proceed with potentially incomplete data?
        }
    } else {
        logger.log(
            "[MewContacts] No second fetch needed based on initial data."
        );
        // Still need to check for label nodes from initially fetched type relations
        for (const relId of directCanonicalRelationIds) {
            const typeRelation = combinedLayerData.data.relationsById[relId];
            if (typeRelation && typeRelation.relationTypeId === "__type__") {
                if (!combinedLayerData.data.nodesById[typeRelation.toId]) {
                    thirdFetchIds.add(typeRelation.toId); // Label node IDs
                }
            }
        }
    }

    // --- Step 4: Third Layer Fetch (Label Nodes) ---
    if (thirdFetchIds.size > 0) {
        const thirdFetchIdsArray = Array.from(thirdFetchIds);
        logger.log(
            `[MewContacts] Fetching third layer data for ${thirdFetchIdsArray.length} missing label nodes...`
        );
        try {
            const thirdLayerData = await mewApi.getLayerData(
                thirdFetchIdsArray
            );
            Object.assign(
                combinedLayerData.data.nodesById,
                thirdLayerData.data.nodesById
            );
            logger.log("[MewContacts] Third fetch complete (label nodes).");
        } catch (error) {
            logger.error(
                "[MewContacts] Error fetching third layer data (label nodes):",
                error
            );
        }
    }

    // Remove diagnostic logging for now
    // --- BEGIN DIAGNOSTIC LOGGING ---
    // ... (keep removed or commented out) ...
    // --- END DIAGNOSTIC LOGGING ---

    // --- Step 5: Process Combined Data ---
    logger.log(
        "[MewContacts] Processing combined layer data to build contact info map..."
    );
    for (const contactNodeId of childNodeIds) {
        const contactNode = combinedLayerData.data.nodesById[contactNodeId];
        if (!contactNode) {
            logger.error(`Missing node data for child ID ${contactNodeId}`);
            continue;
        }

        const contactName = getNodeTextContent(contactNode);
        const properties = new Map<
            string,
            { propertyNodeId: string; value: string | null }
        >();
        let appleIdentifier: string | null = null;

        // Find all relations originating FROM this contact node
        const relations = Object.values(
            combinedLayerData.data.relationsById
        ).filter(
            (rel): rel is Relation =>
                rel !== null &&
                typeof rel === "object" &&
                (rel as Relation).fromId === contactNodeId
        );

        // Iterate through relations to find properties (like appleContactId, email_home, phone_mobile)
        for (const relation of relations) {
            // Find the label of this relation
            const typeRelationId = relation.canonicalRelationId;
            let relationLabel: string | null = null;
            if (typeRelationId) {
                // Use combined data
                const typeRelation =
                    combinedLayerData.data.relationsById[typeRelationId];
                if (
                    typeRelation &&
                    typeRelation.relationTypeId === "__type__"
                ) {
                    const labelNodeId = typeRelation.toId;
                    // Use combined data
                    const labelNode =
                        combinedLayerData.data.nodesById[labelNodeId];
                    relationLabel = getNodeTextContent(labelNode);
                }
            }

            if (!relationLabel) {
                // console.warn(`[MewContacts] Could not find label for relation ${relation.id} from node ${contactNodeId}`);
                continue; // Skip relations without a clear label
            }

            // Get the node the relation points TO (the property value node)
            const propertyNodeId = relation.toId;
            // Use combined data
            const propertyNode =
                combinedLayerData.data.nodesById[propertyNodeId];
            const propertyValue = getNodeTextContent(propertyNode);

            // Store the property info
            properties.set(relationLabel, {
                propertyNodeId,
                value: propertyValue,
            });

            // Specifically capture the appleIdentifier
            if (relationLabel === "appleContactId") {
                appleIdentifier = propertyValue;
            }
        }

        // Only add to map if we found the essential appleIdentifier
        if (appleIdentifier) {
            existingContactsMap.set(appleIdentifier, {
                mewId: contactNodeId,
                name: contactName,
                properties: properties,
            });
        } else {
            logger.error(
                `Could not find appleContactId for Mew node ${contactNodeId}. Skipping map entry.`
            );
        }
    }

    logger.log(
        `[MewContacts] Finished building contact info map with ${existingContactsMap.size} entries.`
    );
    return existingContactsMap;
}

/**
 * Syncs a single Apple Contact to Mew.
 * Creates or updates the contact node and its properties under the specified folder.
 * This function is intended to be called sequentially for contacts that need updating.
 * It compares the appleData with the existingInfo and generates a list of
 * Mew API operations (add/update/delete) needed to sync the state.
 * @param appleData The Apple Contact data from the source (e.g., macOS Contacts).
 * @param existingInfo Pre-fetched information about the contact in Mew, including its ID, name, and properties.
 * @param authorId The author ID to use for creating new properties.
 * @returns {Promise<any[]>} A promise that resolves to an array of Mew API operation objects.
 */
export async function syncContactToMew(
    appleData: AppleContact,
    existingInfo: ExistingContactInfo,
    authorId: string
): Promise<any[]> {
    // Return array of operations
    const contactDisplayName =
        `${appleData.givenName || ""} ${appleData.familyName || ""}`.trim() ||
        appleData.organizationName ||
        "Unnamed Contact";
    logger.log(
        `[MewContacts] Generating operations for: ${contactDisplayName} (Apple ID: ${appleData.identifier}, Mew ID: ${existingInfo.mewId})`
    );

    const mewContactId = existingInfo.mewId;
    const operations: any[] = []; // Array to collect operations
    const timestamp = Date.now(); // Consistent timestamp for operations in this sync cycle

    try {
        // 1. Check/Update Name
        const currentName = existingInfo.name;
        if (currentName !== contactDisplayName) {
            logger.log(
                `[MewContacts]  - Name requires update: \'${currentName}\' -> \'${contactDisplayName}\'`
            );
            // Generate update operation for the main contact node name
            const updateOp = await mewApi._generateUpdateNodeContentOperation(
                mewContactId,
                contactDisplayName,
                timestamp
            );
            if (updateOp) operations.push(updateOp);
        }

        // 2. Check/Update Properties
        // Create a mutable copy of existing properties to track handled/deleted ones
        const mewProperties = new Map(existingInfo.properties.entries());

        // First pass: Check for properties that need to be updated or added
        for (const [key, value] of Object.entries(appleData)) {
            // Skip non-property fields
            if (
                key === "identifier" ||
                key === "givenName" ||
                key === "familyName"
            )
                continue;

            // Handle arrays (like phoneNumbers, emailAddresses)
            if (Array.isArray(value)) {
                for (const item of value) {
                    if (!item || !item.value) continue;

                    const label = item.label ? item.label.toLowerCase() : "";
                    const relationLabel = label ? `${key}_${label}` : key;

                    const existingProp = mewProperties.get(relationLabel);

                    if (existingProp) {
                        // Property exists, check if update needed
                        let needsUpdate = false;
                        if (key === "emailAddresses") {
                            const appleValue = item.value.trim().toLowerCase();
                            const existingValue =
                                existingProp.value?.trim().toLowerCase() ||
                                null;
                            needsUpdate =
                                (appleValue && appleValue !== existingValue) ||
                                (!appleValue && existingValue !== null);
                        } else {
                            needsUpdate = existingProp.value !== item.value;
                        }

                        if (needsUpdate) {
                            logger.log(
                                `[MewContacts]  -- UPDATE Property ${relationLabel} (Node ${existingProp.propertyNodeId}): \'${existingProp.value}\' -> \'${item.value}\'`
                            );
                            const updateOp =
                                await mewApi._generateUpdateNodeContentOperation(
                                    existingProp.propertyNodeId,
                                    item.value,
                                    timestamp
                                );
                            if (updateOp) operations.push(updateOp);
                        }
                        mewProperties.delete(relationLabel);
                    } else {
                        // Property doesn't exist, add it
                        logger.log(
                            `[MewContacts]  -- ADD Property ${relationLabel}: \'${item.value}\'`
                        );
                        const addOps = mewApi.generatePropertyOperations(
                            mewContactId,
                            relationLabel,
                            item.value,
                            authorId,
                            timestamp
                        );
                        operations.push(...addOps);
                    }
                }
            } else if (typeof value === "string" && value) {
                // Handle simple string properties
                const relationLabel = key;
                const existingProp = mewProperties.get(relationLabel);

                if (existingProp) {
                    if (existingProp.value !== value) {
                        logger.log(
                            `[MewContacts]  -- UPDATE Property ${relationLabel} (Node ${existingProp.propertyNodeId}): \'${existingProp.value}\' -> \'${value}\'`
                        );
                        const updateOp =
                            await mewApi._generateUpdateNodeContentOperation(
                                existingProp.propertyNodeId,
                                value,
                                timestamp
                            );
                        if (updateOp) operations.push(updateOp);
                    }
                    mewProperties.delete(relationLabel);
                } else {
                    logger.log(
                        `[MewContacts]  -- ADD Property ${relationLabel}: \'${value}\'`
                    );
                    const addOps = mewApi.generatePropertyOperations(
                        mewContactId,
                        relationLabel,
                        value,
                        authorId,
                        timestamp
                    );
                    operations.push(...addOps);
                }
            }
        }

        // Second pass: Only check for properties to delete if we have operations to perform
        if (operations.length > 0 && mewProperties.size > 0) {
            logger.log(
                `[MewContacts]  - Checking for ${mewProperties.size} properties to delete...`
            );
            for (const [
                labelToDelete,
                propInfoToDelete,
            ] of mewProperties.entries()) {
                // Crucially, DON'T delete the appleContactId property!
                if (labelToDelete === "appleContactId") {
                    continue;
                }
                logger.log(
                    `[MewContacts]  -- DELETE Property ${labelToDelete} (Node ${propInfoToDelete.propertyNodeId}): Value \'${propInfoToDelete.value}\'`
                );
                const deleteOp = mewApi._generateDeleteNodeOperation(
                    propInfoToDelete.propertyNodeId
                );
                operations.push(deleteOp);
            }
        }

        // Only log if we actually generated operations
        if (operations.length > 0) {
            logger.log(
                `[MewContacts] Generated ${operations.length} operations for ${contactDisplayName}`
            );
        } else {
            logger.log(
                `[MewContacts] No changes needed for ${contactDisplayName}`
            );
        }
    } catch (error) {
        logger.error(
            `[MewContacts] Failed generating operations for contact ${contactDisplayName} (Apple ID: ${appleData.identifier}, Mew ID: ${existingInfo.mewId}):`,
            error
        );
        return [];
    }

    return operations;
}

/**
 * Main processing function for syncing Apple Contacts to Mew.
 * This function orchestrates the entire sync process:
 * 1. Parses incoming contact data
 * 2. Ensures the contacts folder exists
 * 3. Fetches existing contact information
 * 4. Processes new, modified, and deleted contacts
 * 5. Performs batch operations for efficiency
 *
 * @param userRootUrl - The root URL for the user's Mew space
 * @param contactsJsonString - Raw JSON string containing contact data from Apple
 */
export async function processContacts(
    userRootUrl: string,
    contactsJsonString: string
): Promise<void> {
    logger.log("Starting contact sync process");
    logger.log("User root URL:", { userRootUrl });

    // Parse incoming data and ensure contacts folder exists
    const contactsData = JSON.parse(contactsJsonString);
    logger.log("Parsed contacts data:", {
        totalContacts: Array.isArray(contactsData)
            ? contactsData.length
            : contactsData.contacts?.length,
        hasDeletedContacts: !!contactsData.deleted_contacts?.length,
    });

    const contactsFolderId = await ensureMyContactsFolder();
    logger.log("Contacts folder:", {
        id: contactsFolderId.folderId,
        wasCreated: contactsFolderId.created,
    });

    const existingMewContactsMap = await fetchExistingContactInfo(
        contactsFolderId.folderId
    );
    logger.log("Existing contacts:", {
        count: existingMewContactsMap.size,
        ids: Array.from(existingMewContactsMap.keys()),
    });

    // Track contacts that need creation or updates
    const contactsToCreate: AppleContact[] = [];
    const contactsToUpdate: { appleData: AppleContact; mewId: string }[] = [];

    // Handle both old and new message formats for backward compatibility
    const contacts = Array.isArray(contactsData)
        ? contactsData
        : contactsData.contacts;
    if (!Array.isArray(contacts)) {
        logger.error("Invalid contacts data format:", contactsData);
        return;
    }

    logger.log("Processing contacts:", {
        total: contacts.length,
        format: Array.isArray(contactsData) ? "array" : "object",
    });

    // Refactored Logic: Prioritize existence check over change_type
    for (const contact of contacts) {
        if (!contact || typeof contact !== "object" || !contact.identifier) {
            logger.error("Skipping invalid contact data:", contact);
            continue;
        }

        const existingContactInfo = existingMewContactsMap.get(
            contact.identifier
        );

        if (existingContactInfo) {
            // Contact exists in Mew, mark for update to ensure properties are synced
            logger.log(
                `[MewContacts] Contact ${contact.identifier} exists in Mew, scheduling for property check/update.`
            );
            contactsToUpdate.push({
                appleData: contact,
                mewId: existingContactInfo.mewId,
            });
        } else {
            // Contact does not exist in Mew, mark for creation
            logger.log(
                `[MewContacts] Contact ${contact.identifier} not found in Mew, scheduling for creation.`
            );
            contactsToCreate.push(contact);
        }
    }

    logger.log("Contacts identified for processing:", {
        toCreate: contactsToCreate.length,
        toUpdate: contactsToUpdate.length,
        totalFromSource: contacts.length,
    });

    // --- Phase 1: Create New Contacts ---
    if (contactsToCreate.length > 0) {
        logger.log(`Creating ${contactsToCreate.length} new contacts`);
        let createdCount = 0;
        for (let i = 0; i < contactsToCreate.length; i += BATCH_CHUNK_SIZE) {
            const chunk = contactsToCreate.slice(i, i + BATCH_CHUNK_SIZE);
            logger.log(
                `Processing batch ${i / BATCH_CHUNK_SIZE + 1} of ${Math.ceil(
                    contactsToCreate.length / BATCH_CHUNK_SIZE
                )}`
            );

            try {
                const createdMap = await mewApi.batchAddContacts(
                    chunk,
                    contactsFolderId.folderId
                );
                createdCount += createdMap.size;
                logger.log(
                    `Batch ${i / BATCH_CHUNK_SIZE + 1} created ${
                        createdMap.size
                    } contacts`
                );
            } catch (error) {
                logger.error(
                    `Batch creation failed for chunk starting at index ${i}:`,
                    error
                );
                throw new Error(
                    `Batch creation failed on chunk ${
                        i / BATCH_CHUNK_SIZE + 1
                    }, stopping sync.`
                );
            }
        }
        logger.log(`Created ${createdCount} new contacts`);
    }

    // --- Phase 2: Update Modified Contacts ---
    if (contactsToUpdate.length > 0) {
        logger.log(`Updating ${contactsToUpdate.length} modified contacts`);
        let allUpdateOps: any[] = [];
        const authorId = mewApi.getAuthorId(); // Get authorId once before the loop
        if (!authorId) {
            logger.error(
                "[MewContacts] Author ID not set in MewAPI for updates. Aborting updates."
            );
            // Optionally throw error or handle differently
        } else {
            // Generate update operations for each modified contact
            for (const updateInfo of contactsToUpdate) {
                const existingInfo = existingMewContactsMap.get(
                    updateInfo.appleData.identifier
                );
                if (!existingInfo) {
                    logger.error(
                        `Could not find pre-fetched info for existing contact ${updateInfo.mewId}. Skipping update.`
                    );
                    continue;
                }
                // Pass the fetched authorId to syncContactToMew
                const contactOps = await syncContactToMew(
                    updateInfo.appleData,
                    existingInfo,
                    authorId
                );
                if (contactOps.length > 0) {
                    allUpdateOps.push(...contactOps);
                }
            }
        }

        logger.log(`Generated ${allUpdateOps.length} update operations`);

        // Send update operations in chunks
        if (allUpdateOps.length > 0) {
            for (let i = 0; i < allUpdateOps.length; i += BATCH_CHUNK_SIZE) {
                const chunk = allUpdateOps.slice(i, i + BATCH_CHUNK_SIZE);
                logger.log(
                    `Processing update batch ${
                        i / BATCH_CHUNK_SIZE + 1
                    } of ${Math.ceil(allUpdateOps.length / BATCH_CHUNK_SIZE)}`
                );
                try {
                    await mewApi.sendBatchOperations(chunk);
                    logger.log(
                        `Batch ${i / BATCH_CHUNK_SIZE + 1} updated successfully`
                    );
                } catch (error) {
                    logger.error(
                        `Batch update failed for chunk starting at index ${i}:`,
                        error
                    );
                    throw new Error(
                        `Batch update failed on chunk ${
                            i / BATCH_CHUNK_SIZE + 1
                        }, stopping sync.`
                    );
                }
            }
            logger.log(`Updated ${allUpdateOps.length} contacts`);
        }
    }

    // --- Phase 3: Handle Deleted Contacts ---
    if (contactsData.deleted_contacts?.length > 0) {
        logger.log(
            `Processing ${contactsData.deleted_contacts.length} deleted contacts`
        );
        const deleteOps: any[] = [];

        // Generate delete operations for deleted contacts
        for (const deletedId of contactsData.deleted_contacts) {
            const existingInfo = existingMewContactsMap.get(deletedId);
            if (existingInfo) {
                deleteOps.push({
                    mewId: existingInfo.mewId,
                    mewUserRootUrl: userRootUrl,
                });
            }
        }

        logger.log(`Generated ${deleteOps.length} delete operations`);

        // Send delete operations in chunks
        if (deleteOps.length > 0) {
            for (let i = 0; i < deleteOps.length; i += BATCH_CHUNK_SIZE) {
                const chunk = deleteOps.slice(i, i + BATCH_CHUNK_SIZE);
                logger.log(
                    `Processing delete batch ${
                        i / BATCH_CHUNK_SIZE + 1
                    } of ${Math.ceil(deleteOps.length / BATCH_CHUNK_SIZE)}`
                );
                try {
                    await mewApi.sendBatchOperations(chunk);
                    logger.log(
                        `Batch ${i / BATCH_CHUNK_SIZE + 1} deleted successfully`
                    );
                } catch (error) {
                    logger.error(
                        `Batch delete failed for chunk starting at index ${i}:`,
                        error
                    );
                    throw new Error(
                        `Batch delete failed on chunk ${
                            i / BATCH_CHUNK_SIZE + 1
                        }, stopping sync.`
                    );
                }
            }
            logger.log(`Deleted ${deleteOps.length} contacts`);
        }
    }

    logger.log("Contact sync process completed");
}

/**
 * Starts the contact change listener process.
 * This function spawns the Python script and handles its output stream.
 */
function startContactListener() {
    const pythonProcess = spawn("python3", ["get_contacts_json.py"]);
    let buffer = "";

    // Initialize API config once when listener starts
    try {
        initializeApiConfigFromRootUrl();
    } catch (initError) {
        logger.error(
            "[MewContacts] Critical error initializing API config:",
            initError
        );
        // Decide how to handle this - maybe exit or retry?
        // For now, log and potentially let the process crash later if URL is needed.
    }

    pythonProcess.stdout.on("data", async (data) => {
        buffer += data.toString();

        // Process complete messages (separated by newlines)
        const messages = buffer.split("\n");
        buffer = messages.pop() || ""; // Keep the last incomplete message in the buffer

        for (const message of messages) {
            const trimmedMessage = message.trim(); // Trim message once
            if (trimmedMessage) {
                // Only process non-empty messages
                try {
                    const update = JSON.parse(trimmedMessage); // Parse the trimmed message
                    logger.log(
                        `[MewContacts] Received update type: ${
                            update.type
                        } with ${update.contacts?.length ?? 0} contacts` // Use safe access
                    );

                    // Pass only the contacts array to processContacts
                    if (
                        update.type === "initial" &&
                        Array.isArray(update.contacts)
                    ) {
                        await processContacts(
                            userRootUrlGlobal!, // userRootUrl is still needed by processContacts
                            JSON.stringify(update.contacts) // Pass only contacts
                        );
                    } else if (update.type === "update") {
                        // Handle changed contacts
                        if (
                            Array.isArray(update.contacts) &&
                            update.contacts.length > 0
                        ) {
                            await processContacts(
                                userRootUrlGlobal!, // userRootUrl is still needed by processContacts
                                JSON.stringify(update.contacts) // Pass only contacts
                            );
                        }

                        // Handle deleted contacts
                        if (
                            update.deleted_contacts &&
                            Array.isArray(update.deleted_contacts) && // Ensure it's an array
                            update.deleted_contacts.length > 0
                        ) {
                            logger.log(
                                `[MewContacts] Processing ${update.deleted_contacts.length} deleted contacts`
                            );
                            try {
                                // Fetch folder ID and author ID safely
                                const { folderId: contactsFolderId } =
                                    await ensureMyContactsFolder(); // Still need folder ID
                                const authorId = mewApi.getAuthorId();
                                if (!contactsFolderId || !authorId) {
                                    throw new Error(
                                        "Failed to get contacts folder ID or author ID for deletion."
                                    );
                                }

                                // Fetch existing contacts to get their Mew IDs
                                const existingContactsMap =
                                    await fetchExistingContactInfo(
                                        contactsFolderId
                                    );
                                const deleteOperations: any[] = []; // Use more specific type if possible

                                // Generate delete operations for deleted contacts
                                for (const deletedId of update.deleted_contacts) {
                                    const existingInfo =
                                        existingContactsMap.get(deletedId);
                                    if (existingInfo) {
                                        // Generate the actual delete operation object
                                        const deleteOp =
                                            mewApi._generateDeleteNodeOperation(
                                                existingInfo.mewId
                                            );
                                        deleteOperations.push(deleteOp);
                                        logger.log(
                                            `[MewContacts] Generated delete operation for Apple ID: ${deletedId}, Mew ID: ${existingInfo.mewId}`
                                        );
                                    } else {
                                        logger.log(
                                            `[MewContacts] Could not find Mew ID for deleted Apple ID: ${deletedId}. Skipping deletion.`
                                        );
                                    }
                                }

                                // Send delete operations in batches
                                if (deleteOperations.length > 0) {
                                    logger.log(
                                        `[MewContacts] Sending ${deleteOperations.length} delete operations...`
                                    );
                                    for (
                                        let i = 0;
                                        i < deleteOperations.length;
                                        i += BATCH_CHUNK_SIZE
                                    ) {
                                        const chunk = deleteOperations.slice(
                                            i,
                                            i + BATCH_CHUNK_SIZE
                                        );
                                        await mewApi.sendBatchOperations(chunk);
                                        logger.log(
                                            `[MewContacts] Sent batch delete operation ${
                                                i / BATCH_CHUNK_SIZE + 1
                                            }`
                                        );
                                    }
                                }
                            } catch (deleteError) {
                                logger.error(
                                    "[MewContacts] Error handling deleted contacts:",
                                    deleteError
                                );
                            }
                        }
                    }
                } catch (error) {
                    // Log parsing errors only for non-empty messages that failed
                    logger.error(
                        `[MewContacts] Error processing update message: "${trimmedMessage}"`, // Log the message that failed
                        error
                    );
                }
            }
        }
    });

    pythonProcess.stderr.on("data", (data) => {
        logger.log(data.toString());
    });

    pythonProcess.on("close", (code) => {
        logger.log(`[MewContacts] Python process exited with code ${code}`);
        // Attempt to restart the process after a delay
        setTimeout(startContactListener, 5000);
    });

    pythonProcess.on("error", (error) => {
        logger.error("[MewContacts] Failed to start Python process:", error);
        // Attempt to restart the process after a delay
        setTimeout(startContactListener, 5000);
    });
}

// Update the main execution block to use ES module syntax
const currentFilePath = fileURLToPath(import.meta.url);
const scriptPath = resolve(process.argv[1]);

if (currentFilePath === scriptPath) {
    const args = process.argv.slice(2);
    if (args.length !== 1) {
        logger.error("Usage: node mewContacts.js <user_root_url>");
        process.exit(1);
    }

    userRootUrlGlobal = args[0];
    startContactListener();
}

function generateOperations(
    appleContacts: AppleContact[],
    mewContacts: MewContact[],
    mewUserRootUrl: string
): Operation[] {
    const operations: Operation[] = [];
    const mewContactMap = new Map(mewContacts.map((c) => [c.id, c]));
    const processedMewIds = new Set<string>();

    // First pass: Update existing contacts and track which Mew contacts we've processed
    for (const appleContact of appleContacts) {
        const mewId = appleContactToMewId(appleContact);
        if (!mewId) continue;

        processedMewIds.add(mewId);
        const mewContact = mewContactMap.get(mewId);
        if (!mewContact) continue;

        // Only generate operations if there are actual changes
        const contactOperations = generateContactOperations(
            appleContact,
            mewContact,
            mewUserRootUrl
        );
        if (contactOperations.length > 0) {
            operations.push(...contactOperations);
        }
    }

    // Second pass: Delete Mew contacts that don't exist in Apple contacts
    for (const mewContact of mewContacts) {
        if (!processedMewIds.has(mewContact.id)) {
            operations.push({
                type: "delete",
                mewId: mewContact.id,
                mewUserRootUrl,
            });
        }
    }

    return operations;
}

function appleContactToMewId(appleContact: AppleContact): string | null {
    // Extract the Mew ID from the Apple contact's note
    if (!appleContact.note) return null;
    const match = appleContact.note.match(/Mew ID: ([a-f0-9-]+)/);
    return match ? match[1] : null;
}

function normalizeValue(value: string): string {
    return value.toLowerCase().trim();
}

function generateContactOperations(
    appleContact: AppleContact,
    mewContact: MewContact,
    mewUserRootUrl: string
): Operation[] {
    const operations: Operation[] = [];
    const processedPropertyTypes = new Set<string>();

    // Check for properties to delete
    const propertiesToDelete = mewContact.properties.filter((prop) => {
        // Skip appleContactId as it should never be deleted
        if (prop.type === "appleContactId") return false;

        // Check if this property exists in the Apple contact
        const exists =
            // Check phone numbers
            appleContact.phoneNumbers?.some(
                (phone) =>
                    prop.type === `phone_${phone.label?.toLowerCase() || ""}` &&
                    normalizeValue(prop.value || "") ===
                        normalizeValue(phone.value)
            ) ||
            false ||
            // Check email addresses
            appleContact.emailAddresses?.some(
                (email) =>
                    prop.type === `email_${email.label?.toLowerCase() || ""}` &&
                    normalizeValue(prop.value || "") ===
                        normalizeValue(email.value)
            ) ||
            false ||
            // Check organization name
            (prop.type === "organization" &&
                prop.value === appleContact.organizationName) ||
            // Check note
            (prop.type === "note" && prop.value === appleContact.note);

        return !exists;
    });

    if (propertiesToDelete.length > 0) {
        logger.log(
            `[MewContacts]  - Checking for ${propertiesToDelete.length} properties to delete...`
        );
        for (const prop of propertiesToDelete) {
            operations.push({
                type: "delete",
                mewId: mewContact.id,
                mewUserRootUrl,
                propertyId: prop.id,
            });
            processedPropertyTypes.add(prop.type);
        }
    }

    // Check for properties to add or update
    // Handle phone numbers
    if (appleContact.phoneNumbers) {
        for (const phone of appleContact.phoneNumbers) {
            const propType = `phone_${phone.label?.toLowerCase() || ""}`;
            if (processedPropertyTypes.has(propType)) continue;

            const existingProp = mewContact.properties.find(
                (prop) => prop.type === propType
            );
            if (!existingProp) {
                operations.push({
                    type: "add",
                    mewId: mewContact.id,
                    mewUserRootUrl,
                    property: {
                        type: propType,
                        value: phone.value,
                    },
                });
            } else if (
                normalizeValue(existingProp.value || "") !==
                normalizeValue(phone.value)
            ) {
                operations.push({
                    type: "update",
                    mewId: mewContact.id,
                    mewUserRootUrl,
                    propertyId: existingProp.id,
                    property: {
                        type: propType,
                        value: phone.value,
                    },
                });
            }
            processedPropertyTypes.add(propType);
        }
    }

    // Handle email addresses
    if (appleContact.emailAddresses) {
        for (const email of appleContact.emailAddresses) {
            const propType = `email_${email.label?.toLowerCase() || ""}`;
            if (processedPropertyTypes.has(propType)) continue;

            const existingProp = mewContact.properties.find(
                (prop) => prop.type === propType
            );
            if (!existingProp) {
                operations.push({
                    type: "add",
                    mewId: mewContact.id,
                    mewUserRootUrl,
                    property: {
                        type: propType,
                        value: email.value,
                    },
                });
            } else if (
                normalizeValue(existingProp.value || "") !==
                normalizeValue(email.value)
            ) {
                operations.push({
                    type: "update",
                    mewId: mewContact.id,
                    mewUserRootUrl,
                    propertyId: existingProp.id,
                    property: {
                        type: propType,
                        value: email.value,
                    },
                });
            }
            processedPropertyTypes.add(propType);
        }
    }

    // Handle organization name
    if (
        appleContact.organizationName &&
        !processedPropertyTypes.has("organization")
    ) {
        const existingProp = mewContact.properties.find(
            (prop) => prop.type === "organization"
        );
        if (!existingProp) {
            operations.push({
                type: "add",
                mewId: mewContact.id,
                mewUserRootUrl,
                property: {
                    type: "organization",
                    value: appleContact.organizationName,
                },
            });
        } else if (existingProp.value !== appleContact.organizationName) {
            operations.push({
                type: "update",
                mewId: mewContact.id,
                mewUserRootUrl,
                propertyId: existingProp.id,
                property: {
                    type: "organization",
                    value: appleContact.organizationName,
                },
            });
        }
        processedPropertyTypes.add("organization");
    }

    // Handle note
    if (appleContact.note && !processedPropertyTypes.has("note")) {
        const existingProp = mewContact.properties.find(
            (prop) => prop.type === "note"
        );
        if (!existingProp) {
            operations.push({
                type: "add",
                mewId: mewContact.id,
                mewUserRootUrl,
                property: {
                    type: "note",
                    value: appleContact.note,
                },
            });
        } else if (existingProp.value !== appleContact.note) {
            operations.push({
                type: "update",
                mewId: mewContact.id,
                mewUserRootUrl,
                propertyId: existingProp.id,
                property: {
                    type: "note",
                    value: appleContact.note,
                },
            });
        }
        processedPropertyTypes.add("note");
    }

    return operations;
}
