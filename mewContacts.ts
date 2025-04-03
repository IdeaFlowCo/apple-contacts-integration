/* mewContacts.ts - Manages syncing Apple Contacts to Mew */

import { spawn } from "child_process"; // Changed from execSync to spawn
import {
    MewAPI,
    parseNodeIdFromUrl,
    getNodeTextContent,
    Relation,
    AppleContact,
    MewContact,
    Operation,
} from "./MewService.js"; // Assuming MewService exports necessary types
import console from "console";
import { fileURLToPath } from "url";
import { resolve } from "path";

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
 * Retrieves the Mew User ID by parsing the globally set user root URL.
 * Sets the user ID in the MewAPI instance upon first retrieval.
 * @throws {Error} If the global user root URL is not set or invalid.
 * @returns {Promise<string>} The Mew User ID.
 */
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

/**
 * Ensures that the target folder (defined by `myContactsFolderName`) exists in Mew
 * under the user's root node. Finds the existing folder or creates a new one.
 * @returns {Promise<{ folderId: string; created: boolean }>} An object containing the Node ID
 *          of the folder and a boolean indicating if it was newly created in this run.
 * @throws {Error} If unable to get user ID or create/find the folder.
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
    console.log(
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
            console.log(
                "[MewContacts] No existing child nodes found in folder."
            );
            return existingContactsMap; // Return empty map
        }
        childNodeIds = potentialContactNodes
            .map((node) => node?.id)
            .filter((id): id is string => !!id);

        if (childNodeIds.length === 0) {
            console.log("[MewContacts] Child nodes found but missing IDs.");
            return existingContactsMap;
        }
        console.log(`[MewContacts] Found ${childNodeIds.length} child nodes.`);
    } catch (error) {
        console.error("[MewContacts] Error getting child nodes:", error);
        return existingContactsMap; // Return empty map on error
    }

    // --- Step 2: First Layer Fetch (Children + Direct Relations) ---
    console.log(
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
        console.log(
            `[MewContacts] Initial fetch complete. Found ${directRelationTargetIds.size} direct targets and ${directCanonicalRelationIds.size} canonical relations.`
        );
    } catch (error) {
        console.error(
            "[MewContacts] Error fetching initial layer data:",
            error
        );
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
        console.log(
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
            console.log("[MewContacts] Second fetch complete.");

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
            console.error(
                "[MewContacts] Error fetching second layer data:",
                error
            );
            // Proceed with potentially incomplete data?
        }
    } else {
        console.log(
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
        console.log(
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
            console.log("[MewContacts] Third fetch complete (label nodes).");
        } catch (error) {
            console.error(
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
    console.log(
        "[MewContacts] Processing combined layer data to build contact info map..."
    );
    for (const contactNodeId of childNodeIds) {
        const contactNode = combinedLayerData.data.nodesById[contactNodeId];
        if (!contactNode) {
            console.warn(
                `[MewContacts] Missing node data for child ID ${contactNodeId}`
            );
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
            console.warn(
                `[MewContacts] Could not find appleContactId for Mew node ${contactNodeId}. Skipping map entry.`
            );
        }
    }

    console.log(
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
 * @returns {Promise<any[]>} A promise that resolves to an array of Mew API operation objects.
 */
export async function syncContactToMew(
    appleData: AppleContact,
    existingInfo: ExistingContactInfo
): Promise<any[]> {
    // Return array of operations
    const contactDisplayName =
        `${appleData.givenName || ""} ${appleData.familyName || ""}`.trim() ||
        appleData.organizationName ||
        "Unnamed Contact";
    console.log(
        `[MewContacts] Generating operations for: ${contactDisplayName} (Apple ID: ${appleData.identifier}, Mew ID: ${existingInfo.mewId})`
    );

    const mewContactId = existingInfo.mewId;
    const authorId = await getUserId(); // Get authorId once
    const operations: any[] = []; // Array to collect operations
    const timestamp = Date.now(); // Consistent timestamp for operations in this sync cycle

    try {
        // 1. Check/Update Name
        const currentName = existingInfo.name;
        if (currentName !== contactDisplayName) {
            console.log(
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
                            console.log(
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
                        console.log(
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
                        console.log(
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
                    console.log(
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
            console.log(
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
                console.log(
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
            console.log(
                `[MewContacts] Generated ${operations.length} operations for ${contactDisplayName}`
            );
        } else {
            console.log(
                `[MewContacts] No changes needed for ${contactDisplayName}`
            );
        }
    } catch (error) {
        console.error(
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
    // Parse incoming data and ensure contacts folder exists
    const contactsData = JSON.parse(contactsJsonString);
    const contactsFolderId = await ensureMyContactsFolder();
    const existingMewContactsMap = await fetchExistingContactInfo(
        contactsFolderId.folderId
    );

    // Track contacts that need creation or updates
    const contactsToCreate: AppleContact[] = [];
    const contactsToUpdate: { appleData: AppleContact; mewId: string }[] = [];

    // Handle both old and new message formats for backward compatibility
    const contacts = Array.isArray(contactsData)
        ? contactsData
        : contactsData.contacts;
    if (!Array.isArray(contacts)) {
        console.error(
            "[MewContacts] Invalid contacts data format:",
            contactsData
        );
        return;
    }

    // First pass: Identify contacts that need creation or updates
    for (const contact of contacts) {
        if (!contact || typeof contact !== "object" || !contact.identifier) {
            console.warn(
                "[MewContacts] Skipping invalid contact data:",
                contact
            );
            continue;
        }

        const existingContactInfo = existingMewContactsMap.get(
            contact.identifier
        );
        const changeType = contact.change_type;

        // Handle new contacts - add to creation list
        if (changeType === "added") {
            contactsToCreate.push(contact);
            continue;
        }

        // Handle modified contacts - check if they actually need updates
        if (changeType === "modified" && existingContactInfo) {
            const contactDisplayName =
                `${contact.givenName || ""} ${
                    contact.familyName || ""
                }`.trim() ||
                contact.organizationName ||
                "Unnamed Contact";

            // Check if name has changed
            const nameChanged = existingContactInfo.name !== contactDisplayName;

            // Check if any properties have actually changed
            const hasPropertyChanges =
                // Check phone numbers for changes
                contact.phoneNumbers?.some(
                    (phone: { label?: string; value: string }) => {
                        const existingPhone =
                            existingContactInfo.properties.get(
                                `phone_${phone.label?.toLowerCase() || ""}`
                            );
                        return (
                            !existingPhone ||
                            normalizeValue(existingPhone.value || "") !==
                                normalizeValue(phone.value)
                        );
                    }
                ) ||
                false ||
                // Check email addresses for changes
                contact.emailAddresses?.some(
                    (email: { label?: string; value: string }) => {
                        const existingEmail =
                            existingContactInfo.properties.get(
                                `email_${email.label?.toLowerCase() || ""}`
                            );
                        return (
                            !existingEmail ||
                            normalizeValue(existingEmail.value || "") !==
                                normalizeValue(email.value)
                        );
                    }
                ) ||
                false ||
                // Check organization name for changes
                (contact.organizationName &&
                    existingContactInfo.properties.get("organization")
                        ?.value !== contact.organizationName) ||
                // Check note for changes
                (contact.note &&
                    existingContactInfo.properties.get("note")?.value !==
                        contact.note);

            // Only add to update list if there are actual changes
            if (nameChanged || hasPropertyChanges) {
                contactsToUpdate.push({
                    appleData: contact,
                    mewId: existingContactInfo.mewId,
                });
            }
        }
    }

    // --- Phase 1: Batch Create New Contacts ---
    if (contactsToCreate.length > 0) {
        let createdCount = 0;
        // Process contacts in chunks to avoid overwhelming the API
        for (let i = 0; i < contactsToCreate.length; i += BATCH_CHUNK_SIZE) {
            const chunk = contactsToCreate.slice(i, i + BATCH_CHUNK_SIZE);
            try {
                const createdMap = await mewApi.batchAddContacts(
                    chunk,
                    contactsFolderId.folderId
                );
                createdCount += createdMap.size;
            } catch (error) {
                console.error(
                    `[MewContacts] Batch creation failed for chunk starting at index ${i}:`,
                    error
                );
                throw new Error(
                    `Batch creation failed on chunk ${
                        i / BATCH_CHUNK_SIZE + 1
                    }, stopping sync.`
                );
            }
        }
        console.log(`[MewContacts] Created ${createdCount} new contacts`);
    }

    // --- Phase 2: Update Modified Contacts ---
    if (contactsToUpdate.length > 0) {
        let allUpdateOps: any[] = [];
        // Generate update operations for each modified contact
        for (const updateInfo of contactsToUpdate) {
            const existingInfo = existingMewContactsMap.get(
                updateInfo.appleData.identifier
            );
            if (!existingInfo) {
                console.warn(
                    `[MewContacts] Could not find pre-fetched info for existing contact ${updateInfo.mewId}. Skipping update.`
                );
                continue;
            }
            const contactOps = await syncContactToMew(
                updateInfo.appleData,
                existingInfo
            );
            if (contactOps.length > 0) {
                allUpdateOps.push(...contactOps);
            }
        }

        // Send update operations in chunks
        if (allUpdateOps.length > 0) {
            for (let i = 0; i < allUpdateOps.length; i += BATCH_CHUNK_SIZE) {
                const chunk = allUpdateOps.slice(i, i + BATCH_CHUNK_SIZE);
                try {
                    await mewApi.sendBatchOperations(chunk);
                } catch (error) {
                    console.error(
                        `[MewContacts] Batch update failed for chunk starting at index ${i}:`,
                        error
                    );
                    throw new Error(
                        `Batch update failed on chunk ${
                            i / BATCH_CHUNK_SIZE + 1
                        }, stopping sync.`
                    );
                }
            }
            console.log(
                `[MewContacts] Updated ${allUpdateOps.length} contacts`
            );
        }
    }

    // --- Phase 3: Handle Deleted Contacts ---
    if (contactsData.deleted_contacts?.length > 0) {
        const deleteOps: any[] = [];

        // Generate delete operations for deleted contacts
        for (const deletedId of contactsData.deleted_contacts) {
            const existingInfo = existingMewContactsMap.get(deletedId);
            if (existingInfo) {
                deleteOps.push({
                    type: "delete",
                    mewId: existingInfo.mewId,
                    mewUserRootUrl: userRootUrl,
                });
            }
        }

        // Send delete operations in chunks
        if (deleteOps.length > 0) {
            for (let i = 0; i < deleteOps.length; i += BATCH_CHUNK_SIZE) {
                const chunk = deleteOps.slice(i, i + BATCH_CHUNK_SIZE);
                try {
                    await mewApi.sendBatchOperations(chunk);
                } catch (error) {
                    console.error(
                        `[MewContacts] Batch delete failed for chunk starting at index ${i}:`,
                        error
                    );
                    throw new Error(
                        `Batch delete failed on chunk ${
                            i / BATCH_CHUNK_SIZE + 1
                        }, stopping sync.`
                    );
                }
            }
            console.log(`[MewContacts] Deleted ${deleteOps.length} contacts`);
        }
    }
}

/**
 * Starts the contact change listener process.
 * This function spawns the Python script and handles its output stream.
 */
function startContactListener() {
    const pythonProcess = spawn("python3", ["get_contacts_json.py"]);
    let buffer = "";

    pythonProcess.stdout.on("data", async (data) => {
        buffer += data.toString();

        // Process complete messages (separated by newlines)
        const messages = buffer.split("\n");
        buffer = messages.pop() || ""; // Keep the last incomplete message in the buffer

        for (const message of messages) {
            if (message.trim()) {
                // Skip empty lines
                try {
                    const update = JSON.parse(message);
                    console.log(
                        `[MewContacts] Received ${update.type} update with ${update.contacts.length} contacts`
                    );

                    if (update.type === "initial") {
                        await processContacts(
                            userRootUrlGlobal!,
                            JSON.stringify(update.contacts)
                        );
                    } else if (update.type === "update") {
                        // Handle changed contacts
                        if (update.contacts.length > 0) {
                            await processContacts(
                                userRootUrlGlobal!,
                                JSON.stringify(update.contacts)
                            );
                        }

                        // Handle deleted contacts
                        if (
                            update.deleted_contacts &&
                            update.deleted_contacts.length > 0
                        ) {
                            console.log(
                                `[MewContacts] Processing ${update.deleted_contacts.length} deleted contacts`
                            );
                            const { folderId: contactsFolderId } =
                                await ensureMyContactsFolder();
                            if (!contactsFolderId) {
                                throw new Error(
                                    "Failed to find contacts folder"
                                );
                            }

                            // Fetch existing contacts to get their Mew IDs
                            const existingContactsMap =
                                await fetchExistingContactInfo(
                                    contactsFolderId
                                );
                            const operations: Operation[] = [];

                            // Generate delete operations for deleted contacts
                            for (const deletedId of update.deleted_contacts) {
                                const existingInfo =
                                    existingContactsMap.get(deletedId);
                                if (existingInfo) {
                                    operations.push({
                                        type: "delete",
                                        mewId: existingInfo.mewId,
                                        mewUserRootUrl: userRootUrlGlobal!,
                                    });
                                }
                            }

                            // Send delete operations in batches
                            if (operations.length > 0) {
                                console.log(
                                    `[MewContacts] Sending ${operations.length} delete operations...`
                                );
                                for (
                                    let i = 0;
                                    i < operations.length;
                                    i += BATCH_CHUNK_SIZE
                                ) {
                                    const chunk = operations.slice(
                                        i,
                                        i + BATCH_CHUNK_SIZE
                                    );
                                    await mewApi.sendBatchOperations(chunk);
                                }
                            }
                        }
                    }
                } catch (error) {
                    // Only log parsing errors for non-empty messages
                    if (message.trim()) {
                        console.error(
                            "[MewContacts] Error processing update:",
                            error
                        );
                    }
                }
            }
        }
    });

    pythonProcess.stderr.on("data", (data) => {
        console.log(data.toString());
    });

    pythonProcess.on("close", (code) => {
        console.log(`[MewContacts] Python process exited with code ${code}`);
        // Attempt to restart the process after a delay
        setTimeout(startContactListener, 5000);
    });

    pythonProcess.on("error", (error) => {
        console.error("[MewContacts] Failed to start Python process:", error);
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
        console.error("Usage: node mewContacts.js <user_root_url>");
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
        console.log(
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
