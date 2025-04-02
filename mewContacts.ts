/* mewContacts.ts - Manages syncing Apple Contacts to Mew */

import { fileURLToPath } from "url";
import { resolve } from "path";
import { execSync } from "child_process"; // For executing Python script
import {
    MewAPI,
    parseNodeIdFromUrl,
    getNodeTextContent,
    Relation,
    AppleContact,
} from "./MewService.js"; // Assuming MewService exports necessary types
import console from "console";

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
        // console.log(`[MewContacts]  - Checking properties...`);
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
        const appleValuesProcessed = new Set<string>(); // Track which specific apple values (e.g., phone number string) have been handled

        // Create a mutable copy of existing properties to track handled/deleted ones
        const mewProperties = new Map(existingInfo.properties.entries());

        for (const propInfo of propertiesToSync) {
            const applePropData = appleData[propInfo.key as keyof AppleContact];

            if (propInfo.array && Array.isArray(applePropData)) {
                // Handle arrays like phoneNumbers, emailAddresses
                const items = applePropData as {
                    label?: string | null;
                    value: string;
                }[];
                for (const item of items) {
                    if (!item.value) continue;
                    appleValuesProcessed.add(item.value); // Mark this value as present in source

                    const sanitizedLabel = item.label
                        ? item.label.replace(/[^a-zA-Z0-9]/g, "_").toLowerCase()
                        : "";
                    const relationLabel = sanitizedLabel
                        ? `${propInfo.baseLabel}_${sanitizedLabel}`
                        : propInfo.baseLabel;

                    const existingProp = mewProperties.get(relationLabel);

                    if (existingProp) {
                        // Property with this label exists in Mew

                        // --- Debug & Normalize Comparison ---
                        let appleItemValue = item.value;
                        let existingMewValue = existingProp.value;
                        let needsUpdate = false;

                        if (propInfo.baseLabel === "email") {
                            // Specific handling for email
                            appleItemValue =
                                appleItemValue?.trim().toLowerCase() || "";
                            existingMewValue =
                                existingMewValue?.trim().toLowerCase() || null; // Keep null distinct from empty
                            // Update only if non-null apple value differs from mew value, or if apple is empty but mew wasn't null
                            needsUpdate =
                                (appleItemValue &&
                                    appleItemValue !== existingMewValue) ||
                                (!appleItemValue && existingMewValue !== null);
                        } else {
                            // Generic comparison for other types
                            needsUpdate = existingMewValue !== appleItemValue;
                        }
                        // Log the comparison details if an update seems needed
                        if (needsUpdate) {
                            console.log(
                                `[DEBUG] Comparing ${relationLabel}: Mew='${
                                    existingProp.value
                                }'(${typeof existingProp.value}) vs Apple='${
                                    item.value
                                }'(${typeof item.value}). Normalized requires update: ${needsUpdate}`
                            );
                        }
                        // --- End Debug & Normalize ---

                        if (needsUpdate) {
                            // Use the needsUpdate flag
                            // Value mismatch - Update needed
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
                        // Mark as handled by removing from the temp map
                        mewProperties.delete(relationLabel);
                    } else {
                        // Property with this label does NOT exist in Mew - Add needed
                        console.log(
                            `[MewContacts]  -- ADD Property ${relationLabel}: \'${item.value}\'`
                        );
                        // Generate ADD operations using the helper
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
            } else if (
                !propInfo.array &&
                typeof applePropData === "string" &&
                applePropData
            ) {
                // Handle simple string properties (org, note)
                const appleValue = applePropData;
                const relationLabel = propInfo.baseLabel;
                appleValuesProcessed.add(appleValue); // Mark this value as present in source

                const existingProp = mewProperties.get(relationLabel);
                if (existingProp) {
                    // Property exists

                    // --- Debug Comparison ---
                    let needsUpdate = existingProp.value !== appleValue;
                    if (needsUpdate) {
                        console.log(
                            `[DEBUG] Comparing ${relationLabel}: Mew='${
                                existingProp.value
                            }'(${typeof existingProp.value}) vs Apple='${appleValue}'(${typeof appleValue}). Requires update: ${needsUpdate}`
                        );
                    }
                    // --- End Debug ---

                    if (needsUpdate) {
                        // Value mismatch - Update needed
                        console.log(
                            `[MewContacts]  -- UPDATE Property ${relationLabel} (Node ${existingProp.propertyNodeId}): \'${existingProp.value}\' -> \'${appleValue}\'`
                        );
                        const updateOp =
                            await mewApi._generateUpdateNodeContentOperation(
                                existingProp.propertyNodeId,
                                appleValue,
                                timestamp
                            );
                        if (updateOp) operations.push(updateOp);
                    }
                    // Mark as handled by removing from the temp map
                    mewProperties.delete(relationLabel);
                } else {
                    // Property does not exist - Add needed
                    console.log(
                        `[MewContacts]  -- ADD Property ${relationLabel}: \'${appleValue}\'`
                    );
                    // Generate ADD operations using the helper
                    const addOps = mewApi.generatePropertyOperations(
                        mewContactId,
                        relationLabel,
                        appleValue,
                        authorId,
                        timestamp
                    );
                    operations.push(...addOps);
                }
            }
        }

        // 3. Handle Deletion of properties remaining in the map
        if (mewProperties.size > 0) {
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
                // Generate DELETE operation
                const deleteOp = mewApi._generateDeleteNodeOperation(
                    propInfoToDelete.propertyNodeId
                );
                operations.push(deleteOp);
                // TODO: Consider deleting label node + relations if they become orphaned?
            }
        }
    } catch (error) {
        console.error(
            `[MewContacts] Failed generating operations for contact ${contactDisplayName} (Apple ID: ${appleData.identifier}, Mew ID: ${existingInfo.mewId}):`,
            error
        );
        // Return empty array or re-throw? Returning empty means this contact's updates are skipped.
        return [];
    }

    // Return the collected operations for this contact
    if (operations.length > 0) {
        console.log(
            `[MewContacts] Generated ${operations.length} operations for ${contactDisplayName}`
        );
    }
    return operations;
}

/**
 * Main processing function for the contact sync script.
 * Orchestrates fetching contacts from the Python script, determining which contacts
 * exist in Mew, batch-creating new contacts, and sequentially updating existing ones.
 * @param userRootUrl The root URL for the user's Mew space (passed as CLI argument).
 * @param contactsJsonString Raw JSON string fetched from the Python script,
 *                         containing an array of AppleContact objects.
 */
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
        // If the contacts folder already exists, fetch all children and their
        // corresponding Apple identifiers into a map for efficient lookup.
        let existingMewContactsMap = new Map<string, ExistingContactInfo>(); // Updated type

        if (!isNewFolder) {
            console.log(
                `[MewContacts] Fetching existing contacts from folder ${contactsFolderId}...`
            );
            try {
                existingMewContactsMap = await fetchExistingContactInfo(
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

        const contactsToCreate: AppleContact[] = []; // Use AppleContact type
        const contactsToUpdate: { appleData: AppleContact; mewId: string }[] =
            [];

        // Separate contacts into create/update lists based on the map
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

            const existingContactInfo = existingMewContactsMap.get(
                contact.identifier
            );
            if (existingContactInfo) {
                // Exists in Mew, needs update check
                contactsToUpdate.push({
                    appleData: contact,
                    mewId: existingContactInfo.mewId,
                }); // Pass mewId
            } else {
                // Does not exist in Mew, needs creation
                contactsToCreate.push(contact); // Push the full contact object
            }
        }

        console.log(
            `[MewContacts] Planning: ${contactsToCreate.length} creates, ${contactsToUpdate.length} updates.`
        );

        // --- Chunked Batch Create ---
        // Create new contacts in batches to improve performance and avoid server timeouts.
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

        // --- Generate Update Operations ---
        console.log(
            `[MewContacts] Generating update/delete operations for ${contactsToUpdate.length} existing contacts...`
        );
        let allUpdateOps: any[] = [];
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
            // Get operations needed for this specific contact
            const contactOps = await syncContactToMew(
                updateInfo.appleData,
                existingInfo
            );
            allUpdateOps.push(...contactOps); // Aggregate operations
        }
        console.log(
            `[MewContacts] Generated a total of ${allUpdateOps.length} update/delete operations.`
        );

        // --- Send Update Operations in Chunks ---
        if (allUpdateOps.length > 0) {
            console.log(
                `[MewContacts] Sending ${allUpdateOps.length} update/delete operations in chunks of ${BATCH_CHUNK_SIZE} operations...`
                // Note: Chunk size here is based on *operations*, not contacts
            );
            for (let i = 0; i < allUpdateOps.length; i += BATCH_CHUNK_SIZE) {
                const chunk = allUpdateOps.slice(i, i + BATCH_CHUNK_SIZE);
                console.log(
                    `[MewContacts] Sending update chunk ${
                        i / BATCH_CHUNK_SIZE + 1
                    } / ${Math.ceil(allUpdateOps.length / BATCH_CHUNK_SIZE)} (${
                        chunk.length
                    } operations)`
                );
                try {
                    await mewApi.sendBatchOperations(chunk); // Send the chunk of operations
                    console.log(
                        `[MewContacts] Update chunk ${
                            i / BATCH_CHUNK_SIZE + 1
                        } successful.`
                    );
                    // Optional delay?
                    // await new Promise(resolve => setTimeout(resolve, 100));
                } catch (error) {
                    console.error(
                        `[MewContacts] Batch update failed for chunk starting at index ${i}:`,
                        error
                    );
                    // Decide how to proceed
                    throw new Error(
                        `Batch update failed on chunk ${
                            i / BATCH_CHUNK_SIZE + 1
                        }, stopping sync.`
                    );
                }
            }
            console.log(`[MewContacts] Batch update phase completed.`);
        }
        // --- End Send Update Operations ---

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
// This ensures the main logic only runs when the script is executed directly,
// not when imported as a module.
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
            // Increase maxBuffer if needed for very large contact lists, though stderr logging is preferred.
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
