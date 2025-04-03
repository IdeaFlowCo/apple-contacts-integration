import sys
import json
import time
import objc
from Contacts import (
    CNContactStore, CNEntityType, CNContactFetchRequest,
    CNEntityTypeContacts, CNContactSortOrderGivenName, CNContactGivenNameKey,
    CNContactFamilyNameKey, CNContactMiddleNameKey, CNContactNamePrefixKey,
    CNContactNameSuffixKey, CNContactNicknameKey, CNContactOrganizationNameKey,
    CNContactPhoneNumbersKey, CNContactEmailAddressesKey, CNContactNoteKey,
    CNContactIdentifierKey, CNContactTypeKey, CNContactStoreDidChangeNotification
)
from Foundation import (
    NSAutoreleasePool, NSNotificationCenter, NSObject,
    NSRunLoop, NSDefaultRunLoopMode, NSDate
)
from libdispatch import dispatch_semaphore_create, dispatch_semaphore_wait, dispatch_semaphore_signal, DISPATCH_TIME_FOREVER

# Global observer to prevent garbage collection
observer = None
last_sync_time = 0
last_contact_ids = set()  # Track last known contact IDs

def log_stderr(message):
    """Prints a log message to stderr with a timestamp and script prefix."""
    print(f"[{time.strftime('%H:%M:%S')}][Python] {message}", file=sys.stderr)
    sys.stderr.flush()  # Ensure log is written immediately

def emit_json(data):
    """Emits JSON data to stdout with a newline separator."""
    print(json.dumps(data), flush=True)
    print("", flush=True)  # Empty line as separator

def get_required_keys():
    """Returns a list of CNContact keys needed for the sync process.

    Ensures essential keys like identifier, name components, organization,
    phone, email, and note are included.
    Removes duplicates while preserving order.

    Returns:
        list: A list of CNContactKey constants.
    """
    keys = [
        CNContactGivenNameKey,
        CNContactFamilyNameKey,
        # CNContactMiddleNameKey, # Usually not needed for basic sync
        # CNContactNamePrefixKey,
        # CNContactNameSuffixKey,
        # CNContactNicknameKey,
        CNContactOrganizationNameKey,
        CNContactPhoneNumbersKey,
        CNContactEmailAddressesKey,
        CNContactNoteKey,
        CNContactIdentifierKey, # Essential for tracking
        # CNContactTypeKey # Usually not needed for basic sync
    ]
    seen = set()
    return [x for x in keys if not (x in seen or seen.add(x))]

KEYS_TO_FETCH = get_required_keys()

def request_access(store):
    """Checks for and requests authorization to access macOS Contacts.

    Uses the Contacts framework to check the current status. If access is
    not determined, it requests access and waits synchronously for the user's
    response. If access is denied or restricted, it logs an error and exits
    the script.

    Args:
        store (CNContactStore): An instance of the contact store.

    Returns:
        bool: True if access is granted, otherwise the script exits.
    """
    # Create a semaphore for synchronization
    semaphore = dispatch_semaphore_create(0)
    granted = [False]  # Use list to allow modification in callback

    def completion_handler(success, error):
        granted[0] = success
        if error:
            log_stderr(f"Error requesting access: {error}")
        dispatch_semaphore_signal(semaphore)

    # Get current authorization status
    auth_status = CNContactStore.authorizationStatusForEntityType_(CNEntityTypeContacts)
    log_stderr(f"Contact access status: {auth_status}") # 0: NotDetermined, 1: Restricted, 2: Denied, 3: Authorized

    if auth_status == 0: # Not Determined
        log_stderr("Requesting contact access...")
        store.requestAccessForEntityType_completionHandler_(
            CNEntityTypeContacts,
            completion_handler
        )
        # Wait for the user's response
        dispatch_semaphore_wait(semaphore, DISPATCH_TIME_FOREVER)
    elif auth_status == 3: # Authorized
        granted[0] = True
    else: # Restricted or Denied
        granted[0] = False

    if not granted[0]:
        log_stderr("Contact access denied or restricted.")
        log_stderr("Please grant access in System Settings > Privacy & Security > Contacts.")
        sys.exit(1) # Exit if access is not granted

    log_stderr("Contact access granted.")
    return True

def fetch_contacts(store, contact_ids=None):
    """Fetches contacts from the provided CNContactStore.
    
    Args:
        store: The CNContactStore instance
        contact_ids: Optional list of specific contact IDs to fetch. If None, fetches all contacts.
    """
    pool = NSAutoreleasePool.alloc().init()
    log_stderr("Fetching contacts...")
    contacts_raw = []

    try:
        request = CNContactFetchRequest.alloc().initWithKeysToFetch_(KEYS_TO_FETCH)
        request.setSortOrder_(CNContactSortOrderGivenName)

        if contact_ids:
            # Fetch specific contacts
            for contact_id in contact_ids:
                try:
                    contact = store.unifiedContactWithIdentifier_keysToFetch_(
                        contact_id,
                        KEYS_TO_FETCH
                    )
                    if contact:
                        contacts_raw.append(contact)
                except Exception as e:
                    log_stderr(f"Error fetching contact {contact_id}: {e}")
        else:
            # Fetch all contacts
            success, error = store.enumerateContactsWithFetchRequest_error_usingBlock_(
                request,
                None,
                lambda contact, stop: contacts_raw.append(contact)
            )
            if not success:
                log_stderr(f"Error fetching contacts: {error}")
                return None

        log_stderr(f"Fetched {len(contacts_raw)} contacts successfully")
        return contacts_raw
    except Exception as e:
        log_stderr(f"Exception while fetching contacts: {e}")
        return None
    finally:
        del pool

def format_contacts_to_json(contacts_raw):
    """Formats a list of raw CNContact objects into a JSON serializable list.

    Iterates through the raw contacts, extracts the required fields
    (identifier, names, org, note, phones, emails), and structures them
    into dictionaries matching the format expected by the Node.js script.
    Handles potential missing keys and formats labeled values (phone/email).

    Args:
        contacts_raw (list): A list of CNContact objects from fetch_contacts.

    Returns:
        list: A list of dictionaries, where each dictionary represents a contact
              in a JSON-friendly format.
    """
    contacts_list = []
    for contact in contacts_raw:
        pool = NSAutoreleasePool.alloc().init() # Pool per contact iteration
        try:
            contact_dict = {
                "identifier": contact.identifier(),
                "givenName": contact.givenName() if contact.isKeyAvailable_(CNContactGivenNameKey) else None,
                "familyName": contact.familyName() if contact.isKeyAvailable_(CNContactFamilyNameKey) else None,
                "organizationName": contact.organizationName() if contact.isKeyAvailable_(CNContactOrganizationNameKey) else None,
                "note": contact.note() if contact.isKeyAvailable_(CNContactNoteKey) else None,
                "phoneNumbers": [],
                "emailAddresses": []
            }

            # Process Phone Numbers
            if contact.isKeyAvailable_(CNContactPhoneNumbersKey):
                phones = contact.phoneNumbers()
                if phones:
                    for labeled_value in phones:
                        phone_number = labeled_value.value().stringValue()
                        label = labeled_value.label() # Can be None
                        contact_dict["phoneNumbers"].append({"label": label, "value": phone_number})

            # Process Email Addresses
            if contact.isKeyAvailable_(CNContactEmailAddressesKey):
                emails = contact.emailAddresses()
                if emails:
                    for labeled_value in emails:
                        email_address = labeled_value.value()
                        label = labeled_value.label() # Can be None
                        contact_dict["emailAddresses"].append({"label": label, "value": email_address})

            contacts_list.append(contact_dict)
        except Exception as e:
            identifier = "Unknown"
            try:
                 identifier = contact.identifier()
            except: pass
            log_stderr(f"Error processing contact {identifier}: {e}")
        finally:
            del pool # Release pool for this contact

    return contacts_list

class ContactStoreObserver(NSObject):
    def init(self):
        self = objc.super(ContactStoreObserver, self).init()
        if self is None:
            return None
        self.contact_store = CNContactStore.alloc().init()
        self.last_contact_ids = set()
        return self

    def contactStoreDidChange_(self, notification):
        try:
            # Get current contact IDs
            current_contact_ids = set()
            contacts = fetch_contacts(self.contact_store)
            if contacts:
                for contact in contacts:
                    current_contact_ids.add(contact.identifier())

            # Find changed and deleted contacts
            changed_contacts = []
            deleted_contacts = list(self.last_contact_ids - current_contact_ids)

            # Get full data for changed contacts
            if current_contact_ids != self.last_contact_ids:
                changed_contacts = fetch_contacts(self.contact_store, list(current_contact_ids - self.last_contact_ids))
                if changed_contacts:
                    for contact in changed_contacts:
                        contact.change_type = 'added'
                
                # Get modified contacts
                modified_contacts = fetch_contacts(self.contact_store, list(current_contact_ids & self.last_contact_ids))
                if modified_contacts:
                    for contact in modified_contacts:
                        contact.change_type = 'modified'
                    changed_contacts.extend(modified_contacts)

            # Update last known IDs
            self.last_contact_ids = current_contact_ids

            # Send update message with only changed contacts
            if changed_contacts or deleted_contacts:
                print(json.dumps({
                    "type": "update",
                    "contacts": [format_contacts_to_json(c) for c in changed_contacts],
                    "deleted_contacts": deleted_contacts
                }))
                print("", flush=True)  # Empty line as separator

        except Exception as e:
            log_stderr(f"Unexpected error: {e}")
            # On error, send full sync to ensure consistency
            contacts = fetch_contacts(self.contact_store)
            if contacts:
                print(json.dumps({
                    "type": "initial",
                    "contacts": [format_contacts_to_json(c) for c in contacts]
                }))
                print("", flush=True)  # Empty line as separator

def setup_observer(store):
    """Sets up the contact change observer."""
    global observer, last_contact_ids
    observer = ContactStoreObserver.alloc().init()
    if observer is None:
        log_stderr("Failed to create observer")
        return None
    
    # Get initial contact IDs
    initial_contacts = fetch_contacts(store)
    if initial_contacts is not None:
        last_contact_ids = {contact.identifier() for contact in initial_contacts}
        log_stderr(f"Initial contact count: {len(last_contact_ids)}")
    
    # Register for notifications
    NSNotificationCenter.defaultCenter().addObserver_selector_name_object_(
        observer,
        "contactStoreDidChange:",
        CNContactStoreDidChangeNotification,
        store
    )
    log_stderr("Contact change observer set up successfully")
    return observer

if __name__ == "__main__":
    """Main execution block."""
    store = CNContactStore.alloc().init()
    pool = NSAutoreleasePool.alloc().init()

    try:
        if request_access(store):
            # Initial fetch
            raw_contacts = fetch_contacts(store)
            if raw_contacts is not None:
                formatted_contacts = format_contacts_to_json(raw_contacts)
                current_time = time.time()
                emit_json({
                    "type": "initial",
                    "timestamp": current_time,
                    "contacts": formatted_contacts
                })
                last_sync_time = current_time
                last_contact_ids = {contact.identifier() for contact in raw_contacts}
                log_stderr(f"Initial fetch: {len(formatted_contacts)} contacts")

            # Set up change observer
            observer = setup_observer(store)
            if observer is None:
                log_stderr("Failed to set up observer")
                sys.exit(1)

            log_stderr("Contact change observer set up, waiting for changes...")
            
            # Keep the process running
            run_loop = NSRunLoop.currentRunLoop()
            while True:
                run_loop.runMode_beforeDate_(
                    NSDefaultRunLoopMode,
                    NSDate.dateWithTimeIntervalSinceNow_(1.0)
                )
        else:
            log_stderr("Failed to get contact access")
            sys.exit(1)
    except KeyboardInterrupt:
        log_stderr("Received keyboard interrupt, shutting down")
    except Exception as e:
        log_stderr(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        if observer:
            NSNotificationCenter.defaultCenter().removeObserver_(observer)
            log_stderr("Removed observer")
        del pool 