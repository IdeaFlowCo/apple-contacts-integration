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
    CNContactIdentifierKey, CNContactTypeKey
)
from Foundation import NSAutoreleasePool
from libdispatch import dispatch_semaphore_create, dispatch_semaphore_wait, dispatch_semaphore_signal, DISPATCH_TIME_FOREVER

def log_stderr(message):
    """Prints a log message to stderr with a timestamp and script prefix.

    Args:
        message (str): The message to log.
    """
    print(f"[{time.strftime('%H:%M:%S')}][Python] {message}", file=sys.stderr)

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

def fetch_contacts(store):
    """Fetches all contacts from the provided CNContactStore.

    Uses a CNContactFetchRequest with the keys defined in KEYS_TO_FETCH.
    Enumerates contacts and returns them as a list of raw CNContact objects.
    Handles potential errors during fetching.

    Args:
        store (CNContactStore): An authorized instance of the contact store.

    Returns:
        list | None: A list of CNContact objects, or None if fetching fails.
    """
    pool = NSAutoreleasePool.alloc().init()
    log_stderr("Fetching contacts...")
    all_contacts_raw = []

    try:
        request = CNContactFetchRequest.alloc().initWithKeysToFetch_(KEYS_TO_FETCH)
        request.setSortOrder_(CNContactSortOrderGivenName)

        # Use ObjC block directly for potentially better performance/stability
        success, error = store.enumerateContactsWithFetchRequest_error_usingBlock_(
            request,
            None,
            lambda contact, stop: all_contacts_raw.append(contact)
        )

        if not success:
            log_stderr(f"Error fetching contacts: {error}")
            return None

        log_stderr(f"Fetched {len(all_contacts_raw)} raw contacts successfully")
        return all_contacts_raw
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

if __name__ == "__main__":
    """Main execution block.

    Initializes the contact store, requests access, fetches contacts,
    formats them to JSON, and prints the JSON array to stdout.
    Logs progress and errors to stderr.
    """
    store = CNContactStore.alloc().init()

    if request_access(store):
        raw_contacts = fetch_contacts(store)
        if raw_contacts is not None:
            formatted_contacts = format_contacts_to_json(raw_contacts)
            # Print the final JSON array to stdout
            print(json.dumps(formatted_contacts, indent=2))
            log_stderr(f"Successfully processed and output {len(formatted_contacts)} contacts as JSON.")
        else:
            log_stderr("Failed to fetch contacts.")
            sys.exit(1)
    else:
        # request_access already logged and exited if needed
        pass 